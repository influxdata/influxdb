package cluster

import (
	log "code.google.com/p/log4go"
	"engine"
	"errors"
	"fmt"
	"parser"
	"protocol"
	"sort"
	"time"
	"wal"
)

// A shard imements an interface for writing and querying data.
// It can be copied to multiple servers or the local datastore.
// Shard contains data from [startTime, endTime)
// Ids are unique across the cluster
type Shard interface {
	Id() uint32
	StartTime() time.Time
	EndTime() time.Time
	Write(*protocol.Request) error
	Query(querySpec *parser.QuerySpec, response chan *protocol.Response) error
	IsMicrosecondInRange(t int64) bool
}

// Passed to a shard (local datastore or whatever) that gets yielded points from series.
type QueryProcessor interface {
	// This method returns true if the query should continue. If the query should be stopped,
	// like maybe the limit was hit, it should return false
	YieldPoint(seriesName *string, columnNames []string, point *protocol.Point) bool
}

type PassthroughProcessor struct {
	responseChan chan *protocol.Response
	response     *protocol.Response
}

func NewPassthroughProcessor(responseChan chan *protocol.Response) *PassthroughProcessor {
	response := &protocol.Response{
		Type:   &queryResponse,
		Series: &protocol.Series{Points: make([]*protocol.Point, 0)}}

	return &PassthroughProcessor{
		responseChan: responseChan,
		response:     response,
	}
}

func (self *PassthroughProcessor) YieldPoint(seriesName *string, columnNames []string, point *protocol.Point) bool {
	self.response.Series.Name = seriesName
	self.response.Series.Fields = columnNames
	self.response.Series.Points = append(self.response.Series.Points, point)
	return true
}

func (self *PassthroughProcessor) Close() {
	self.responseChan <- self.response
	response := &protocol.Response{Type: &endStreamResponse}
	self.responseChan <- response
}

type NewShardData struct {
	Id        uint32 `json:",omitempty"`
	StartTime time.Time
	EndTime   time.Time
	ServerIds []uint32
	Type      ShardType
}

type ShardType int

const (
	LONG_TERM ShardType = iota
	SHORT_TERM
)

type ShardData struct {
	id           uint32
	startTime    time.Time
	startMicro   int64
	endMicro     int64
	endTime      time.Time
	wal          WAL
	servers      []wal.Server
	store        LocalShardStore
	localShard   LocalShardDb
	localWrites  chan *protocol.Request
	serverWrites map[*ClusterServer]chan *protocol.Request
	serverIds    []uint32
	shardType    ShardType
}

func NewShard(id uint32, startTime, endTime time.Time, shardType ShardType, wal WAL) *ShardData {
	return &ShardData{
		id:         id,
		startTime:  startTime,
		endTime:    endTime,
		wal:        wal,
		startMicro: startTime.Unix() * int64(1000*1000),
		endMicro:   endTime.Unix() * int64(1000*1000),
		serverIds:  make([]uint32, 0),
		shardType:  shardType,
	}
}

const (
	PER_SERVER_BUFFER_SIZE  = 10
	LOCAL_WRITE_BUFFER_SIZE = 10
)

var (
	queryResponse     = protocol.Response_QUERY
	endStreamResponse = protocol.Response_END_STREAM
)

type LocalShardDb interface {
	Write(database string, series *protocol.Series) error
	Query(*parser.QuerySpec, QueryProcessor) error
}

type LocalShardStore interface {
	GetOrCreateShard(id uint32) (LocalShardDb, error)
}

func (self *ShardData) Id() uint32 {
	return self.id
}

func (self *ShardData) StartTime() time.Time {
	return self.startTime
}

func (self *ShardData) EndTime() time.Time {
	return self.endTime
}

func (self *ShardData) IsMicrosecondInRange(t int64) bool {
	return t >= self.startMicro && t < self.endMicro
}

func (self *ShardData) SetServers(servers []*ClusterServer) {
	self.servers = make([]wal.Server, len(servers), len(servers))
	self.serverWrites = make(map[*ClusterServer]chan *protocol.Request)
	for i, server := range servers {
		self.serverIds = append(self.serverIds, server.Id())
		self.servers[i] = server
		writeBuffer := make(chan *protocol.Request, PER_SERVER_BUFFER_SIZE)
		go self.handleWritesToServer(server, writeBuffer)
		self.serverWrites[server] = writeBuffer
	}
}

func (self *ShardData) SetLocalStore(store LocalShardStore, localServerId uint32) error {
	self.serverIds = append(self.serverIds, localServerId)
	self.store = store
	self.localWrites = make(chan *protocol.Request, LOCAL_WRITE_BUFFER_SIZE)
	shard, err := self.store.GetOrCreateShard(self.id)
	if err != nil {
		return err
	}
	self.localShard = shard

	go self.handleLocalWrites()
	return nil
}

func (self *ShardData) IsLocal() bool {
	return self.store != nil
}

func (self *ShardData) ServerIds() []uint32 {
	return self.serverIds
}

func (self *ShardData) Write(request *protocol.Request) error {
	fmt.Println("SHARD Write: ", self.id, request)
	request.ShardId = &self.id
	requestNumber, err := self.wal.AssignSequenceNumbersAndLog(request, self, self.servers)
	if err != nil {
		return err
	}
	request.RequestNumber = &requestNumber
	if self.store != nil {
		self.localWrites <- request
	}
	for _, writeBuffer := range self.serverWrites {
		writeBuffer <- request
	}
	return nil
}

func (self *ShardData) WriteLocalOnly(request *protocol.Request) error {
	requestNumber, err := self.wal.AssignSequenceNumbersAndLog(request, self, self.servers)
	if err != nil {
		return err
	}
	request.RequestNumber = &requestNumber
	if self.store != nil {
		self.localWrites <- request
	}
	return nil
}

func (self *ShardData) Query(querySpec *parser.QuerySpec, response chan *protocol.Response) error {
	if self.localShard != nil {
		processor := engine.NewQueryEngine(querySpec.SelectQuery(), response)
		err := self.localShard.Query(querySpec, processor)
		processor.Close()
		return err
	}
	// TODO: make remote shards work
	return errors.New("Remote shards not implemented!")
}

// used to serialize shards when sending around in raft or when snapshotting in the log
func (self *ShardData) ToNewShardData() *NewShardData {
	return &NewShardData{
		Id:        self.id,
		StartTime: self.startTime,
		EndTime:   self.endTime,
		Type:      self.shardType,
		ServerIds: self.serverIds,
	}
}

func (self *ShardData) handleWritesToServer(server *ClusterServer, writeBuffer chan *protocol.Request) {
	responseStream := make(chan *protocol.Response)
	fmt.Println("HandleWritesToServer: ", server)
	for {
		request := <-writeBuffer
		requestNumber := *request.RequestNumber
		// this doens't need to be sent to the remote server, we just keep it around for the WAL commit
		request.SequenceNumber = nil

		// TODO: make some sort of timeout for this response along with a replay from the WAL.
		// Basically, if the server is in a timeout state do the following:
		// * keep pulling requests from the writeBuffer and throw them on the ground. Or keep some in memory and then toss
		// * check periodically for the server to come back. when it has:
		// * self.WAL.RecoverServerFromRequestNumber(requestNumber, server, yield func(request *protocol.Request, shard wal.Shard) error)
		// * once all those have been sent to the server, resume sending requests
		server.MakeRequest(request, responseStream)
		response := <-responseStream
		if *response.Type == protocol.Response_WRITE_OK {
			fmt.Println("COMMIT!")
			self.wal.Commit(requestNumber, server)
		} else {
			// TODO: retry logic for failed request
			log.Error("REQUEST to server %s failed:: ", server.ProtobufConnectionString, response.GetErrorMessage())
		}
	}
}

func (self *ShardData) handleLocalWrites() {
	for {
		request := <-self.localWrites
		err := self.localShard.Write(*request.Database, request.Series)

		// TODO: handle errors writing to local store
		if err != nil {
			log.Error("Writing to local shard: ", err)
		}
	}
}

func SortShardsByTimeAscending(shards []*ShardData) {
	sort.Sort(ByShardTimeAsc{shards})
}

func SortShardsByTimeDescending(shards []*ShardData) {
	sort.Sort(ByShardTimeDesc{shards})
}

type ShardCollection []*ShardData

func (s ShardCollection) Len() int      { return len(s) }
func (s ShardCollection) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type ByShardTimeDesc struct{ ShardCollection }
type ByShardTimeAsc struct{ ShardCollection }

func (s ByShardTimeAsc) Less(i, j int) bool {
	if s.ShardCollection[i] != nil && s.ShardCollection[j] != nil {
		iStartTime := s.ShardCollection[i].StartTime().Unix()
		jStartTime := s.ShardCollection[j].StartTime().Unix()
		if iStartTime == jStartTime {
			return s.ShardCollection[i].Id() < s.ShardCollection[j].Id()
		}
		return iStartTime < jStartTime
	}
	return false
}
func (s ByShardTimeDesc) Less(i, j int) bool {
	if s.ShardCollection[i] != nil && s.ShardCollection[j] != nil {
		iStartTime := s.ShardCollection[i].StartTime().Unix()
		jStartTime := s.ShardCollection[j].StartTime().Unix()
		if iStartTime == jStartTime {
			return s.ShardCollection[i].Id() < s.ShardCollection[j].Id()
		}
		return iStartTime > jStartTime
	}
	return false
}
