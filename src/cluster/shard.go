package cluster

import (
	log "code.google.com/p/log4go"
	"engine"
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
	Close()
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
	id              uint32
	startTime       time.Time
	startMicro      int64
	endMicro        int64
	endTime         time.Time
	wal             WAL
	servers         []wal.Server
	clusterServers  []*ClusterServer
	store           LocalShardStore
	localShard      LocalShardDb
	localWrites     chan *protocol.Request
	serverWrites    map[*ClusterServer]chan *protocol.Request
	serverIds       []uint32
	shardType       ShardType
	durationIsSplit bool
	shardDuration   time.Duration
}

func NewShard(id uint32, startTime, endTime time.Time, shardType ShardType, durationIsSplit bool, wal WAL) *ShardData {
	return &ShardData{
		id:              id,
		startTime:       startTime,
		endTime:         endTime,
		wal:             wal,
		startMicro:      startTime.Unix() * int64(1000*1000),
		endMicro:        endTime.Unix() * int64(1000*1000),
		serverIds:       make([]uint32, 0),
		shardType:       shardType,
		durationIsSplit: durationIsSplit,
		shardDuration:   endTime.Sub(startTime),
	}
}

const (
	PER_SERVER_BUFFER_SIZE  = 10
	LOCAL_WRITE_BUFFER_SIZE = 10
)

var (
	queryResponse       = protocol.Response_QUERY
	endStreamResponse   = protocol.Response_END_STREAM
	queryRequest        = protocol.Request_QUERY
	dropDatabaseRequest = protocol.Request_DROP_DATABASE
)

type LocalShardDb interface {
	Write(database string, series *protocol.Series) error
	Query(*parser.QuerySpec, QueryProcessor) error
	DropDatabase(database string) error
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
	self.clusterServers = servers
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
	requestNumber, err := self.wal.AssignSequenceNumbersAndLog(request, self)
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
	requestNumber, err := self.wal.AssignSequenceNumbersAndLog(request, self)
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
	fmt.Printf("query spec: %#v\n", querySpec)
	// This is only for queries that are deletes or drops. They need to be sent everywhere as opposed to just the local or one of the remote shards.
	// But this boolean should only be set to true on the server that receives the initial query.
	if querySpec.RunAgainstAllServersInShard {
		if querySpec.IsDeleteFromSeriesQuery() {
			return self.logAndHandleDeleteQuery(querySpec, response)
		} else if querySpec.IsDropSeriesQuery() {
			return self.logAndHandleDropSeriesQuery(querySpec, response)
		}
	}

	if self.localShard != nil {
		var processor QueryProcessor
		if querySpec.IsListSeriesQuery() {
			processor = engine.NewListSeriesEngine(response)
		} else if querySpec.IsDeleteFromSeriesQuery() || querySpec.IsDropSeriesQuery() {
			maxDeleteResults := 10000
			processor = engine.NewPassthroughEngine(response, maxDeleteResults)
		} else {
			if self.ShouldAggregateLocally(querySpec) {
				fmt.Println("SHARD: query aggregate locally", self.id)
				processor = engine.NewQueryEngine(querySpec.SelectQuery(), response)
			} else {
				fmt.Println("SHARD: query passthrough", self.id)
				maxPointsToBufferBeforeSending := 1000
				processor = engine.NewPassthroughEngine(response, maxPointsToBufferBeforeSending)
			}
		}
		fmt.Println("SHARD query local: ", self.id)
		err := self.localShard.Query(querySpec, processor)
		fmt.Println("SHARD: processor.Close()", self.id)
		processor.Close()
		return err
	}

	randServerIndex := int(time.Now().UnixNano() % int64(len(self.clusterServers)))
	server := self.clusterServers[randServerIndex]
	request := self.createRequest(querySpec)

	return server.MakeRequest(request, response)
}

func (self *ShardData) DropDatabase(database string, sendToServers bool) {
	if self.localShard != nil {
		fmt.Println("SHARD DropDatabase: ", database)
		self.localShard.DropDatabase(database)
	}

	if !sendToServers {
		return
	}

	responses := make([]chan *protocol.Response, len(self.clusterServers), len(self.clusterServers))
	for i, server := range self.clusterServers {
		responseChan := make(chan *protocol.Response, 1)
		responses[i] = responseChan
		request := &protocol.Request{Type: &dropDatabaseRequest, Database: &database, ShardId: &self.id}
		go server.MakeRequest(request, responseChan)
	}
	for _, responseChan := range responses {
		// TODO: handle error responses
		<-responseChan
	}
}

func (self *ShardData) ShouldAggregateLocally(querySpec *parser.QuerySpec) bool {
	if self.durationIsSplit && querySpec.ReadsFromMultipleSeries() {
		return false
	}
	groupByInterval := querySpec.GetGroupByInterval()
	if groupByInterval == nil {
		return false
	}
	if self.shardDuration%*groupByInterval == 0 {
		return true
	}
	return false
}

func (self *ShardData) logAndHandleDeleteQuery(querySpec *parser.QuerySpec, response chan *protocol.Response) error {
	queryString := querySpec.GetQueryStringWithTimeCondition()
	request := self.createRequest(querySpec)
	request.Query = &queryString
	return self.logAndHandleDestructiveQuery(querySpec, request, response)
}

func (self *ShardData) logAndHandleDropSeriesQuery(querySpec *parser.QuerySpec, response chan *protocol.Response) error {
	return self.logAndHandleDestructiveQuery(querySpec, self.createRequest(querySpec), response)
}

func (self *ShardData) logAndHandleDestructiveQuery(querySpec *parser.QuerySpec, request *protocol.Request, response chan *protocol.Response) error {
	requestNumber, err := self.wal.AssignSequenceNumbersAndLog(request, self)
	if err != nil {
		return err
	}
	responses := make([]chan *protocol.Response, len(self.clusterServers), len(self.clusterServers))
	for i, server := range self.clusterServers {
		fmt.Println("SHARD: requesting to server: ", server.id)
		responseChan := make(chan *protocol.Response, 1)
		responses[i] = responseChan
		// do this so that a new id will get assigned
		request.Id = nil
		server.MakeRequest(request, responseChan)
	}
	if self.localShard != nil {
		responseChan := make(chan *protocol.Response, 1)
		responses = append(responses, responseChan)

		// this doesn't really apply at this point since destructive queries don't output anything, but it may later
		maxPointsFromDestructiveQuery := 1000
		processor := engine.NewPassthroughEngine(responseChan, maxPointsFromDestructiveQuery)
		err := self.localShard.Query(querySpec, processor)
		processor.Close()
		if err != nil {
			return err
		}
	}
	for i, responseChan := range responses {
		for {
			res := <-responseChan
			if *res.Type == endStreamResponse {
				// don't need to do a commit for the local datastore for now.
				if i < len(self.clusterServers) {
					self.wal.Commit(requestNumber, self.clusterServers[i])
				}
				break
			}
			response <- res
		}
	}
	response <- &protocol.Response{Type: &endStreamResponse}
	return nil
}

func (self *ShardData) createRequest(querySpec *parser.QuerySpec) *protocol.Request {
	queryString := querySpec.GetQueryString()
	user := querySpec.User()
	userName := user.GetName()
	database := querySpec.Database()
	isDbUser := !user.IsClusterAdmin()

	return &protocol.Request{
		Type:     &queryRequest,
		ShardId:  &self.id,
		Query:    &queryString,
		UserName: &userName,
		Database: &database,
		IsDbUser: &isDbUser,
	}
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
