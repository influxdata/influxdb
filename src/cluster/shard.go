package cluster

import (
	log "code.google.com/p/log4go"
	"parser"
	"protocol"
	"time"
	"wal"
)

// A shard imements an interface for writing and querying data.
// It can be copied to multiple servers.
// Shard contains data from [startTime, endTime)
// Ids are unique across the cluster
type Shard interface {
	Id() uint32
	StartTime() time.Time
	EndTime() time.Time
	Write(*protocol.Request) error
	Query(*parser.Query, chan *protocol.Response) error
}

type ShardData struct {
	id           uint32
	startTime    time.Time
	endTime      time.Time
	wal          WAL
	servers      []wal.Server
	store        LocalShardStore
	localWrites  chan *protocol.Request
	serverWrites map[*ClusterServer]chan *protocol.Request
}

func NewShard(id uint32, startTime, endTime time.Time, wal WAL) *ShardData {
	return &ShardData{id: id, startTime: startTime, endTime: endTime, wal: wal}
}

const (
	PER_SERVER_BUFFER_SIZE  = 10
	LOCAL_WRITE_BUFFER_SIZE = 10
)

type LocalShardDb interface {
	Write(database string, series *protocol.Series) error
	Query(*parser.Query, chan *protocol.Response) error
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

func (self *ShardData) SetServers(servers []*ClusterServer) {
	self.servers = make([]wal.Server, len(servers), len(servers))
	self.serverWrites = make(map[*ClusterServer]chan *protocol.Request)
	for i, server := range servers {
		self.servers[i] = server
		writeBuffer := make(chan *protocol.Request, PER_SERVER_BUFFER_SIZE)
		go self.handleWritesToServer(server, writeBuffer)
		self.serverWrites[server] = writeBuffer
	}
}

func (self *ShardData) SetLocalStore(store LocalShardStore) {
	self.store = store
	self.localWrites = make(chan *protocol.Request, LOCAL_WRITE_BUFFER_SIZE)
	go self.handleLocalWrites()
}

func (self *ShardData) Write(request *protocol.Request) error {
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

func (self *ShardData) Query(*parser.Query, chan *protocol.Response) error {
	return nil
}

func (self *ShardData) handleWritesToServer(server *ClusterServer, writeBuffer chan *protocol.Request) {
	responseStream := make(chan *protocol.Response)
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
		shard, err := self.store.GetOrCreateShard(self.id)
		if err != nil {
			log.Error("Creating shard %d: %s", self.id, err)

			// TODO: handle write retry
			continue
		}
		err = shard.Write(*request.Database, request.Series)
		if err != nil {
			log.Error("Writing to local shard: ", err)
		}
	}
}
