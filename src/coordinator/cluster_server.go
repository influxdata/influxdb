package coordinator

import (
	log "code.google.com/p/log4go"
	"protocol"
)

type ClusterServer struct {
	Id                       uint32
	RaftName                 string
	State                    ServerState
	RaftConnectionString     string
	ProtobufConnectionString string
	protobufClient           *ProtobufClient
}

type ServerState int

const (
	LoadingRingData ServerState = iota
	SendingRingData
	DeletingOldData
	Running
	Potential
)

func (self *ClusterServer) Connect() {
	log.Info("ClusterServer: %d connecting to: %s", self.Id, self.ProtobufConnectionString)
	self.protobufClient = NewProtobufClient(self.ProtobufConnectionString)
}

func (self *ClusterServer) MakeRequest(request *protocol.Request, responseStream chan *protocol.Response) error {
	if self.protobufClient == nil {
		self.protobufClient = NewProtobufClient(self.ProtobufConnectionString)
	}
	return self.protobufClient.MakeRequest(request, responseStream)
}
