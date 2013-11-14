package coordinator

import (
	"fmt"
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
	fmt.Printf("ClusterServer: %d connecting to: %s\n", self.Id, self.ProtobufConnectionString)
	self.protobufClient = NewProtobufClient(self.ProtobufConnectionString)
}
