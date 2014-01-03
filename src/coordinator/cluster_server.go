package coordinator

import (
	log "code.google.com/p/log4go"
	"net"
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

// in the coordinator test we don't want to create protobuf servers,
// so we just ignore creating a protobuf client when the connection
// string has a 0 port
func shouldConnect(addr string) bool {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.Error("Error parsing address '%s': %s", addr, err)
		return false
	}

	if port == "0" {
		log.Warn("Cannot connect to port 0. Not creating a protobuf client")
		return false
	}
	return true
}

func (self *ClusterServer) Connect() {
	if self.protobufClient != nil {
		return
	}

	if !shouldConnect(self.ProtobufConnectionString) {
		return
	}

	log.Info("ClusterServer: %d connecting to: %s", self.Id, self.ProtobufConnectionString)
	self.protobufClient = NewProtobufClient(self.ProtobufConnectionString)
}

func (self *ClusterServer) MakeRequest(request *protocol.Request, responseStream chan *protocol.Response) error {
	self.Connect()
	return self.protobufClient.MakeRequest(request, responseStream)
}
