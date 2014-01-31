package cluster

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
	connection               ServerConnection
}

type ServerConnection interface {
	Connect()
	MakeRequest(request *protocol.Request, responseStream chan *protocol.Response) error
}

type ServerState int

const (
	LoadingRingData ServerState = iota
	SendingRingData
	DeletingOldData
	Running
	Potential
)

func NewClusterServer(raftName, raftConnectionString, protobufConnectionString string, connection ServerConnection) *ClusterServer {
	return &ClusterServer{
		RaftName:                 raftName,
		RaftConnectionString:     raftConnectionString,
		ProtobufConnectionString: protobufConnectionString,
		connection:               connection,
	}
}

// in the coordinator test we don't want to create protobuf servers,
// so we just ignore creating a protobuf client when the connection
// string has a 0 port
func shouldConnect(addr string) bool {
	log.Debug("SHOULD CONNECT: ", addr)
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
	if !shouldConnect(self.ProtobufConnectionString) {
		return
	}

	log.Info("ClusterServer: %d connecting to: %s", self.Id, self.ProtobufConnectionString)
	self.connection.Connect()
}

func (self *ClusterServer) MakeRequest(request *protocol.Request, responseStream chan *protocol.Response) error {
	self.Connect()
	return self.connection.MakeRequest(request, responseStream)
}
