package cluster

import (
	log "code.google.com/p/log4go"
	"errors"
	"fmt"
	"net"
	"protocol"
	"time"
)

const (
	DEFAULT_BACKOFF   = time.Second
	MAX_BACKOFF       = 10 * time.Second
	HEARTBEAT_TIMEOUT = 100 * time.Millisecond
)

type ClusterServer struct {
	Id                       uint32
	RaftName                 string
	State                    ServerState
	RaftConnectionString     string
	ProtobufConnectionString string
	connection               ServerConnection
	HeartbeatInterval        time.Duration
	Backoff                  time.Duration
	isUp                     bool
	writeBuffer              *WriteBuffer
	heartbeatStarted         bool
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

func NewClusterServer(raftName, raftConnectionString, protobufConnectionString string, connection ServerConnection, heartbeatInterval time.Duration) *ClusterServer {
	if heartbeatInterval.Nanoseconds() < 1000 {
		heartbeatInterval = time.Millisecond * 10
	}

	s := &ClusterServer{
		RaftName:                 raftName,
		RaftConnectionString:     raftConnectionString,
		ProtobufConnectionString: protobufConnectionString,
		connection:               connection,
		HeartbeatInterval:        heartbeatInterval,
		Backoff:                  DEFAULT_BACKOFF,
		heartbeatStarted:         false,
	}

	return s
}

func (self *ClusterServer) StartHeartbeat() {
	if self.heartbeatStarted {
		return
	}

	self.heartbeatStarted = true
	self.isUp = true
	go self.heartbeat()
}

func (self *ClusterServer) SetWriteBuffer(writeBuffer *WriteBuffer) {
	self.writeBuffer = writeBuffer
}

func (self *ClusterServer) GetId() uint32 {
	return self.Id
}

func (self *ClusterServer) Connect() {
	if !shouldConnect(self.ProtobufConnectionString) {
		return
	}

	log.Info("ClusterServer: %d connecting to: %s", self.Id, self.ProtobufConnectionString)
	self.connection.Connect()
}

func (self *ClusterServer) MakeRequest(request *protocol.Request, responseStream chan *protocol.Response) error {
	err := self.connection.MakeRequest(request, responseStream)
	if err != nil {
		self.isUp = false
		message := err.Error()
		select {
		case responseStream <- &protocol.Response{Type: &endStreamResponse, ErrorMessage: &message}:
		default:
		}
	}
	return err
}

func (self *ClusterServer) Write(request *protocol.Request) error {
	responseChan := make(chan *protocol.Response)
	err := self.connection.MakeRequest(request, responseChan)
	if err != nil {
		return err
	}
	response := <-responseChan
	if response.ErrorMessage != nil {
		return errors.New(*response.ErrorMessage)
	}
	return nil
}

func (self *ClusterServer) BufferWrite(request *protocol.Request) {
	self.writeBuffer.Write(request)
}

func (self *ClusterServer) IsUp() bool {
	return self.isUp
}

// private methods

var HEARTBEAT_TYPE = protocol.Request_HEARTBEAT

func (self *ClusterServer) heartbeat() {
	defer func() {
		self.heartbeatStarted = false
	}()

	responseChan := make(chan *protocol.Response)
	heartbeatRequest := &protocol.Request{
		Type:     &HEARTBEAT_TYPE,
		Database: protocol.String(""),
	}
	for {
		heartbeatRequest.Id = nil
		err := self.MakeRequest(heartbeatRequest, responseChan)
		if err != nil {
			self.handleHeartbeatError(err)
			continue
		}
		err = self.getHeartbeatResponse(responseChan)
		if err != nil {
			self.handleHeartbeatError(err)
			continue
		}

		// otherwise, reset the backoff and mark the server as up
		self.isUp = true
		self.Backoff = DEFAULT_BACKOFF
		<-time.After(self.HeartbeatInterval)
	}
}

func (self *ClusterServer) getHeartbeatResponse(responseChan <-chan *protocol.Response) error {
	select {
	case response := <-responseChan:
		if response.ErrorMessage != nil {
			return fmt.Errorf("Server %d returned error to heartbeat: %s", self.Id, *response.ErrorMessage)
		}

		if *response.Type != protocol.Response_HEARTBEAT {
			return fmt.Errorf("Server returned a non heartbeat response")
		}
	case <-time.After(HEARTBEAT_TIMEOUT):
		return fmt.Errorf("Server failed to return heartbeat in 100ms: %d", self.Id)
	}

	return nil
}

func (self *ClusterServer) handleHeartbeatError(err error) {
	self.isUp = false
	self.Backoff *= 2
	if self.Backoff > MAX_BACKOFF {
		self.Backoff = MAX_BACKOFF
	}
	<-time.After(self.Backoff)
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
