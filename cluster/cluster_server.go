package cluster

import (
	"errors"
	"fmt"
	"net"
	"time"

	log "code.google.com/p/log4go"
	c "github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/protocol"
)

const (
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
	MinBackoff               time.Duration
	MaxBackoff               time.Duration
	isUp                     bool
	writeBuffer              *WriteBuffer
	heartbeatStarted         bool
}

type ServerConnection interface {
	Connect()
	Close()
	ClearRequests()
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

func NewClusterServer(raftName, raftConnectionString, protobufConnectionString string, connection ServerConnection, config *c.Configuration) *ClusterServer {

	s := &ClusterServer{
		RaftName:                 raftName,
		RaftConnectionString:     raftConnectionString,
		ProtobufConnectionString: protobufConnectionString,
		connection:               connection,
		HeartbeatInterval:        config.ProtobufHeartbeatInterval.Duration,
		Backoff:                  config.ProtobufMinBackoff.Duration,
		MinBackoff:               config.ProtobufMinBackoff.Duration,
		MaxBackoff:               config.ProtobufMaxBackoff.Duration,
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

func (self *ClusterServer) MakeRequest(request *protocol.Request, responseStream chan *protocol.Response) {
	err := self.connection.MakeRequest(request, responseStream)
	if err != nil {
		message := err.Error()
		select {
		case responseStream <- &protocol.Response{Type: &endStreamResponse, ErrorMessage: &message}:
		default:
		}
		self.markServerAsDown()
	}
}

func (self *ClusterServer) Write(request *protocol.Request) error {
	responseChan := make(chan *protocol.Response, 1)
	err := self.connection.MakeRequest(request, responseChan)
	if err != nil {
		return err
	}
	log.Debug("Waiting for response to %d", request.GetRequestNumber())
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

	for {
		// this chan is buffered and in the loop on purpose. This is so
		// that if reading a heartbeat times out, and the heartbeat then comes through
		// later, it will be dumped into this chan and not block the protobuf client reader.
		responseChan := make(chan *protocol.Response, 1)
		heartbeatRequest := &protocol.Request{
			Type:     &HEARTBEAT_TYPE,
			Database: protocol.String(""),
		}
		self.MakeRequest(heartbeatRequest, responseChan)
		err := self.getHeartbeatResponse(responseChan)
		if err != nil {
			self.handleHeartbeatError(err)
			continue
		}

		if !self.isUp {
			log.Warn("Server marked as up. Hearbeat succeeded")
		}
		// otherwise, reset the backoff and mark the server as up
		self.isUp = true
		self.Backoff = self.MinBackoff
		time.Sleep(self.HeartbeatInterval)
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

	case <-time.After(self.HeartbeatInterval):
		return fmt.Errorf("Server failed to return heartbeat in %s: %d", self.HeartbeatInterval, self.Id)
	}

	return nil
}

func (self *ClusterServer) markServerAsDown() {
	self.isUp = false
	self.connection.ClearRequests()
}

func (self *ClusterServer) handleHeartbeatError(err error) {
	if self.isUp {
		log.Warn("Server marked as down. Hearbeat error for server: %d - %s: %s", self.Id, self.ProtobufConnectionString, err)
	}
	self.markServerAsDown()
	self.Backoff *= 2
	if self.Backoff > self.MaxBackoff {
		self.Backoff = self.MaxBackoff
	}
	time.Sleep(self.Backoff)
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
