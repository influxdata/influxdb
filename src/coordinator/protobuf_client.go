package coordinator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"protocol"
	"sync/atomic"
	"time"

	log "code.google.com/p/log4go"
)

type ProtobufClient struct {
	conn          net.Conn
	hostAndPort   string
	connectCalled bool
	lastRequestId uint32
	writeTimeout  time.Duration
	stopped       bool
	requests      chan encodedRequest
	responses     chan *protocol.Response
	clear         chan struct{}
}

type encodedRequest struct {
	id     uint32
	data   []byte
	future future
}

type future struct {
	timeMade     time.Time
	responseChan chan *protocol.Response
}

const (
	REQUEST_RETRY_ATTEMPTS = 2
	MAX_RESPONSE_SIZE      = MAX_REQUEST_SIZE
	MAX_REQUEST_TIME       = time.Second * 1200
	RECONNECT_RETRY_WAIT   = time.Millisecond * 100
)

func NewProtobufClient(hostAndPort string, writeTimeout time.Duration) *ProtobufClient {
	log.Debug("NewProtobufClient: ", hostAndPort)
	return &ProtobufClient{
		hostAndPort:  hostAndPort,
		requests:     make(chan encodedRequest),
		responses:    make(chan *protocol.Response),
		clear:        make(chan struct{}),
		writeTimeout: writeTimeout,
		stopped:      false,
	}
}

func (self *ProtobufClient) Connect() {
	if self.connectCalled {
		return
	}
	self.connectCalled = true
	go self.ReqRespLoop()
	go self.readResponses()
}

func (self *ProtobufClient) deliver(data []byte) (err error) {
	for i := 0; i < REQUEST_RETRY_ATTEMPTS; i++ {
		if self.conn == nil || err == io.EOF {
			self.reconnect()
		}

		if self.writeTimeout > 0 {
			self.conn.SetWriteDeadline(time.Now().Add(self.writeTimeout))
		}
		binary.Write(self.conn, binary.LittleEndian, uint32(len(data)))
		_, err = self.conn.Write(data)

		if err == nil {
			return
		}
	}

	return
}

func (self *ProtobufClient) ReqRespLoop() {
	futures := make(map[uint32]future)
	cleanup := time.NewTicker(time.Minute)
	cleanupMessage := "clearing all requests"

	for !self.stopped {
		select {

		case request := <-self.requests:
			log.Info("request: %d len: %d", request.id, len(request.data))
			if request.future.responseChan != nil {
				futures[request.id] = request.future
			}

			err := self.deliver(request.data)

			if err != nil && request.future.responseChan != nil {
				e := err.Error()
				request.future.responseChan <- &protocol.Response{Type: &endStreamResponse, ErrorMessage: &e}
			}

		case response := <-self.responses:
			log.Info("response: ", *response.RequestId)
			future, ok := futures[*response.RequestId]
			if ok {
				if *response.Type == protocol.Response_END_STREAM || *response.Type == protocol.Response_WRITE_OK || *response.Type == protocol.Response_HEARTBEAT || *response.Type == protocol.Response_ACCESS_DENIED {
					delete(futures, *response.RequestId)
				}
				future.responseChan <- response
			}

		case nowish := <-cleanup.C:
			maxAge := nowish.Add(-MAX_REQUEST_TIME)
			for id, future := range futures {
				if future.timeMade.Before(maxAge) {
					delete(futures, id)
					log.Warn("Request timed out: ", id)
				}
			}

		case <-self.clear:
			for id, future := range futures {
				select {
				case future.responseChan <- &protocol.Response{Type: &endStreamResponse, ErrorMessage: &cleanupMessage}:
				default:
					log.Debug("Cannot send cleanup response on channel for request %d", id)
				}
				delete(futures, id)
			}

		}
	}
}

func (self *ProtobufClient) Close() {
	if self.conn != nil {
		self.conn.Close()
		self.stopped = true
		self.conn = nil
	}
	self.ClearRequests()
}

func (self *ProtobufClient) ClearRequests() {
	self.clear <- struct{}{}
}

// Makes a request to the server. If the responseStream chan is not nil it will expect a response from the server
// with a matching request.Id. The REQUEST_RETRY_ATTEMPTS constant of 3 and the RECONNECT_RETRY_WAIT of 100ms means
// that an attempt to make a request to a downed server will take 300ms to time out.
func (self *ProtobufClient) MakeRequest(request *protocol.Request, responseStream chan *protocol.Response) error {
	if request.Id == nil {
		id := atomic.AddUint32(&self.lastRequestId, uint32(1))
		request.Id = &id
	}

	data, err := request.Encode()
	if err != nil {
		return err
	}

	self.requests <- encodedRequest{id: *request.Id, data: data, future: future{timeMade: time.Now(), responseChan: responseStream}}
	return nil
}

func (self *ProtobufClient) readResponses() {
	var messageSizeU uint32
	buff := bytes.NewBuffer(make([]byte, 0, MAX_RESPONSE_SIZE))

	for !self.stopped {
		if self.conn == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		err := binary.Read(self.conn, binary.LittleEndian, &messageSizeU)
		if err != nil {
			log.Error("Error while reading message: %d", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		buff.Reset()
		_, err = io.CopyN(buff, self.conn, int64(messageSizeU))
		if err != nil {
			log.Error("Error while reading message: %d", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		response, err := protocol.DecodeResponse(buff)
		if err != nil {
			log.Error("error unmarshaling response: %s", err)
			time.Sleep(200 * time.Millisecond)
		} else {
			self.responses <- response
		}
	}
}

func (self *ProtobufClient) reconnect() error {
	if self.conn != nil {
		self.conn.Close()
	}

	for i := 0; i <= 100; i++ {
		conn, err := net.DialTimeout("tcp", self.hostAndPort, self.writeTimeout)
		if err == nil {
			self.conn = conn
			log.Info("Protobuf Client connected to %s", self.hostAndPort)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	err := fmt.Errorf("failed to connect to %s 100 times", self.hostAndPort)
	log.Error(err)
	return err
}
