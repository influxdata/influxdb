package coordinator

import (
	"bytes"
	log "code.google.com/p/log4go"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"protocol"
	"sync"
	"sync/atomic"
	"time"
)

type ProtobufClient struct {
	connLock          sync.Mutex
	conn              net.Conn
	hostAndPort       string
	requestBufferLock sync.RWMutex
	requestBuffer     map[uint32]*runningRequest
	reconnectWait     sync.WaitGroup
	connectCalled     bool
	lastRequestId     uint32
	writeTimeout      time.Duration
}

type runningRequest struct {
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
		hostAndPort:   hostAndPort,
		requestBuffer: make(map[uint32]*runningRequest),
		writeTimeout:  writeTimeout,
	}
}

func (self *ProtobufClient) Connect() {
	self.connLock.Lock()
	defer self.connLock.Unlock()
	if self.connectCalled {
		return
	}
	self.connectCalled = true
	go func() {
		self.reconnect()
		self.readResponses()
	}()
	go self.peridicallySweepTimedOutRequests()
}

func (self *ProtobufClient) Close() {
	self.connLock.Lock()
	defer self.connLock.Unlock()
	if self.conn != nil {
		self.conn.Close()
		self.conn = nil
	}
}

func (self *ProtobufClient) getConnection() net.Conn {
	self.connLock.Lock()
	defer self.connLock.Unlock()
	return self.conn
}

// Makes a request to the server. If the responseStream chan is not nil it will expect a response from the server
// with a matching request.Id. The REQUEST_RETRY_ATTEMPTS constant of 3 and the RECONNECT_RETRY_WAIT of 100ms means
// that an attempt to make a request to a downed server will take 300ms to time out.
func (self *ProtobufClient) MakeRequest(request *protocol.Request, responseStream chan *protocol.Response) error {
	if request.Id == nil {
		id := atomic.AddUint32(&self.lastRequestId, uint32(1))
		request.Id = &id
	}
	if responseStream != nil {
		self.requestBufferLock.Lock()

		// this should actually never happen. The sweeper should clear out dead requests
		// before the uint32 ids roll over.
		if oldReq, alreadyHasRequestById := self.requestBuffer[*request.Id]; alreadyHasRequestById {
			message := "already has a request with this id, must have timed out"
			log.Error(message)
			oldReq.responseChan <- &protocol.Response{Type: &endStreamResponse, ErrorMessage: &message}
		}
		self.requestBuffer[*request.Id] = &runningRequest{time.Now(), responseStream}
		self.requestBufferLock.Unlock()
	}

	data, err := request.Encode()
	if err != nil {
		return err
	}

	conn := self.getConnection()
	if conn == nil {
		conn = self.reconnect()
		if conn == nil {
			return fmt.Errorf("Failed to connect to server %s", self.hostAndPort)
		}
	}

	if self.writeTimeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(self.writeTimeout))
	}
	buff := bytes.NewBuffer(make([]byte, 0, len(data)+8))
	binary.Write(buff, binary.LittleEndian, uint32(len(data)))
	_, err = conn.Write(append(buff.Bytes(), data...))

	if err == nil {
		return nil
	}

	// if we got here it errored out, clear out the request
	self.requestBufferLock.Lock()
	delete(self.requestBuffer, *request.Id)
	self.requestBufferLock.Unlock()
	self.reconnect()
	return err
}

func (self *ProtobufClient) readResponses() {
	message := make([]byte, 0, MAX_RESPONSE_SIZE)
	buff := bytes.NewBuffer(message)
	for {
		buff.Reset()
		conn := self.getConnection()
		if conn == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		var messageSizeU uint32
		var err error
		err = binary.Read(conn, binary.LittleEndian, &messageSizeU)
		if err != nil {
			log.Error("Error while reading messsage size: %d", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		messageSize := int64(messageSizeU)
		messageReader := io.LimitReader(conn, messageSize)
		_, err = io.Copy(buff, messageReader)
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
			self.sendResponse(response)
		}
	}
}

func (self *ProtobufClient) sendResponse(response *protocol.Response) {
	self.requestBufferLock.RLock()
	req, ok := self.requestBuffer[*response.RequestId]
	self.requestBufferLock.RUnlock()
	if ok {
		if *response.Type == protocol.Response_END_STREAM || *response.Type == protocol.Response_WRITE_OK {
			self.requestBufferLock.Lock()
			delete(self.requestBuffer, *response.RequestId)
			self.requestBufferLock.Unlock()
		}
		select {
		case req.responseChan <- response:
		default:
			log.Error("ProtobufClient: Response buffer full! ", self.hostAndPort, response)
			panic("fuck, dropping shit!")
			// if it's an end stream response, we have to send it so start it in a goroutine so we can make sure it gets through without blocking the reading of responses.
			if *response.Type == protocol.Response_END_STREAM || *response.Type == protocol.Response_WRITE_OK || *response.Type == protocol.Response_ACCESS_DENIED {
				go func() {
					req.responseChan <- response
				}()
			}
		}
	}
}

func (self *ProtobufClient) reconnect() net.Conn {
	self.connLock.Lock()
	defer self.connLock.Unlock()

	if self.conn != nil {
		self.conn.Close()
	}
	conn, err := net.DialTimeout("tcp", self.hostAndPort, self.writeTimeout)
	if err == nil {
		self.conn = conn
		log.Info("connected to %s", self.hostAndPort)
		return self.conn
	}
	log.Error("failed to connect to %s", self.hostAndPort)
	return nil
}

func (self *ProtobufClient) peridicallySweepTimedOutRequests() {
	for {
		time.Sleep(time.Minute)
		self.requestBufferLock.Lock()
		maxAge := time.Now().Add(-MAX_REQUEST_TIME)
		for k, req := range self.requestBuffer {
			if req.timeMade.Before(maxAge) {
				delete(self.requestBuffer, k)
				log.Warn("Request timed out.")
			}
		}
		self.requestBufferLock.Unlock()
	}
}
