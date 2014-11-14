package coordinator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/protocol"
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
	attempts          int
	stopped           bool
	reconChan         chan struct{}
	reconGroup        *sync.WaitGroup
	once              *sync.Once
}

type runningRequest struct {
	timeMade time.Time
	r        cluster.ResponseChannel
	request  *protocol.Request
}

const (
	REQUEST_RETRY_ATTEMPTS = 2
	MAX_RESPONSE_SIZE      = MAX_REQUEST_SIZE
	MAX_REQUEST_TIME       = time.Second * 1200
	RECONNECT_RETRY_WAIT   = time.Millisecond * 100
)

func NewProtobufClient(hostAndPort string, writeTimeout time.Duration) *ProtobufClient {
	log.Debug("NewProtobufClient: %s", hostAndPort)
	return &ProtobufClient{
		hostAndPort:   hostAndPort,
		requestBuffer: make(map[uint32]*runningRequest),
		writeTimeout:  writeTimeout,
		reconChan:     make(chan struct{}, 1),
		reconGroup:    new(sync.WaitGroup),
		once:          new(sync.Once),
		stopped:       false,
	}
}

func (self *ProtobufClient) Connect() {
	self.once.Do(self.connect)
}

func (self *ProtobufClient) connect() {
	self.reconChan <- struct{}{}
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
		self.stopped = true
		self.conn = nil
	}
	self.ClearRequests()
}

func (self *ProtobufClient) getConnection() net.Conn {
	self.connLock.Lock()
	defer self.connLock.Unlock()
	return self.conn
}

func (self *ProtobufClient) ClearRequests() {
	self.requestBufferLock.Lock()
	defer self.requestBufferLock.Unlock()

	for _, req := range self.requestBuffer {
		self.cancelRequest(req.request)
	}

	self.requestBuffer = map[uint32]*runningRequest{}
}

func (self *ProtobufClient) CancelRequest(request *protocol.Request) {
	self.requestBufferLock.Lock()
	defer self.requestBufferLock.Unlock()
	self.cancelRequest(request)
}

func (self *ProtobufClient) cancelRequest(request *protocol.Request) {
	req, ok := self.requestBuffer[*request.Id]
	if !ok {
		return
	}
	message := "cancelling request"
	req.r.Yield(&protocol.Response{
		Type:         protocol.Response_ERROR.Enum(),
		ErrorMessage: &message,
	})
	delete(self.requestBuffer, *request.Id)
}

// Makes a request to the server. If the responseStream chan is not nil it will expect a response from the server
// with a matching request.Id. The REQUEST_RETRY_ATTEMPTS constant of 3 and the RECONNECT_RETRY_WAIT of 100ms means
// that an attempt to make a request to a downed server will take 300ms to time out.
func (self *ProtobufClient) MakeRequest(request *protocol.Request, r cluster.ResponseChannel) error {
	if request.Id == nil {
		id := atomic.AddUint32(&self.lastRequestId, uint32(1))
		request.Id = &id
	}
	if r != nil {
		self.requestBufferLock.Lock()

		// this should actually never happen. The sweeper should clear out dead requests
		// before the uint32 ids roll over.
		if oldReq, alreadyHasRequestById := self.requestBuffer[*request.Id]; alreadyHasRequestById {
			message := "already has a request with this id, must have timed out"
			log.Error(message)
			oldReq.r.Yield(&protocol.Response{
				Type:         protocol.Response_ERROR.Enum(),
				ErrorMessage: &message,
			})
		}
		self.requestBuffer[*request.Id] = &runningRequest{timeMade: time.Now(), r: r, request: request}
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
	buff.Write(data)
	_, err = conn.Write(buff.Bytes())

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

	connErrFn := func(err error) {
		log.Error("Error while reading messsage: %s", err.Error())
		self.conn.Close()
		self.conn = nil
		time.Sleep(200 * time.Millisecond)
	}

	for !self.stopped {
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
			connErrFn(err)
			continue
		}
		messageSize := int64(messageSizeU)
		messageReader := io.LimitReader(conn, messageSize)
		_, err = io.Copy(buff, messageReader)
		if err != nil {
			connErrFn(err)
			continue
		}
		response, err := protocol.DecodeResponse(buff)
		if err != nil {
			log.Error("error unmarshaling response: %s", err.Error())
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
	if !ok {
		return
	}

	deleteRequest := false
	switch rt := response.GetType(); rt {
	case protocol.Response_END_STREAM,
		protocol.Response_HEARTBEAT,
		protocol.Response_ERROR:
		deleteRequest = true
	case protocol.Response_QUERY:
		// do nothing
	default:
		panic(fmt.Errorf("Unknown response type: %s", rt))
	}

	self.requestBufferLock.Lock()
	req, ok = self.requestBuffer[*response.RequestId]
	if deleteRequest {
		delete(self.requestBuffer, *response.RequestId)
	}
	self.requestBufferLock.Unlock()
	if !ok {
		return
	}

	log.Debug("ProtobufClient yielding to %s %s", req.r.Name(), response)
	req.r.Yield(response)
}

func (self *ProtobufClient) reconnect() net.Conn {
	select {
	case <-self.reconChan:
		self.reconGroup.Add(1)
		defer func() {
			self.reconGroup.Done()
			self.reconChan <- struct{}{}
		}()
	default:
		self.reconGroup.Wait()
		return self.conn
	}

	if self.conn != nil {
		self.conn.Close()
	}
	conn, err := net.DialTimeout("tcp", self.hostAndPort, self.writeTimeout)
	if err != nil {
		self.attempts++
		if self.attempts < 100 {
			return nil
		}

		log.Error("failed to connect to %s %d times", self.hostAndPort, self.attempts)
		self.attempts = 0
	}

	self.attempts = 0
	self.conn = conn
	log.Info("connected to %s", self.hostAndPort)
	return conn
}

func (self *ProtobufClient) peridicallySweepTimedOutRequests() {
	for {
		time.Sleep(time.Minute)
		self.requestBufferLock.Lock()
		maxAge := time.Now().Add(-MAX_REQUEST_TIME)
		for k, req := range self.requestBuffer {
			if req.timeMade.Before(maxAge) {
				delete(self.requestBuffer, k)
				log.Warn("Request timed out: %v", req.request)
			}
		}
		self.requestBufferLock.Unlock()
	}
}
