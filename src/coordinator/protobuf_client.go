package coordinator

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
	"protocol"
	"sync"
	"sync/atomic"
	"time"
)

type ProtobufClient struct {
	conn              net.Conn
	hostAndPort       string
	requestBufferLock sync.RWMutex
	requestBuffer     map[uint32]*runningRequest
	connectionStatus  uint32
	reconnectWait     sync.WaitGroup
}

type runningRequest struct {
	timeMade     time.Time
	responseChan chan *protocol.Response
}

const (
	REQUEST_RETRY_ATTEMPTS = 3
	MAX_RESPONSE_SIZE      = 1024
	IS_RECONNECTING        = uint32(1)
	IS_CONNECTED           = uint32(0)
	MAX_REQUEST_TIME       = time.Second * 1200
	RECONNECT_RETRY_WAIT   = time.Millisecond * 100
)

func NewProtobufClient(hostAndPort string) *ProtobufClient {
	client := &ProtobufClient{hostAndPort: hostAndPort, requestBuffer: make(map[uint32]*runningRequest), connectionStatus: IS_CONNECTED}
	go func() {
		client.reconnect()
		client.readResponses()
	}()
	go client.peridicallySweepTimedOutRequests()
	return client
}

func (self *ProtobufClient) Close() {
	if self.conn != nil {
		self.conn.Close()
		self.conn = nil
	}
}

// Makes a request to the server. If the responseStream chan is not nil it will expect a response from the server
// with a matching request.Id. The REQUEST_RETRY_ATTEMPTS constant of 3 and the RECONNECT_RETRY_WAIT of 100ms means
// that an attempt to make a request to a downed server will take 300ms to time out.
func (self *ProtobufClient) MakeRequest(request *protocol.Request, responseStream chan *protocol.Response) error {
	if responseStream != nil {
		self.requestBufferLock.Lock()

		// this should actually never happen. The sweeper should clear out dead requests
		// before the uint32 ids roll over.
		if oldReq, alreadyHasRequestById := self.requestBuffer[*request.Id]; alreadyHasRequestById {
			log.Println("ProtobufClient: error, already has a request with this id, must have timed out: ")
			close(oldReq.responseChan)
		}
		self.requestBuffer[*request.Id] = &runningRequest{time.Now(), responseStream}
		self.requestBufferLock.Unlock()
	}

	data, err := request.Encode()
	if err != nil {
		return err
	}

	// retry sending this at least a few times
	for attempts := 0; attempts < REQUEST_RETRY_ATTEMPTS; attempts++ {
		if self.conn == nil {
			self.reconnect()
		} else {
			err = binary.Write(self.conn, binary.LittleEndian, uint32(len(data)))
			if err == nil {
				_, err = self.conn.Write(data)
				if err == nil {
					return nil
				}
			}
			log.Println("ProtobufClient: error making request: ", err)
			// TODO: do something smarter here based on whatever the error is.
			// failed to make the request, reconnect and try again.
			self.reconnect()
		}
	}

	// if we got here it errored out, clear out the request
	self.requestBufferLock.Lock()
	delete(self.requestBuffer, *request.Id)
	self.requestBufferLock.Unlock()
	return err
}

func (self *ProtobufClient) readResponses() {
	message := make([]byte, 0, MAX_RESPONSE_SIZE)
	buff := bytes.NewBuffer(message)
	for {
		if self.conn == nil {
			self.reconnect()
		} else {
			var messageSizeU uint32
			var err error
			if err = binary.Read(self.conn, binary.LittleEndian, &messageSizeU); err == nil {
				messageSize := int64(messageSizeU)
				messageReader := io.LimitReader(self.conn, messageSize)
				if _, err = io.Copy(buff, messageReader); err == nil {
					response, err := protocol.DecodeResponse(buff)
					if err != nil {
						log.Println("ProtobufClient: error unmarshaling response: ", err)
					} else {
						self.sendResponse(response)
					}
				}
			}
		}
		buff.Reset()
	}
}

func (self *ProtobufClient) sendResponse(response *protocol.Response) {
	self.requestBufferLock.RLock()
	req, ok := self.requestBuffer[*response.RequestId]
	self.requestBufferLock.RUnlock()
	if ok {
		req.responseChan <- response
		if *response.Type == protocol.Response_END_STREAM || *response.Type == protocol.Response_WRITE_OK {
			close(req.responseChan)
			self.requestBufferLock.Lock()
			delete(self.requestBuffer, *response.RequestId)
			self.requestBufferLock.Unlock()
		}
	}
}

func (self *ProtobufClient) reconnect() {
	swapped := atomic.CompareAndSwapUint32(&self.connectionStatus, IS_CONNECTED, IS_RECONNECTING)

	// if it's not swapped, some other goroutine is already handling the reconect. Wait for it
	if !swapped {
		self.reconnectWait.Wait()
		return
	}
	self.reconnectWait.Add(1)

	self.Close()
	conn, err := net.Dial("tcp", self.hostAndPort)
	if err == nil {
		self.conn = conn
		log.Println("ProtobufClient: connected to ", self.hostAndPort)
	} else {
		log.Println("ProtobufClient: failed to connect to ", self.hostAndPort)
		time.Sleep(RECONNECT_RETRY_WAIT)
	}
	self.connectionStatus = IS_CONNECTED
	self.reconnectWait.Done()
	return
}

func (self *ProtobufClient) peridicallySweepTimedOutRequests() {
	for {
		time.Sleep(time.Minute)
		self.requestBufferLock.Lock()
		maxAge := time.Now().Add(-MAX_REQUEST_TIME)
		for k, req := range self.requestBuffer {
			if req.timeMade.Before(maxAge) {
				delete(self.requestBuffer, k)
				log.Println("Request timed out.")
			}
		}
		self.requestBufferLock.Unlock()
	}
}
