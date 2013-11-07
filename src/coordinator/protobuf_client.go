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
	requestId         uint32
	requestBufferLock sync.RWMutex
	requestBuffer     map[uint32]chan *protocol.Response
	reconnectLock     sync.Mutex
	reconnecting      bool
	reconnectWait     sync.WaitGroup
}

const (
	REQUEST_RETRY_ATTEMPTS = 3
	MAX_RESPONSE_SIZE      = 1024
)

func NewProtobufClient(hostAndPort string) *ProtobufClient {
	client := &ProtobufClient{hostAndPort: hostAndPort, requestBuffer: make(map[uint32]chan *protocol.Response)}
	go func() {
		client.reconnect()
		client.readResponses()
	}()
	return client
}

func (self *ProtobufClient) Close() {
	if self.conn != nil {
		self.conn.Close()
		self.conn = nil
	}
}

func (self *ProtobufClient) MakeRequest(request *protocol.Request, responseStream chan *protocol.Response) error {
	id := atomic.AddUint32(&self.requestId, 1)
	request.Id = &id
	self.requestBufferLock.Lock()
	if oldResponseChannel, alreadyHasRequestById := self.requestBuffer[id]; alreadyHasRequestById {
		log.Println("ProtobufClient: error, already has a request with this id, must have timed out: ")
		close(oldResponseChannel)
	}
	self.requestBuffer[id] = responseStream
	self.requestBufferLock.Unlock()

	data, err := request.Encode()
	if err != nil {
		return err
	}

	// retry sending this at least a few times
	for attempts := 0; attempts < REQUEST_RETRY_ATTEMPTS; attempts++ {
		err = binary.Write(self.conn, binary.LittleEndian, uint32(len(data)))
		if err == nil {
			_, err = self.conn.Write(data)
			if err == nil {
				return nil
			}
		} else {
			log.Println("Error making request: ", err)
		}
		// TODO: do something smarter here based on whatever the error is.
		// failed to make the request, reconnect and try again.
		self.reconnect()
	}

	// if we got here it errored out, clear out the request
	self.requestBufferLock.Lock()
	delete(self.requestBuffer, id)
	self.requestBufferLock.Unlock()
	return err
}

func (self *ProtobufClient) readResponses() {
	message := make([]byte, 0, MAX_RESPONSE_SIZE)
	buff := bytes.NewBuffer(message)
	for {
		var messageSizeU uint32
		if err := binary.Read(self.conn, binary.LittleEndian, &messageSizeU); err == nil {
			messageSize := int64(messageSizeU)
			messageReader := io.LimitReader(self.conn, messageSize)
			if _, err := io.Copy(buff, messageReader); err == nil {
				response, err := protocol.DecodeResponse(buff)
				if err != nil {
					log.Println("Error unmarshaling response: ", err)
				}
				self.requestBufferLock.RLock()
				responseChan, ok := self.requestBuffer[*response.RequestId]
				self.requestBufferLock.RUnlock()
				if ok {
					responseChan <- response
					if *response.Type == protocol.Response_END_STREAM {
						close(responseChan)
					}
				}
			} else {
				// TODO: do something smarter based on the error
				self.reconnect()
			}
		} else {
			// TODO: do something smarter based on the error
			self.reconnect()
		}
		buff.Reset()
	}
}

func (self *ProtobufClient) reconnect() {
	self.reconnectLock.Lock()
	if self.reconnecting {
		self.reconnectLock.Unlock()
		self.reconnectWait.Wait()
		return
	}
	self.reconnectWait.Add(1)
	self.reconnecting = true
	self.reconnectLock.Unlock()

	self.Close()
	attempts := 0
	for {
		attempts++
		conn, err := net.Dial("tcp", self.hostAndPort)
		if err == nil {
			self.conn = conn
			log.Println("Connected to ", self.hostAndPort)
			self.reconnectLock.Lock()
			self.reconnecting = false
			self.reconnectWait.Done()
			self.reconnectLock.Unlock()
			return
		} else {
			if attempts%100 == 0 {
				log.Println("Failed to connect to ", self.hostAndPort, " after 10 seconds. Continuing to retry...")
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}
