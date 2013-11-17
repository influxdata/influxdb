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
	requestBuffer     map[uint32]chan *protocol.Response
	reconnecting      uint32
	reconnectWait     sync.WaitGroup
}

const (
	REQUEST_RETRY_ATTEMPTS = 3
	MAX_RESPONSE_SIZE      = 1024
	IS_RECONNECTING        = uint32(1)
	IS_CONNECTED           = uint32(0)
)

func NewProtobufClient(hostAndPort string) *ProtobufClient {
	client := &ProtobufClient{hostAndPort: hostAndPort, requestBuffer: make(map[uint32]chan *protocol.Response), reconnecting: IS_CONNECTED}
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
	if responseStream != nil {
		self.requestBufferLock.Lock()
		if oldResponseChannel, alreadyHasRequestById := self.requestBuffer[*request.Id]; alreadyHasRequestById {
			log.Println("ProtobufClient: error, already has a request with this id, must have timed out: ")
			close(oldResponseChannel)
		}
		self.requestBuffer[*request.Id] = responseStream
		self.requestBufferLock.Unlock()
	}

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
			log.Println("ProtobufClient: error making request: ", err)
		}
		// TODO: do something smarter here based on whatever the error is.
		// failed to make the request, reconnect and try again.
		self.reconnect()
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
		var messageSizeU uint32
		if err := binary.Read(self.conn, binary.LittleEndian, &messageSizeU); err == nil {
			messageSize := int64(messageSizeU)
			messageReader := io.LimitReader(self.conn, messageSize)
			if _, err := io.Copy(buff, messageReader); err == nil {
				response, err := protocol.DecodeResponse(buff)
				if err != nil {
					log.Println("ProtobufClient: error unmarshaling response: ", err)
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
	swapped := atomic.CompareAndSwapUint32(&self.reconnecting, self.reconnecting, IS_RECONNECTING)

	// if it's not swapped, some other goroutine is already handling the reconect. Wait for it
	if !swapped {
		self.reconnectWait.Wait()
		return
	}
	self.reconnectWait.Add(1)

	self.Close()
	attempts := 0
	for {
		attempts++
		conn, err := net.Dial("tcp", self.hostAndPort)
		if err == nil {
			self.conn = conn
			log.Println("ProtobufClient: connected to ", self.hostAndPort)
			atomic.CompareAndSwapUint32(&self.reconnecting, self.reconnecting, IS_CONNECTED)
			self.reconnectWait.Done()
			return
		} else {
			if attempts%100 == 0 {
				log.Println("ProtobufClient: failed to connect to ", self.hostAndPort, " after 10 seconds. Continuing to retry...")
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}
