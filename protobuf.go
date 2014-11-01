package influxdb

// NOTE(benbjohnson): Only writes need to be forwarded to broker leader.

/*
import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/protocol"
)

const (
	// maxProtobufRequestSize is the largest protobuf request size allowed.
	maxProtobufRequestSize = 2 * (1 << 20) // 2MB

	// maxProtobufResponseSize is the largest protobuf response size allowed.
	maxProtobufResponseSize = 2 * (1 << 20) // 2MB

	// maxProtobufRequestTimeout is the duration before a request times out.
	maxProtobufRequestTimeout = 1200 * time.Second

	// protobufRetryCount is the maximum number of times a retry is attempted.
	protobufRetryCount = 2

	// protobufRetryTimeout is the time between retry attempts.
	protobufRetryTimeout = 100 * time.Millisecond
)

type ProtobufServer struct {
	server            *Server
	port              string
	listener          net.Listener
	connectionMapLock sync.Mutex
	connectionMap     map[net.Conn]bool
}

func NewProtobufServer(server *Server, port string) *ProtobufServer {
	return &ProtobufServer{
		server:        server,
		port:          port,
		connectionMap: make(map[net.Conn]bool),
	}
}

func (s *ProtobufServer) Close() {
	s.listener.Close()
	s.connectionMapLock.Lock()
	defer s.connectionMapLock.Unlock()
	for conn := range s.connectionMap {
		conn.Close()
	}

	// loop while the port is still accepting connections
	for {
		_, port, _ := net.SplitHostPort(s.port)
		conn, err := net.Dial("tcp", "localhost:"+port)
		if err != nil {
			log.Error("Received error %s, assuming connection is closed.", err)
			break
		}
		conn.Close()

		log.Info("Waiting while the server port is closing")
		time.Sleep(1 * time.Second)
	}
}

func (s *ProtobufServer) ListenAndServe() {
	ln, err := net.Listen("tcp", s.port)
	if err != nil {
		panic(err)
	}
	s.listener = ln
	log.Info("ProtobufServer listening on %s", s.port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Error("Error with TCP connection. Assuming server is closing: %s", err)
			break
		}
		s.connectionMapLock.Lock()
		s.connectionMap[conn] = true
		s.connectionMapLock.Unlock()
		go s.handleConnection(conn)
	}
}

func (s *ProtobufServer) handleConnection(conn net.Conn) {
	log.Info("ProtobufServer: client connected: %s", conn.RemoteAddr().String())

	message := make([]byte, 0, maxProtobufRequestSize)
	buff := bytes.NewBuffer(message)
	var messageSizeU uint32
	for {
		err := binary.Read(conn, binary.LittleEndian, &messageSizeU)
		if err != nil {
			log.Error("Error reading from connection (%s): %s", conn.RemoteAddr().String(), err)
			s.connectionMapLock.Lock()
			delete(s.connectionMap, conn)
			s.connectionMapLock.Unlock()
			conn.Close()
			return
		}

		messageSize := int64(messageSizeU)
		if messageSize > maxProtobufRequestSize {
			err = s.handleRequestTooLarge(conn, messageSize)
		} else {
			err = s.handleRequest(conn, messageSize, buff)
		}

		if err != nil {
			log.Error("Error, closing connection: %s", err)
			s.connectionMapLock.Lock()
			delete(s.connectionMap, conn)
			s.connectionMapLock.Unlock()
			conn.Close()
			return
		}
		buff.Reset()
	}
}

func (s *ProtobufServer) handleRequest(conn net.Conn, messageSize int64, buff *bytes.Buffer) error {
	reader := io.LimitReader(conn, messageSize)
	_, err := io.Copy(buff, reader)
	if err != nil {
		return err
	}
	request, err := protocol.DecodeRequest(buff)
	if err != nil {
		return err
	}

	log.Debug("Received %s", request)

	switch *request.Type {
	case protocol.Request_WRITE:
		go s.handleWrites(request, conn)
	case protocol.Request_QUERY:
		go s.handleQuery(request, conn)
	case protocol.Request_HEARTBEAT:
		response := &protocol.Response{
			RequestId: request.Id,
			Type:      protocol.Response_HEARTBEAT.Enum(),
		}
		return s.WriteResponse(conn, response)
	default:
		log.Error("unknown request type: %v", request)
		return errors.New("Unknown request type")
	}
	return nil
}

func (s *ProtobufServer) handleWrites(request *protocol.Request, conn net.Conn) {
	shard := s.server.GetLocalShardById(*request.ShardId)
	log.Debug("HANDLE: (%d):%d:%v", s.server.LocalServer.Id, request.GetId(), shard)
	err := shard.WriteLocalOnly(request)
	var response *protocol.Response
	if err != nil {
		log.Error("ProtobufServer: error writing local shard: %s", err)
		response = &protocol.Response{
			RequestId:    request.Id,
			Type:         protocol.Response_ERROR.Enum(),
			ErrorMessage: protocol.String(err.Error()),
		}
	} else {
		response = &protocol.Response{
			RequestId: request.Id,
			Type:      protocol.Response_END_STREAM.Enum(),
		}
	}
	if err := s.WriteResponse(conn, response); err != nil {
		log.Error("ProtobufServer: error writing local shard: %s", err)
	}
}

func (s *ProtobufServer) handleQuery(request *protocol.Request, conn net.Conn) {
	// the query should always parse correctly since it was parsed at the originating server.
	queries, err := parser.ParseQuery(*request.Query)
	if err != nil || len(queries) < 1 {
		log.Error("Error parsing query: ", err)
		errorMsg := fmt.Sprintf("Cannot find user %s", *request.UserName)
		response := &protocol.Response{
			Type:         protocol.Response_ERROR.Enum(),
			ErrorMessage: &errorMsg,
			RequestId:    request.Id,
		}
		s.WriteResponse(conn, response)
		return
	}
	query := queries[0]
	var user common.User
	if *request.IsDbUser {
		user = s.clusterConfig.GetDbUser(*request.Database, *request.UserName)
	} else {
		user = s.clusterConfig.GetClusterAdmin(*request.UserName)
	}

	if user == nil {
		errorMsg := fmt.Sprintf("Cannot find user %s", *request.UserName)
		response := &protocol.Response{
			Type:         protocol.Response_ERROR.Enum(),
			ErrorMessage: &errorMsg,
			RequestId:    request.Id,
		}
		s.WriteResponse(conn, response)
		return
	}

	shard := s.clusterConfig.GetLocalShardById(*request.ShardId)

	querySpec := parser.NewQuerySpec(user, *request.Database, query)

	responseChan := make(chan *protocol.Response)
	if querySpec.IsDestructiveQuery() {
		go shard.HandleDestructiveQuery(querySpec, request, responseChan, true)
	} else {
		go shard.Query(querySpec, responseChan)
	}
	for {
		response := <-responseChan
		response.RequestId = request.Id
		s.WriteResponse(conn, response)

		switch rt := response.GetType(); rt {
		case protocol.Response_END_STREAM,
			protocol.Response_ERROR:
			return
		case protocol.Response_QUERY:
			continue
		default:
			panic(fmt.Errorf("Unexpected response type: %s", rt))
		}
	}
}

func (s *ProtobufServer) WriteResponse(conn net.Conn, response *protocol.Response) error {
	if response.Size() >= maxProtobufResponseSize {
		f, s := splitResponse(response)
		err := s.WriteResponse(conn, f)
		if err != nil {
			return err
		}
		return s.WriteResponse(conn, s)
	}

	data, err := response.Encode()
	if err != nil {
		log.Error("error encoding response: %s", err)
		return err
	}

	buff := bytes.NewBuffer(make([]byte, 0, len(data)+8))
	binary.Write(buff, binary.LittleEndian, uint32(len(data)))
	_, err = conn.Write(append(buff.Bytes(), data...))
	if err != nil {
		log.Error("error writing response: %s", err)
		return err
	}
	return nil
}

func (s *ProtobufServer) handleRequestTooLarge(conn net.Conn, messageSize int64) error {
	log.Error("request too large, dumping: %s -> %s (%d)", conn.RemoteAddr(), conn.LocalAddr(), messageSize)
	reader := io.LimitReader(conn, messageSize)
	_, err := io.Copy(ioutil.Discard, reader)
	if err != nil {
		return err
	}
	return s.sendErrorResponse(conn, "request too large")
}

func (s *ProtobufServer) sendErrorResponse(conn net.Conn, message string) error {
	response := &protocol.Response{
		Type:         protocol.Response_ERROR.Enum(),
		ErrorMessage: protocol.String(message),
	}
	data, err := response.Encode()
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(make([]byte, 0, len(data)+4))
	err = binary.Write(buff, binary.LittleEndian, uint32(len(data)))

	if err != nil {
		return err
	}

	_, err = conn.Write(append(buff.Bytes(), data...))
	return err
}

func splitResponse(response *protocol.Response) (f, s *protocol.Response) {
	f = &protocol.Response{}
	s = &protocol.Response{}
	*f = *response
	*s = *response

	if l := len(response.MultiSeries); l > 1 {
		f.MultiSeries = f.MultiSeries[:l/2]
		s.MultiSeries = s.MultiSeries[l/2:]
		return
	}

	l := len(response.MultiSeries[0].Points)
	f.MultiSeries[0].Points = f.MultiSeries[0].Points[:l/2]
	s.MultiSeries[0].Points = s.MultiSeries[0].Points[l/2:]
	return
}

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
	r        ResponseChannel
	request  *protocol.Request
}

func NewProtobufClient(hostAndPort string, writeTimeout time.Duration) *ProtobufClient {
	log.Debug("NewProtobufClient: ", hostAndPort)
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

func (c *ProtobufClient) Connect() {
	c.once.Do(c.connect)
}

func (c *ProtobufClient) connect() {
	c.reconChan <- struct{}{}
	go func() {
		c.reconnect()
		c.readResponses()
	}()
	go c.peridicallySweepTimedOutRequests()
}

func (c *ProtobufClient) Close() {
	c.connLock.Lock()
	defer c.connLock.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.stopped = true
		c.conn = nil
	}
	c.ClearRequests()
}

func (c *ProtobufClient) getConnection() net.Conn {
	c.connLock.Lock()
	defer c.connLock.Unlock()
	return c.conn
}

func (c *ProtobufClient) ClearRequests() {
	c.requestBufferLock.Lock()
	defer c.requestBufferLock.Unlock()

	for _, req := range c.requestBuffer {
		c.cancelRequest(req.request)
	}

	c.requestBuffer = map[uint32]*runningRequest{}
}

func (c *ProtobufClient) CancelRequest(request *protocol.Request) {
	c.requestBufferLock.Lock()
	defer c.requestBufferLock.Unlock()
	c.cancelRequest(request)
}

func (c *ProtobufClient) cancelRequest(request *protocol.Request) {
	req, ok := c.requestBuffer[*request.Id]
	if !ok {
		return
	}
	message := "cancelling request"
	req.r.Yield(&protocol.Response{
		Type:         protocol.Response_ERROR.Enum(),
		ErrorMessage: &message,
	})
	delete(c.requestBuffer, *request.Id)
}

// Makes a request to the server. If the responseStream chan is not nil it will expect a response from the server
// with a matching request.Id. The REQUEST_RETRY_ATTEMPTS constant of 3 and the RECONNECT_RETRY_WAIT of 100ms means
// that an attempt to make a request to a downed server will take 300ms to time out.
func (c *ProtobufClient) MakeRequest(request *protocol.Request, r cluster.ResponseChannel) error {
	if request.Id == nil {
		id := atomic.AddUint32(&c.lastRequestId, uint32(1))
		request.Id = &id
	}
	if r != nil {
		c.requestBufferLock.Lock()

		// this should actually never happen. The sweeper should clear out dead requests
		// before the uint32 ids roll over.
		if oldReq, alreadyHasRequestById := c.requestBuffer[*request.Id]; alreadyHasRequestById {
			message := "already has a request with this id, must have timed out"
			log.Error(message)
			oldReq.r.Yield(&protocol.Response{
				Type:         protocol.Response_ERROR.Enum(),
				ErrorMessage: &message,
			})
		}
		c.requestBuffer[*request.Id] = &runningRequest{timeMade: time.Now(), r: r, request: request}
		c.requestBufferLock.Unlock()
	}

	data, err := request.Encode()
	if err != nil {
		return err
	}

	conn := c.getConnection()
	if conn == nil {
		conn = c.reconnect()
		if conn == nil {
			return fmt.Errorf("Failed to connect to server %s", c.hostAndPort)
		}
	}

	if c.writeTimeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	buff := bytes.NewBuffer(make([]byte, 0, len(data)+8))
	binary.Write(buff, binary.LittleEndian, uint32(len(data)))
	buff.Write(data)
	_, err = conn.Write(buff.Bytes())

	if err == nil {
		return nil
	}

	// if we got here it errored out, clear out the request
	c.requestBufferLock.Lock()
	delete(c.requestBuffer, *request.Id)
	c.requestBufferLock.Unlock()
	c.reconnect()
	return err
}

func (c *ProtobufClient) readResponses() {
	message := make([]byte, 0, MAX_RESPONSE_SIZE)
	buff := bytes.NewBuffer(message)
	for !c.stopped {
		buff.Reset()
		conn := c.getConnection()
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
			c.sendResponse(response)
		}
	}
}

func (c *ProtobufClient) sendResponse(response *protocol.Response) {
	c.requestBufferLock.RLock()
	req, ok := c.requestBuffer[*response.RequestId]
	c.requestBufferLock.RUnlock()
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

	c.requestBufferLock.Lock()
	req, ok = c.requestBuffer[*response.RequestId]
	if deleteRequest {
		delete(c.requestBuffer, *response.RequestId)
	}
	c.requestBufferLock.Unlock()
	if !ok {
		return
	}

	log.Debug("ProtobufClient yielding to %s %s", req.r.Name(), response)
	req.r.Yield(response)
}

func (c *ProtobufClient) reconnect() net.Conn {
	select {
	case <-c.reconChan:
		c.reconGroup.Add(1)
		defer func() {
			c.reconGroup.Done()
			c.reconChan <- struct{}{}
		}()
	default:
		c.reconGroup.Wait()
		return c.conn
	}

	if c.conn != nil {
		c.conn.Close()
	}
	conn, err := net.DialTimeout("tcp", c.hostAndPort, c.writeTimeout)
	if err != nil {
		c.attempts++
		if c.attempts < 100 {
			return nil
		}

		log.Error("failed to connect to %s %d times", c.hostAndPort, c.attempts)
		c.attempts = 0
	}

	c.conn = conn
	log.Info("connected to %s", c.hostAndPort)
	return conn
}

func (c *ProtobufClient) peridicallySweepTimedOutRequests() {
	for {
		time.Sleep(time.Minute)
		c.requestBufferLock.Lock()
		maxAge := time.Now().Add(-maxProtobufRequestTimeout)
		for k, req := range c.requestBuffer {
			if req.timeMade.Before(maxAge) {
				delete(c.requestBuffer, k)
				log.Warn("Request timed out: ", req.request)
			}
		}
		c.requestBufferLock.Unlock()
	}
}
*/
