package coordinator

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"time"

	"launchpad.net/gocheck"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/protocol"
)

type PingResponseServer struct {
	Listener net.Listener
}

func (prs *PingResponseServer) Start() {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	prs.Listener = l
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				break
			}

			go prs.handleConnection(conn)
		}
	}()
}

func (prs *PingResponseServer) handleConnection(conn net.Conn) {
	message := make([]byte, 0, MAX_REQUEST_SIZE)
	buff := bytes.NewBuffer(message)
	var messageSizeU uint32

	for {
		buff.Reset()
		err := binary.Read(conn, binary.LittleEndian, &messageSizeU)
		if err != nil {
			log.Error("Error reading from connection (%s): %s", conn.RemoteAddr().String(), err)
			return
		}

		_, err = io.CopyN(buff, conn, int64(messageSizeU))

		if err != nil {
			break
		}

		request, err := protocol.DecodeRequest(buff)
		if err != nil {
			break
		}

		switch *request.Type {
		case protocol.Request_HEARTBEAT:
			response := &protocol.Response{RequestId: request.Id, Type: protocol.Response_HEARTBEAT.Enum()}

			data, err := response.Encode()
			if err != nil {
				panic(err)
			}
			binary.Write(conn, binary.LittleEndian, uint32(len(data)))
			_, err = conn.Write(data)
		default:
			panic("Not a heartbeat request")

		}
	}
	conn.Close()
}

func FakeHeartbeatServer() *PingResponseServer {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	prs := PingResponseServer{Listener: l}
	prs.Start()
	return &prs
}

type ProtobufClientSuite struct{}

var _ = gocheck.Suite(&ProtobufClientSuite{})

func (self *ProtobufClientSuite) BenchmarkSingle(c *gocheck.C) {
	var HEARTBEAT_TYPE = protocol.Request_HEARTBEAT
	prs := FakeHeartbeatServer()
	client := NewProtobufClient(prs.Listener.Addr().String(), time.Second)
	client.Connect()
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		responseChan := make(chan *protocol.Response, 1)
		heartbeatRequest := &protocol.Request{
			Type:     &HEARTBEAT_TYPE,
			Database: protocol.String(""),
		}
		rcw := cluster.NewResponseChannelWrapper(responseChan)
		client.MakeRequest(heartbeatRequest, rcw)
		<-responseChan
	}
}

func (pcs *ProtobufClientSuite) TestReadResponsesWhenRemoteClosesConnection(c *gocheck.C) {
	// Channel used to kill the remote connection
	dieCh := make(chan struct{})
	// Channel for remote client to notify test that ProtobufClient connected
	connectedCh := make(chan struct{})
	// Channel the remote client will use to tell the test that it has closed
	connClosedCh := make(chan struct{})

	// Remote connection (talking to a ProtobufClient)
	handleConnFn := func(conn net.Conn) {
		connectedCh <- struct{}{}
		<-dieCh
		conn.Close()
		close(connClosedCh)
	}

	// Remote server listening for ProtobufClient connection requests
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() {
		for {
			conn, _ := l.Accept()
			go handleConnFn(conn)
		}
	}()

	// Create a ProtobufClient and connect to the remote server we just setup
	client := NewProtobufClient(l.Addr().String(), time.Second)
	client.Connect()
	select {
	case <-connectedCh:
	case <-time.After(500 * time.Millisecond):
		c.Errorf("Waiting for ProtobufClient to connect timed out")
		return
	}
	c.Assert(client, gocheck.NotNil)
	c.Assert(client.conn, gocheck.NotNil)

	// Tell remote side to close the connection
	dieCh <- struct{}{}

	// Make sure ProtobufClient set the connection to nil
	<-connClosedCh
	time.Sleep(100 * time.Millisecond)
	c.Assert(client.conn, gocheck.IsNil)
}
