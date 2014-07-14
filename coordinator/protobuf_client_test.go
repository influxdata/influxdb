package coordinator

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	log "code.google.com/p/log4go"
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
			response := &protocol.Response{RequestId: request.Id, Type: &heartbeatResponse}

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

func FakeHearbeatServer() *PingResponseServer {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	prs := PingResponseServer{Listener: l}
	prs.Start()
	return &prs
}

func BenchmarkSingle(b *testing.B) {
	var HEARTBEAT_TYPE = protocol.Request_HEARTBEAT
	prs := FakeHearbeatServer()
	client := NewProtobufClient(prs.Listener.Addr().String(), time.Second)
	client.Connect()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		responseChan := make(chan *protocol.Response, 1)
		heartbeatRequest := &protocol.Request{
			Type:     &HEARTBEAT_TYPE,
			Database: protocol.String(""),
		}
		client.MakeRequest(heartbeatRequest, responseChan)
		<-responseChan
	}
}
