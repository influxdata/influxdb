package coordinator

import (
	"bytes"
	log "code.google.com/p/log4go"
	"encoding/binary"
	"io"
	"net"
	"protocol"
	"sync"
	"time"
)

type ProtobufServer struct {
	listener          net.Listener
	port              string
	requestHandler    RequestHandler
	connectionMapLock sync.Mutex
	connectionMap     map[net.Conn]bool
}

const KILOBYTE = 1024
const MEGABYTE = 1024 * KILOBYTE
const MAX_REQUEST_SIZE = MEGABYTE * 2

func NewProtobufServer(port string, requestHandler RequestHandler) *ProtobufServer {
	server := &ProtobufServer{port: port, requestHandler: requestHandler, connectionMap: make(map[net.Conn]bool)}
	return server
}

func (self *ProtobufServer) Close() {
	self.listener.Close()
	self.connectionMapLock.Lock()
	defer self.connectionMapLock.Unlock()
	for conn, _ := range self.connectionMap {
		conn.Close()
	}

	// loop while the port is still accepting connections
	for {
		_, port, _ := net.SplitHostPort(self.port)
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

func (self *ProtobufServer) ListenAndServe() {
	ln, err := net.Listen("tcp", self.port)
	if err != nil {
		panic(err)
	}
	self.listener = ln
	log.Info("ProtobufServer listening on %s", self.port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Error("Error with TCP connection. Assuming server is closing: %s", err)
			break
		}
		self.connectionMapLock.Lock()
		self.connectionMap[conn] = true
		self.connectionMapLock.Unlock()
		go self.handleConnection(conn)
	}
}

func (self *ProtobufServer) handleConnection(conn net.Conn) {
	log.Info("ProtobufServer: client connected: %s", conn.RemoteAddr().String())

	message := make([]byte, 0, MAX_REQUEST_SIZE)
	buff := bytes.NewBuffer(message)
	var messageSizeU uint32
	for {
		err := binary.Read(conn, binary.LittleEndian, &messageSizeU)
		if err != nil {
			log.Error("Error reading from connection (%s): %s", conn.RemoteAddr().String(), err)
			self.connectionMapLock.Lock()
			delete(self.connectionMap, conn)
			self.connectionMapLock.Unlock()
			conn.Close()
			return
		}

		messageSize := int64(messageSizeU)
		if messageSize > MAX_REQUEST_SIZE {
			err = self.handleRequestTooLarge(conn, messageSize, buff)
		} else {
			err = self.handleRequest(conn, messageSize, buff)
		}

		if err != nil {
			log.Error("Error, closing connection: %s", err)
			self.connectionMapLock.Lock()
			delete(self.connectionMap, conn)
			self.connectionMapLock.Unlock()
			conn.Close()
			return
		}
		buff.Reset()
	}
}

func (self *ProtobufServer) handleRequest(conn net.Conn, messageSize int64, buff *bytes.Buffer) error {
	reader := io.LimitReader(conn, messageSize)
	_, err := io.Copy(buff, reader)
	if err != nil {
		return err
	}
	request, err := protocol.DecodeRequest(buff)
	if err != nil {
		return err
	}

	return self.requestHandler.HandleRequest(request, conn)
}

func (self *ProtobufServer) handleRequestTooLarge(conn net.Conn, messageSize int64, buff *bytes.Buffer) error {
	log.Error("request too large, dumping: %s (%d)", conn.RemoteAddr().String(), messageSize)
	for messageSize > 0 {
		reader := io.LimitReader(conn, MAX_REQUEST_SIZE)
		_, err := io.Copy(buff, reader)
		if err != nil {
			return err
		}
		messageSize -= MAX_REQUEST_SIZE
		buff.Reset()
	}
	return self.sendErrorResponse(conn, protocol.Response_REQUEST_TOO_LARGE, "request too large")
}

func (self *ProtobufServer) sendErrorResponse(conn net.Conn, code protocol.Response_ErrorCode, message string) error {
	response := &protocol.Response{ErrorCode: &code, ErrorMessage: &message}
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
