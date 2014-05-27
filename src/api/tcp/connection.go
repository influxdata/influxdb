package tcp

import (
	. "common"
	"net"
	log "code.google.com/p/log4go"
	"bytes"
	"io"
	"encoding/binary"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"errors"
)

const (
	STATE_INITIALIZED State = 1
	STATE_AUTHENTICATED State = 2
)

type ConnectionError struct {
	s string
}

func (e *ConnectionError) Error() string {
	return e.s
}

type ConnectionResetError struct {
	s string
}

func (e *ConnectionResetError) Error() string {
	return e.s
}

type State int32

type Buffer struct {
	ReadBuffer *bytes.Buffer
	WriteBuffer *bytes.Buffer
}

type Connection struct {
	Socket net.Conn
	Address net.Addr
	Buffer *Buffer

	Sequence uint32
	Database string

	User User
	AccountType Account_AccountType
	State State

	Ticker *time.Ticker
	Connected time.Time
	yield func(conn *Connection, time time.Time)
}

func (self *Connection) ResetState() {
	self.Buffer.ReadBuffer.Reset()
	self.Buffer.WriteBuffer.Reset()
	self.Sequence = 0
	self.Database = ""
	self.User = nil
	self.AccountType = 0
	self.State = STATE_INITIALIZED
}

func NewConnection(socket net.Conn, yield func(conn *Connection, time time.Time)) *Connection {
	conn := &Connection{
		Socket: socket,
		Address: socket.RemoteAddr(),
		Database: "debug",
		State: STATE_INITIALIZED,
		Connected: time.Now(),
		Ticker:  time.NewTicker(10 * time.Second),
		yield: yield,
	}

	readMessage := make([]byte, 0, MAX_REQUEST_SIZE)
	writeMessage := make([]byte, 0, MAX_REQUEST_SIZE)

	buffer := &Buffer{}
	buffer.ReadBuffer = bytes.NewBuffer(readMessage)
	buffer.WriteBuffer = bytes.NewBuffer(writeMessage)
	conn.Buffer = buffer

	go func() {
		for t := range conn.Ticker.C {
			conn.yield(conn, t)
		}
	}()

	return conn
}

func (self *Connection) IncrementSequence() {
	self.Sequence++
}

func (self *Connection) SetSequenceFromMessage(message interface{}) {
	if req, ok := message.(*Greeting); ok {
		self.Sequence = req.GetSequence()
	} else if req, ok := message.(*Command); ok {
		self.Sequence = req.GetSequence()
	}
}

func (self *Connection) IsAlived() bool {
	return self.Socket != nil
}

func (self *Connection) readBuffer() (error) {
	var messageSizeU uint32
	var err error

	self.Buffer.ReadBuffer.Reset()
	err = binary.Read(self.Socket, binary.LittleEndian, &messageSizeU)
	if err != nil {
		return err
	}

	size := int64(messageSizeU)
	reader := io.LimitReader(self.Socket, size)
	_, err = io.Copy(self.Buffer.ReadBuffer, reader)
	if err != nil {
		return err
	}
	return nil
}

func (self *Connection) ReadMessage(message interface{}) error {
	err := self.readBuffer()
	if err != nil {
		return err
	}

	if greet, ok := message.(*Greeting); ok {
		err = proto.Unmarshal(self.Buffer.ReadBuffer.Bytes(), greet)
		if err != nil {
			return err
		}
	} else if command, ok := message.(*Command); ok {
		err = proto.Unmarshal(self.Buffer.ReadBuffer.Bytes(), command)
		if err != nil {
			return err
		}
	}

	return nil
}

func (self *Connection) WriteRequest(request interface{}) (error){
	if greeting, ok := request.(*Greeting); ok {
		data , err := proto.Marshal(greeting)
		if err != nil {
			return err
		}
		return self.Write(uint32(len(data)), bytes.NewReader(data))
	} else if command, ok := request.(*Command); ok {
		data , err := proto.Marshal(command)
		if err != nil {
			return err
		}
		return self.Write(uint32(len(data)), bytes.NewReader(data))
	} else {
		return errors.New("does not supported");
	}
}


func (self *Connection) Write(length uint32, reader *bytes.Reader) (error){
	var err error
	self.Buffer.WriteBuffer.Reset()

	defer func() {
		self.Buffer.WriteBuffer.Reset();
	}()

	binary.Write(self.Buffer.WriteBuffer, binary.LittleEndian, length)
	_, err = io.Copy(self.Buffer.WriteBuffer, reader);
	if err != nil {
		return err
	}

	_, err = self.Socket.Write(self.Buffer.WriteBuffer.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (self *Connection) Close() {
	log.Debug("[Connection Closed]")
	self.Ticker.Stop()
	self.Socket.Close()
}
