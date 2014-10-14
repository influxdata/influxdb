package tcp

import (
	. "github.com/influxdb/influxdb/common"
	"net"
	"bytes"
)

type State int32

//   ConnectionState: Idle, Send, Receive and Handshake?
const (
	STATE_INITIALIZED State = 1
	STATE_AUTHENTICATED State = 2
	STATE_IDLE State = 3
	STATE_SEND State = 4
	STATE_RECEIVE State = 5
	STATE_SHUTDOWN State = 6
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

// TODO: blush up
type Connection interface {
	GetUser() User
	SetUser(user User)
	GetDatabase() string
	SetDatabase(name string)
	GetSequence() uint32
	WriteRequest(request interface{}) (error)
	Close()
	ResetState()
	ReadMessage(message interface{}) error
	// TODO: remove this. maybe don't need
	IncrementSequence()
	SetState(State)
	GetState() State
	SetAccountType(Greeting_Authentication_AccountType)
	GetAccountType() *Greeting_Authentication_AccountType
	GetSocket() net.Conn
	SetSocket(net.Conn)
	ClearBuffer()
	GetAddress() net.Addr
	Write(length uint32, reader *bytes.Reader) (error)
	IsAlived() bool
}
