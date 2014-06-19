package tcp_test

import (
	. "launchpad.net/gocheck"
	"testing"

	"cluster"
	. "common"
	"coordinator"
	"encoding/binary"
	"parser"
	"protocol"

	"api/tcp"
	"bytes"
	"fmt"
	"io"
	"net"
)

func Test(t *testing.T) { TestingT(t) }

type MockServer struct {
	Coordinator coordinator.Coordinator
}

type MockCoordinator struct {
	coordinator.Coordinator
	series            []*protocol.Series
	continuousQueries map[string][]*cluster.ContinuousQuery
	deleteQueries     []*parser.DeleteQuery
	db                string
	droppedDb         string
	returnedError     error
}

func (self *MockCoordinator) RunQuery(_ User, _ string, query string, yield coordinator.SeriesWriter) error {
	fmt.Printf("Query: %s\n", query)
	return nil
}

func (self *MockCoordinator) CreateDatabase(_ User, db string) error {
	self.db = db
	return nil
}

func (self *MockCoordinator) ListDatabases(_ User) ([]*cluster.Database, error) {
	return []*cluster.Database{
		&cluster.Database{
			Name: "db",
		},
	}, nil
}

func (self *MockCoordinator) DropDatabase(_ User, db string) error {
	return nil
}

func (self *MockCoordinator) WriteSeriesData(_ User, db string, series []*protocol.Series) error {
	self.series = append(self.series, series...)
	return nil
}

func (self *MockServer) GetCoordinator() coordinator.Coordinator {
	return self.Coordinator
}

func (self *MockServer) SendErrorMessage(conn tcp.Connection, t tcp.Command_CommandType, message string) error {
	return nil
}
func (self *MockServer) IsForceSSLUser(name string) bool {
	return false
}

func (self *MockServer) HandleConnection(conn tcp.Connection) {

}

func (self *MockServer) Authenticate(conn tcp.Connection, info *tcp.Greeting_Authentication) error {
	conn.SetAccountType(tcp.G_Authentication_CLUSTER_ADMIN)
	return nil
}

func (self *MockServer) RemoveConnection(conn tcp.Connection) {
}

type MockConnection struct {
	Socket      net.Conn
	Address     net.Addr
	Sequence    uint32
	Database    string
	User        User
	AccountType tcp.Greeting_Authentication_AccountType
	State       tcp.State
	Value       []byte
	Command     *tcp.Command
	Messages    []*tcp.Greeting
	ReceivedMessages []*tcp.Greeting
	Offset      int
}

func (self *MockConnection) GetUser() User {
	return self.User
}
func (self *MockConnection) SetUser(user User) {
}
func (self *MockConnection) GetDatabase() string {
	return self.Database
}
func (self *MockConnection) SetDatabase(name string) {
	self.Database = name
}
func (self *MockConnection) GetSequence() uint32 {
	return 0
}

func (self *MockServer) SSLAvailable() bool {
	return true
}

func (self *MockConnection) WriteRequest(request interface{}) error {
	if v, ok := request.(*tcp.Command); ok {
		self.Command = v
	} else if v, ok := request.(*tcp.Greeting); ok {
		self.ReceivedMessages = append(self.ReceivedMessages, v)
	}
	return nil
}

func (self *MockConnection) GetRequest() *tcp.Command {
	return self.Command
}
func (self *MockConnection) Close()      {}
func (self *MockConnection) ResetState() {}

func (self *MockConnection) PrepareGreetingMessages(message []*tcp.Greeting) {
	self.Messages = message
}

func (self *MockConnection) ReadMessage(message interface{}) error {
	if self.Offset >= len(self.Messages){
		return nil
	}

	if g, ok := message.(*tcp.Greeting); ok {
		*g = *self.Messages[self.Offset]
		self.Offset++
		return nil
	}

	return nil
}

func (self *MockConnection) IncrementSequence() {
}
func (self *MockConnection) SetState(tcp.State) {
}
func (self *MockConnection) GetState() tcp.State {
	return self.State
}
func (self *MockConnection) SetAccountType(t tcp.Greeting_Authentication_AccountType) {
	self.AccountType = t
}
func (self *MockConnection) GetAccountType() *tcp.Greeting_Authentication_AccountType {
	return &self.AccountType
}
func (self *MockConnection) GetSocket() net.Conn {
	return self.Socket
}
func (self *MockConnection) SetSocket(net.Conn) {
}
func (self *MockConnection) ClearBuffer() {
}
func (self *MockConnection) GetAddress() net.Addr {
	return self.Address
}

func (self *MockConnection) Write(length uint32, reader *bytes.Reader) error {
	var err error
	buffer := bytes.NewBuffer(self.Value)

	binary.Write(buffer, binary.LittleEndian, length)
	_, err = io.Copy(buffer, reader)
	if err != nil {
		return err
	}
	io.Copy(buffer, reader)
	self.Value = buffer.Bytes()
	return nil
}

func (self *MockConnection) GetValue() []byte {
	return self.Value
}

func (self *MockConnection) IsAlived() bool {
	return true
}

func getMock() (tcp.RequestHandler, *MockConnection) {
	handler := tcp.RequestHandler{
		Server: &MockServer{
			Coordinator: &MockCoordinator{},
		},
	}

	conn := &MockConnection{
		Value: []byte{},
	}
	return handler, conn
}
