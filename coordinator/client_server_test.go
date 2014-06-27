package coordinator

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/influxdb/influxdb/protocol"
	. "launchpad.net/gocheck"
)

type ClientServerSuite struct{}

var _ = Suite(&ClientServerSuite{})

const DB_DIR = "/tmp/influxdb/datastore_test"

type MockRequestHandler struct {
}

var writeOk = protocol.Response_WRITE_OK

func Test(t *testing.T) {
	TestingT(t)
}

func stringToSeries(seriesString string, c *C) *protocol.Series {
	series := &protocol.Series{}
	err := json.Unmarshal([]byte(seriesString), &series)
	c.Assert(err, IsNil)
	return series
}

func (self *MockRequestHandler) HandleRequest(request *protocol.Request, conn net.Conn) error {
	response := &protocol.Response{RequestId: request.Id, Type: &writeOk}
	data, _ := response.Encode()
	binary.Write(conn, binary.LittleEndian, uint32(len(data)))
	conn.Write(data)
	return nil
}

func (self *ClientServerSuite) TestClientCanMakeRequests(c *C) {
	requestHandler := &MockRequestHandler{}
	protobufServer := NewProtobufServer(":8091", requestHandler)
	go protobufServer.ListenAndServe()
	c.Assert(protobufServer, Not(IsNil))
	protobufClient := NewProtobufClient("localhost:8091", 0)
	protobufClient.Connect()
	responseStream := make(chan *protocol.Response, 1)

	mock := `
  {
    "points": [
      { "values": [{"int64_value": 3}]}
    ],
    "name": "foo",
    "fields": ["val"]
  }`
	fmt.Println("creating series")
	series := stringToSeries(mock, c)
	t := time.Now().Unix()
	s := uint64(1)
	series.Points[0].Timestamp = &t
	series.Points[0].SequenceNumber = &s
	id := uint32(1)
	database := "pauldb"
	proxyWrite := protocol.Request_WRITE
	request := &protocol.Request{Id: &id, Type: &proxyWrite, Database: &database, MultiSeries: []*protocol.Series{series}}

	time.Sleep(time.Second * 1)
	err := protobufClient.MakeRequest(request, responseStream)
	c.Assert(err, IsNil)
	timer := time.NewTimer(time.Second)
	select {
	case <-timer.C:
		c.Error("Timed out waiting for response")
	case response := <-responseStream:
		c.Assert(*response.Type, Equals, protocol.Response_WRITE_OK)
	}
}

func (self *ClientServerSuite) TestClientReconnectsIfDisconnected(c *C) {
}

func (self *ClientServerSuite) TestServerExecutesReplayRequestIfWriteIsOutOfSequence(c *C) {
}

func (self *ClientServerSuite) TestServerKillsOldHandlerWhenClientReconnects(c *C) {

}
