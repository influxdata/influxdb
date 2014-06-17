package tcp_test

import (
	. "launchpad.net/gocheck"
	"code.google.com/p/goprotobuf/proto"

	"protocol"
	. "common"
	"encoding/binary"

	"api/tcp"
	"bytes"
	"fmt"
)

type RequestHandlerSuite struct{}
var _ = Suite(&RequestHandlerSuite{})

func (s *RequestHandlerSuite) SetUpSuite(c *C) {
}

func (s *RequestHandlerSuite) TestPing(c *C) {
	handler, conn := getMock()
	err := handler.Ping(conn, &tcp.Command{
			Type: &tcp.C_PING,
	})

	c.Assert(err, IsNil)
	result := conn.GetRequest()
	c.Assert(*result.Type, Equals, tcp.C_PING)
	c.Assert(*result.Result, Equals, tcp.C_OK)
}

func (s *RequestHandlerSuite) TestCreateDatabase(c *C) {
	handler, conn := getMock()
	dbname := "hello"
	err := handler.CreateDatabase(conn, &tcp.Command{
			Type: &tcp.C_CREATEDATABASE,
			Database: &tcp.Command_Database{
				Name: []string{
					dbname,
				},
			},
		})

	c.Assert(err, IsNil)
	result := conn.GetRequest()
	c.Assert(*result.Type, Equals, tcp.C_CREATEDATABASE)
	c.Assert(*result.Result, Equals, tcp.C_OK)
	coord := handler.Server.GetCoordinator()
	mockcoord, _ := coord.(*MockCoordinator)
	c.Assert(mockcoord.db, Equals, dbname)
}

func (s *RequestHandlerSuite) TestListDatabase(c *C) {
	handler, conn := getMock()
	mockcoord, _ := handler.Server.GetCoordinator().(*MockCoordinator)
	mockcoord.db = "hello"

	err := handler.ListDatabase(conn, &tcp.Command{
			Type: &tcp.C_LISTDATABASE,
			Database: &tcp.Command_Database{
			},
		})

	c.Assert(err, IsNil)
	result := conn.GetRequest()
	fmt.Printf("Result: %+v\n", result)
	c.Assert(*result.Type, Equals, tcp.C_LISTDATABASE)
	c.Assert(*result.Result, Equals, tcp.C_OK)
	c.Assert(result.GetDatabase().Name[0], Equals, "db")
}

func (s *RequestHandlerSuite) TestDropDatabase(c *C) {
	handler, conn := getMock()
	mockcoord, _ := handler.Server.GetCoordinator().(*MockCoordinator)
	mockcoord.db = "hello"

	err := handler.DropDatabase(conn, &tcp.Command{
			Type: &tcp.C_DROPDATABASE,
			Database: &tcp.Command_Database{
				Name: []string{
					"hello",
				},
			},
		})

	c.Assert(err, IsNil)
	result := conn.GetRequest()
	c.Assert(*result.Type, Equals, tcp.C_DROPDATABASE)
	c.Assert(*result.Result, Equals, tcp.C_OK)
}

func (s *RequestHandlerSuite) TestWriteSeries(c *C) {
	handler, conn := getMock()
	mockcoord, _ := handler.Server.GetCoordinator().(*MockCoordinator)

	series := []*protocol.Series{
		&protocol.Series{
			Name: proto.String("chobie"),
			Fields: []string{"value"},
			Points: []*protocol.Point{
				&protocol.Point{
					Values: []*protocol.FieldValue{
						&protocol.FieldValue{
							DoubleValue: proto.Float64(3.0),
						},
					},
				},
			},
		},
	}

	err := handler.WriteSeries(conn, &tcp.Command{
			Type: &tcp.C_WRITESERIES,
			Series: &tcp.Command_Series{
				Series: series,
			},
		})

	c.Assert(err, IsNil)
	result := conn.GetRequest()

	c.Assert(*result.Type, Equals, tcp.C_WRITESERIES)
	c.Assert(*result.Result, Equals, tcp.C_OK)
	c.Assert(mockcoord.series, DeepEquals, series)
}

func (s *RequestHandlerSuite) TestChangeDatabase(c *C) {
	handler, conn := getMock()

	err := handler.ChangeDatabase(conn, &tcp.Command{
			Type: &tcp.C_CHANGEDATABASE,
			Database: &tcp.Command_Database{
				Name: []string{
					"change",
				},
			},
		})

	c.Assert(err, IsNil)
	result := conn.GetRequest()
	c.Assert(*result.Type, Equals, tcp.C_CHANGEDATABASE)
	c.Assert(*result.Result, Equals, tcp.C_OK)
	c.Assert(conn.GetDatabase(), Equals, "change")
}

// TODO: CloseでError返すのおかしいよねー
func (s *RequestHandlerSuite) TestCloseConnection(c *C) {
	handler, conn := getMock()
	_ = handler.CloseConnection(conn, &tcp.Command{
		Type: &tcp.C_CLOSE,
	})

	//	c.Assert(err, Equals, tcp.ConnectionError)
}

//func (self *RequestHandler) Query(conn Connection, request *Command) error {
func (s *RequestHandlerSuite) TestQuery(c *C) {
	handler, conn := getMock()

	fmt.Printf("QUERY\n")
	err := handler.Query(conn, &tcp.Command{
		Type: &tcp.C_QUERY,
		Query: &tcp.Command_Query{
			Query: []byte("select * from series"),
		},
	})

	c.Assert(err, IsNil)
	result := conn.GetValue()

	var length uint32
	reader := bytes.NewReader(result)
	binary.Read(reader, binary.LittleEndian, &length)
	v := tcp.Command{}
	err = proto.Unmarshal(result[4:], &v)

	c.Assert(*v.Type, Equals, tcp.C_QUERY)
	c.Assert(*v.Result, Equals, tcp.C_OK)
}

// TODO
//func (self *RequestHandler) ResetConnection(conn Connection, request *Command) error {
//func (self *RequestHandler) HandleRequest(conn Connection) error {
