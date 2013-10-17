package hapi

import (
	"common"
	"encoding/json"
	"fmt"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net"
	"net/http"
	"net/url"
	"protocol"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type ApiSuite struct {
	listener net.Listener
	server   *HttpServer
}

var _ = Suite(&ApiSuite{})

type MockEngine struct{}

func (self *MockEngine) RunQuery(_ string, query string, yield func(*protocol.Series) error) error {
	series, err := common.StringToSeriesArray(`
[
  {
    "points": [
      {
        "values": [
          {
            "string_value": "some_value"
          },
          {
            "int_value": 1
          }
        ],
        "timestamp": 1381346631,
        "sequence_number": 1
      },
      {
        "values": [
          {
            "string_value": "some_value"
          },
          {
            "int_value": 2
          }

        ],
        "timestamp": 1381346631,
        "sequence_number": 2
      }
    ],
    "name": "foo",
    "fields": [
      {
        "type": "STRING",
        "name": "column_one"
      },
      {
        "type": "INT32",
        "name": "column_two"
      }
    ]
  }
]
`)
	if err != nil {
		return err
	}
	return yield(series[0])
}

func (self *ApiSuite) SetUpSuite(c *C) {
	self.server = NewHttpServer(nil, &MockEngine{})
	var err error
	self.listener, err = net.Listen("tcp4", ":")
	c.Assert(err, IsNil)
	go func() {
		self.server.Serve(self.listener)
	}()
	time.Sleep(1 * time.Second)
}

func (self *ApiSuite) TearDownSuite(c *C) {
	self.server.Close()
}

func (self *ApiSuite) TestQuerying(c *C) {
	port := self.listener.Addr().(*net.TCPAddr).Port
	query := "select * from foo where column_one == 'some_value';"
	query = url.QueryEscape(query)
	addr := fmt.Sprintf("http://localhost:%d/api/db/foo/series?q=%s", port, query)
	resp, err := http.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	series := []SerializedSeries{}
	err = json.Unmarshal(data, &series)
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "foo")
	// time, seq, column_one, column_two
	c.Assert(series[0].Columns, HasLen, 4)
	c.Assert(series[0].Points, HasLen, 2)
}
