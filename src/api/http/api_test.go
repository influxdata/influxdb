package http

import (
	"bytes"
	"common"
	"coordinator"
	"encoding/json"
	"fmt"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net"
	libhttp "net/http"
	"net/url"
	"parser"
	"protocol"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type ApiSuite struct {
	listener    net.Listener
	server      *HttpServer
	coordinator *MockCoordinator
	manager     *MockUserManager
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
				  { "string_value": "some_value"},{"int64_value": 1}
        ],
        "timestamp": 1381346631000000,
        "sequence_number": 1
      },
      {
        "values": [
				  {"string_value": "some_value"},{"int64_value": 2}
				],
        "timestamp": 1381346632000000,
        "sequence_number": 2
      }
    ],
    "name": "foo",
    "fields": [{"type": "STRING","name": "column_one"},{"type": "INT64","name": "column_two"}]
  },
  {
    "points": [
      {
        "values": [
				  { "string_value": "some_value"},{"int64_value": 3}
        ],
        "timestamp": 1381346633000000,
        "sequence_number": 1
      },
      {
        "values": [
				  {"string_value": "some_value"},{"int64_value": 4}
				],
        "timestamp": 1381346634000000,
        "sequence_number": 2
      }
    ],
    "name": "foo",
    "fields": [{"type": "STRING","name": "column_one"},{"type": "INT64","name": "column_two"}]
  }
]
`)
	if err != nil {
		return err
	}
	if err := yield(series[0]); err != nil {
		return err
	}
	return yield(series[1])
}

type MockCoordinator struct {
	series           []*protocol.Series
	db               string
	initialApiKey    string
	requestingApiKey string
	users            map[string]*coordinator.User
}

func (self *MockCoordinator) DistributeQuery(db string, query *parser.Query, yield func(*protocol.Series) error) error {
	return nil
}
func (self *MockCoordinator) WriteSeriesData(db string, series *protocol.Series) error {
	self.series = append(self.series, series)
	return nil
}
func (self *MockCoordinator) CreateDatabase(db, initialApiKey, requestingApiKey string) error {
	self.db = db
	self.initialApiKey = initialApiKey
	self.requestingApiKey = requestingApiKey
	return nil
}

func (self *ApiSuite) formatUrl(path string, args ...interface{}) string {
	path = fmt.Sprintf(path, args...)
	port := self.listener.Addr().(*net.TCPAddr).Port
	return fmt.Sprintf("http://localhost:%d%s", port, path)
}

func (self *ApiSuite) SetUpSuite(c *C) {
	self.coordinator = &MockCoordinator{
		users: map[string]*coordinator.User{},
	}
	self.manager = &MockUserManager{}
	self.server = NewHttpServer("", &MockEngine{}, self.coordinator, self.manager)
	var err error
	self.listener, err = net.Listen("tcp4", ":8081")
	c.Assert(err, IsNil)
	go func() {
		self.server.Serve(self.listener)
	}()
	time.Sleep(1 * time.Second)
}

func (self *ApiSuite) TearDownSuite(c *C) {
	self.server.Close()
}

func (self *ApiSuite) SetUpTest(c *C) {
	self.coordinator.series = nil
	self.manager.ops = nil
}

func (self *ApiSuite) TestQueryWithSecondsPrecision(c *C) {
	query := "select * from foo where column_one == 'some_value';"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s&time_precision=s", query)
	resp, err := libhttp.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	data, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	series := []SerializedSeries{}
	err = json.Unmarshal(data, &series)
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "foo")
	// time, seq, column_one, column_two
	c.Assert(series[0].Columns, HasLen, 4)
	c.Assert(series[0].Points, HasLen, 4)
	c.Assert(int(series[0].Points[0][0].(float64)), Equals, 1381346631)
}

func (self *ApiSuite) TestNotChunkedQuery(c *C) {
	query := "select * from foo where column_one == 'some_value';"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s", query)
	resp, err := libhttp.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	data, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	series := []SerializedSeries{}
	err = json.Unmarshal(data, &series)
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "foo")
	// time, seq, column_one, column_two
	c.Assert(series[0].Columns, HasLen, 4)
	c.Assert(series[0].Points, HasLen, 4)
	// timestamp precision is milliseconds by default
	c.Assert(int(series[0].Points[0][0].(float64)), Equals, 1381346631000)
}

func (self *ApiSuite) TestChunkedQuery(c *C) {
	query := "select * from foo where column_one == 'some_value';"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s&chunked=true", query)
	resp, err := libhttp.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()

	for i := 0; i < 2; i++ {
		chunk := make([]byte, 2048, 2048)
		n, err := resp.Body.Read(chunk)
		c.Assert(err, IsNil)

		series := SerializedSeries{}
		err = json.Unmarshal(chunk[0:n], &series)
		c.Assert(err, IsNil)
		c.Assert(series.Name, Equals, "foo")
		// time, seq, column_one, column_two
		c.Assert(series.Columns, HasLen, 4)
		// each chunk should have 2 points
		c.Assert(series.Points, HasLen, 2)
	}
}

func (self *ApiSuite) TestWriteDataWithTimeInSeconds(c *C) {
	data := `
[
  {
    "points": [
				[1382131686, "1"]
    ],
    "name": "foo",
    "columns": ["time", "column_one"]
  }
]
`

	addr := self.formatUrl("/db/foo/series?time_precision=s")
	resp, err := libhttp.Post(addr, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.coordinator.series, HasLen, 1)
	series := self.coordinator.series[0]

	// check the types
	c.Assert(series.Fields, HasLen, 1)
	c.Assert(*series.Fields[0].Name, Equals, "column_one")
	c.Assert(*series.Fields[0].Type, Equals, protocol.FieldDefinition_STRING)

	// check the values
	c.Assert(series.Points, HasLen, 1)
	c.Assert(*series.Points[0].Values[0].StringValue, Equals, "1")
	c.Assert(*series.Points[0].GetTimestampInMicroseconds(), Equals, int64(1382131686000000))
}

func (self *ApiSuite) TestWriteDataWithTime(c *C) {
	data := `
[
  {
    "points": [
				[1382131686000, "1"]
    ],
    "name": "foo",
    "columns": ["time", "column_one"]
  }
]
`

	addr := self.formatUrl("/db/foo/series")
	resp, err := libhttp.Post(addr, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	fmt.Printf("body: %s\n", string(body))
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.coordinator.series, HasLen, 1)
	series := self.coordinator.series[0]

	// check the types
	c.Assert(series.Fields, HasLen, 1)
	c.Assert(*series.Fields[0].Name, Equals, "column_one")
	c.Assert(*series.Fields[0].Type, Equals, protocol.FieldDefinition_STRING)

	// check the values
	c.Assert(series.Points, HasLen, 1)
	c.Assert(*series.Points[0].Values[0].StringValue, Equals, "1")
	c.Assert(*series.Points[0].GetTimestampInMicroseconds(), Equals, int64(1382131686000000))
}

func (self *ApiSuite) TestWriteData(c *C) {
	data := `
[
  {
    "points": [
				["1", 1, 1.0, true],
				["2", 2, 2.0, false],
				["3", 3, 3.0, true]
    ],
    "name": "foo",
    "integer_columns": [1],
    "columns": ["column_one", "column_two", "column_three", "column_four"]
  }
]
`

	addr := self.formatUrl("/db/foo/series")
	resp, err := libhttp.Post(addr, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	fmt.Printf("body: %s\n", string(body))
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.coordinator.series, HasLen, 1)
	series := self.coordinator.series[0]
	c.Assert(series.Fields, HasLen, 4)

	// check the types
	c.Assert(*series.Fields[0].Name, Equals, "column_one")
	c.Assert(*series.Fields[0].Type, Equals, protocol.FieldDefinition_STRING)
	c.Assert(*series.Fields[1].Name, Equals, "column_two")
	c.Assert(*series.Fields[1].Type, Equals, protocol.FieldDefinition_INT64)
	c.Assert(*series.Fields[2].Name, Equals, "column_three")
	c.Assert(*series.Fields[2].Type, Equals, protocol.FieldDefinition_DOUBLE)
	c.Assert(*series.Fields[3].Name, Equals, "column_four")
	c.Assert(*series.Fields[3].Type, Equals, protocol.FieldDefinition_BOOL)

	// check the values
	c.Assert(series.Points, HasLen, 3)
	c.Assert(*series.Points[0].Values[0].StringValue, Equals, "1")
	c.Assert(*series.Points[0].Values[1].Int64Value, Equals, int64(1))
	c.Assert(*series.Points[0].Values[2].DoubleValue, Equals, 1.0)
	c.Assert(*series.Points[0].Values[3].BoolValue, Equals, true)
}

func (self *ApiSuite) TestCreateDatabase(c *C) {
	data := `{"name": "foo", "apiKey": "bar"}`
	addr := self.formatUrl("/db?api_key=asdf")
	resp, err := libhttp.Post(addr, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusCreated)
	c.Assert(self.coordinator.db, Equals, "foo")
	c.Assert(self.coordinator.initialApiKey, Equals, "bar")
	c.Assert(self.coordinator.requestingApiKey, Equals, "asdf")
}

func (self *ApiSuite) TestClusterAdminOperations(c *C) {
	url := self.formatUrl("/cluster_admins?username=root&password=root")
	resp, err := libhttp.Post(url, "", bytes.NewBufferString(`{"username":"new_user", "password": "new_pass"}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 2)
	c.Assert(self.manager.ops[0].operation, Equals, "cluster_admin_add")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
	c.Assert(self.manager.ops[1].operation, Equals, "cluster_admin_passwd")
	c.Assert(self.manager.ops[1].username, Equals, "new_user")
	c.Assert(self.manager.ops[1].password, Equals, "new_pass")
	self.manager.ops = nil

	url = self.formatUrl("/cluster_admins/new_user?username=root&password=root")
	resp, err = libhttp.Post(url, "", bytes.NewBufferString(`{"password":"new_password"}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "cluster_admin_passwd")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
	c.Assert(self.manager.ops[0].password, Equals, "new_password")
	self.manager.ops = nil

	url = self.formatUrl("/cluster_admins/new_user?username=root&password=root")
	req, _ := libhttp.NewRequest("DELETE", url, nil)
	resp, err = libhttp.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "cluster_admin_del")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
}

func (self *ApiSuite) TestDbUSerOperations(c *C) {
	url := self.formatUrl("/db/db1/users?username=root&password=root")
	resp, err := libhttp.Post(url, "", bytes.NewBufferString(`{"username":"new_user", "password": "new_pass"}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 2)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_add")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
	c.Assert(self.manager.ops[1].operation, Equals, "db_user_passwd")
	c.Assert(self.manager.ops[1].username, Equals, "new_user")
	c.Assert(self.manager.ops[1].password, Equals, "new_pass")
	self.manager.ops = nil

	url = self.formatUrl("/db/db1/users/new_user?username=root&password=root")
	resp, err = libhttp.Post(url, "", bytes.NewBufferString(`{"password":"new_password"}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_passwd")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
	c.Assert(self.manager.ops[0].password, Equals, "new_password")
	self.manager.ops = nil

	// set and unset the db admin flag
	url = self.formatUrl("/db/db1/admins/new_user?username=root&password=root")
	resp, err = libhttp.Post(url, "", nil)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_admin")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
	c.Assert(self.manager.ops[0].isAdmin, Equals, true)
	self.manager.ops = nil

	url = self.formatUrl("/db/db1/admins/new_user?username=root&password=root")
	req, _ := libhttp.NewRequest("DELETE", url, nil)
	resp, err = libhttp.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_admin")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
	c.Assert(self.manager.ops[0].isAdmin, Equals, false)
	self.manager.ops = nil

	url = self.formatUrl("/db/db1/users/new_user?username=root&password=root")
	req, _ = libhttp.NewRequest("DELETE", url, nil)
	resp, err = libhttp.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_del")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
}
