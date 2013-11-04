package http

import (
	"bytes"
	"common"
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

func (self *MockEngine) RunQuery(_ common.User, _ string, query string, yield func(*protocol.Series) error) error {
	series, err := common.StringToSeriesArray(`
[
  {
    "points": [
      {
        "values": [
				  { "string_value": "some_value"},null
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
    "fields": ["column_one", "column_two"]
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
    "fields": ["column_one", "column_two"]
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
	series    []*protocol.Series
	db        string
	droppedDb string
}

func (self *MockCoordinator) DistributeQuery(_ common.User, db string, query *parser.Query, yield func(*protocol.Series) error) error {
	return nil
}

func (self *MockCoordinator) WriteSeriesData(_ common.User, db string, series *protocol.Series) error {
	self.series = append(self.series, series)
	return nil
}

func (self *MockCoordinator) CreateDatabase(_ common.User, db string) error {
	self.db = db
	return nil
}

func (self *MockCoordinator) ListDatabases(_ common.User) ([]string, error) {
	return []string{"db1", "db2"}, nil
}

func (self *MockCoordinator) DropDatabase(_ common.User, db string) error {
	self.droppedDb = db
	return nil
}

func (self *ApiSuite) formatUrl(path string, args ...interface{}) string {
	path = fmt.Sprintf(path, args...)
	port := self.listener.Addr().(*net.TCPAddr).Port
	return fmt.Sprintf("http://localhost:%d%s", port, path)
}

func (self *ApiSuite) SetUpSuite(c *C) {
	self.coordinator = &MockCoordinator{}
	self.manager = &MockUserManager{
		clusterAdmins: []string{"root"},
		dbUsers:       map[string][]string{"db1": []string{"db_user1"}},
	}
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

func (self *ApiSuite) TestClusterAdminAuthentication(c *C) {
	url := self.formatUrl("/cluster_admins/authenticate?u=root&p=root")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	resp.Body.Close()

	url = self.formatUrl("/cluster_admins/authenticate?u=fail_auth&p=anypass")
	resp, err = libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusUnauthorized)
}

func (self *ApiSuite) TestDbUserAuthentication(c *C) {
	url := self.formatUrl("/db/foo/authenticate?u=user&p=password")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	resp.Body.Close()

	url = self.formatUrl("/db/foo/authenticate?u=fail_auth&p=anypass")
	resp, err = libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusUnauthorized)
	resp.Body.Close()
}

func (self *ApiSuite) TestQueryWithNullColumns(c *C) {
	query := "select * from foo;"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s&time_precision=s&u=dbuser&p=password", query)
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
	c.Assert(series[0].Points[0][3], Equals, nil)
}

func (self *ApiSuite) TestQueryWithSecondsPrecision(c *C) {
	query := "select * from foo where column_one == 'some_value';"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s&time_precision=s&u=dbuser&p=password", query)
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
	addr := self.formatUrl("/db/foo/series?q=%s&u=dbuser&p=password", query)
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
	addr := self.formatUrl("/db/foo/series?q=%s&chunked=true&u=dbuser&p=password", query)
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

	addr := self.formatUrl("/db/foo/series?time_precision=s&u=dbuser&p=password")
	resp, err := libhttp.Post(addr, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.coordinator.series, HasLen, 1)
	series := self.coordinator.series[0]

	// check the types
	c.Assert(series.Fields, HasLen, 1)
	c.Assert(series.Fields[0], Equals, "column_one")

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

	addr := self.formatUrl("/db/foo/series?u=dbuser&p=password")
	resp, err := libhttp.Post(addr, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.coordinator.series, HasLen, 1)
	series := self.coordinator.series[0]

	// check the types
	c.Assert(series.Fields, HasLen, 1)
	c.Assert(series.Fields[0], Equals, "column_one")

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
    "columns": ["column_one", "column_two", "column_three", "column_four"]
  }
]
`

	addr := self.formatUrl("/db/foo/series?u=dbuser&p=password")
	resp, err := libhttp.Post(addr, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.coordinator.series, HasLen, 1)
	series := self.coordinator.series[0]
	c.Assert(series.Fields, HasLen, 4)

	// check the types
	c.Assert(series.Fields[0], Equals, "column_one")
	c.Assert(series.Fields[1], Equals, "column_two")
	c.Assert(series.Fields[2], Equals, "column_three")
	c.Assert(series.Fields[3], Equals, "column_four")

	// check the values
	c.Assert(series.Points, HasLen, 3)
	c.Assert(*series.Points[0].Values[0].StringValue, Equals, "1")
	c.Assert(*series.Points[0].Values[1].Int64Value, Equals, int64(1))
	c.Assert(*series.Points[0].Values[2].Int64Value, Equals, int64(1))
	c.Assert(*series.Points[0].Values[3].BoolValue, Equals, true)
}

func (self *ApiSuite) TestCreateDatabase(c *C) {
	data := `{"name": "foo", "apiKey": "bar"}`
	addr := self.formatUrl("/db?api_key=asdf&u=dbuser&p=password")
	resp, err := libhttp.Post(addr, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusCreated)
	c.Assert(self.coordinator.db, Equals, "foo")
}

func (self *ApiSuite) TestDropDatabase(c *C) {
	addr := self.formatUrl("/db/foo?u=root&p=root")
	req, err := libhttp.NewRequest("DELETE", addr, nil)
	c.Assert(err, IsNil)
	resp, err := libhttp.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusNoContent)
	c.Assert(self.coordinator.droppedDb, Equals, "foo")
}

func (self *ApiSuite) TestClusterAdminOperations(c *C) {
	url := self.formatUrl("/cluster_admins?u=root&p=root")
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

	url = self.formatUrl("/cluster_admins/new_user?u=root&p=root")
	resp, err = libhttp.Post(url, "", bytes.NewBufferString(`{"password":"new_password"}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "cluster_admin_passwd")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
	c.Assert(self.manager.ops[0].password, Equals, "new_password")
	self.manager.ops = nil

	url = self.formatUrl("/cluster_admins/new_user?u=root&p=root")
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
	url := self.formatUrl("/db/db1/users?u=root&p=root")
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

	url = self.formatUrl("/db/db1/users/new_user?u=root&p=root")
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
	url = self.formatUrl("/db/db1/admins/new_user?u=root&p=root")
	resp, err = libhttp.Post(url, "", nil)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_admin")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
	c.Assert(self.manager.ops[0].isAdmin, Equals, true)
	self.manager.ops = nil

	url = self.formatUrl("/db/db1/admins/new_user?u=root&p=root")
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

	url = self.formatUrl("/db/db1/users/new_user?u=root&p=root")
	req, _ = libhttp.NewRequest("DELETE", url, nil)
	resp, err = libhttp.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_del")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
}

func (self *ApiSuite) TestClusterAdminsIndex(c *C) {
	url := self.formatUrl("/cluster_admins?u=root&p=root")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	users := []*User{}
	err = json.Unmarshal(body, &users)
	c.Assert(err, IsNil)
	c.Assert(users, DeepEquals, []*User{&User{"root"}})
}

func (self *ApiSuite) TestDbUsersIndex(c *C) {
	url := self.formatUrl("/db/db1/users?u=root&p=root")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	users := []*User{}
	err = json.Unmarshal(body, &users)
	c.Assert(err, IsNil)
	c.Assert(users, DeepEquals, []*User{&User{"db_user1"}})
}

func (self *ApiSuite) TestDatabasesIndex(c *C) {
	url := self.formatUrl("/dbs?u=root&p=root")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	users := []*Database{}
	err = json.Unmarshal(body, &users)
	c.Assert(err, IsNil)
	c.Assert(users, DeepEquals, []*Database{&Database{"db1"}, &Database{"db2"}})
}
