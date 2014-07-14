package http

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	libhttp "net/http"
	"net/url"
	"testing"
	"time"

	"github.com/influxdb/influxdb/cluster"
	. "github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/coordinator"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
	. "launchpad.net/gocheck"
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

func (self *MockCoordinator) RunQuery(_ User, _ string, query string, yield coordinator.SeriesWriter) error {
	if self.returnedError != nil {
		return self.returnedError
	}

	series, err := StringToSeriesArray(`
[
  {
    "points": [
      {
        "values": [
				  { "string_value": "some_value"},{"is_null": true}
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
	if err := yield.Write(series[0]); err != nil {
		return err
	}
	return yield.Write(series[1])
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

func (self *MockCoordinator) WriteSeriesData(_ User, db string, series []*protocol.Series) error {
	self.series = append(self.series, series...)
	return nil
}

func (self *MockCoordinator) DeleteSeriesData(_ User, db string, query *parser.DeleteQuery, localOnly bool) error {
	self.deleteQueries = append(self.deleteQueries, query)
	return nil
}

func (self *MockCoordinator) CreateDatabase(_ User, db string) error {
	self.db = db
	return nil
}

func (self *MockCoordinator) ListDatabases(_ User) ([]*cluster.Database, error) {
	return []*cluster.Database{{"db1"}, {"db2"}}, nil
}

func (self *MockCoordinator) DropDatabase(_ User, db string) error {
	self.droppedDb = db
	return nil
}

func (self *MockCoordinator) ListContinuousQueries(_ User, db string) ([]*protocol.Series, error) {
	points := []*protocol.Point{}

	for _, query := range self.continuousQueries[db] {
		queryId := int64(query.Id)
		queryString := query.Query
		points = append(points, &protocol.Point{
			Values: []*protocol.FieldValue{
				{Int64Value: &queryId},
				{StringValue: &queryString},
			},
			Timestamp:      nil,
			SequenceNumber: nil,
		})
	}

	seriesName := "continuous queries"
	series := []*protocol.Series{{
		Name:   &seriesName,
		Fields: []string{"id", "query"},
		Points: points,
	}}
	return series, nil
}

func (self *MockCoordinator) CreateContinuousQuery(_ User, db string, query string) error {
	self.continuousQueries[db] = append(self.continuousQueries[db], &cluster.ContinuousQuery{2, query})
	return nil
}

func (self *MockCoordinator) DeleteContinuousQuery(_ User, db string, id uint32) error {
	length := len(self.continuousQueries[db])
	_, self.continuousQueries[db] = self.continuousQueries[db][length-1], self.continuousQueries[db][:length-1]
	return nil
}

func (self *ApiSuite) formatUrl(path string, args ...interface{}) string {
	path = fmt.Sprintf(path, args...)
	port := self.listener.Addr().(*net.TCPAddr).Port
	return fmt.Sprintf("http://localhost:%d%s", port, path)
}

func (self *ApiSuite) SetUpSuite(c *C) {
	self.coordinator = &MockCoordinator{
		continuousQueries: map[string][]*cluster.ContinuousQuery{
			"db1": {
				{1, "select * from foo into bar;"},
			},
		},
	}

	self.manager = &MockUserManager{
		clusterAdmins: []string{"root"},
		dbUsers:       map[string]map[string]MockDbUser{"db1": {"db_user1": {Name: "db_user1", IsAdmin: false}}},
	}
	dir := c.MkDir()
	self.server = NewHttpServer(
		"",
		10*time.Second,
		dir,
		self.coordinator,
		self.manager,
		cluster.NewClusterConfiguration(&configuration.Configuration{}, nil, nil, nil, nil),
		nil)
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
	self.coordinator.returnedError = nil
	self.manager.ops = nil
}

func (self *ApiSuite) TestHealthCheck(c *C) {
	url := self.formatUrl("/ping")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	resp.Body.Close()
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
	c.Assert(resp.Header.Get("WWW-Authenticate"), Equals, "Basic realm=\"influxdb\"")
	resp.Body.Close()
}

func (self *ApiSuite) TestDbUserAuthentication(c *C) {
	url := self.formatUrl("/db/foo/authenticate?u=dbuser&p=password")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	resp.Body.Close()

	url = self.formatUrl("/db/foo/authenticate?u=fail_auth&p=anypass")
	resp, err = libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusUnauthorized)
	c.Assert(resp.Header.Get("WWW-Authenticate"), Equals, "Basic realm=\"influxdb\"")
	resp.Body.Close()
}

func (self *ApiSuite) TestDbUserBasicAuthentication(c *C) {
	url := self.formatUrl("/db/foo/authenticate")
	req, err := libhttp.NewRequest("GET", url, nil)
	auth := base64.StdEncoding.EncodeToString([]byte("dbuser:password"))
	req.Header.Add("Authorization", "Basic "+auth)
	c.Assert(err, IsNil)
	resp, err := libhttp.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	resp.Body.Close()
}

func (self *ApiSuite) TestQueryAsClusterAdmin(c *C) {
	query := "select * from foo;"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s&u=root&p=root", query)
	resp, err := libhttp.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
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

func (self *ApiSuite) TestQueryErrorPropagatesProperly(c *C) {
	self.coordinator.returnedError = fmt.Errorf("some error")
	query := "select * from does_not_exist;"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s&time_precision=s&u=dbuser&p=password", query)
	resp, err := libhttp.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusBadRequest)
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

func (self *ApiSuite) TestQueryWithInvalidPrecision(c *C) {
	query := "select * from foo where column_one == 'some_value';"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s&time_precision=foo&u=dbuser&p=password", query)
	resp, err := libhttp.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusBadRequest)
	c.Assert(resp.Header.Get("content-type"), Equals, "text/plain")
}

func (self *ApiSuite) TestNotChunkedQuery(c *C) {
	query := "select * from foo where column_one == 'some_value';"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s&u=dbuser&p=password", query)
	resp, err := libhttp.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")
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
	c.Assert(int64(series[0].Points[0][0].(float64)), Equals, int64(1381346631000))
}

func (self *ApiSuite) TestNotChunkedPrettyQuery(c *C) {
	query := "select * from foo where column_one == 'some_value';"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s&u=dbuser&p=password&pretty=true", query)
	resp, err := libhttp.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")
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
	c.Assert(int64(series[0].Points[0][0].(float64)), Equals, int64(1381346631000))
}

func (self *ApiSuite) TestNotChunkedNotPrettyQuery(c *C) {
	query := "select * from foo where column_one == 'some_value';"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s&u=dbuser&p=password&pretty=false", query)
	resp, err := libhttp.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")
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
	c.Assert(int64(series[0].Points[0][0].(float64)), Equals, int64(1381346631000))
}

func (self *ApiSuite) TestChunkedQuery(c *C) {
	query := "select * from foo where column_one == 'some_value';"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s&chunked=true&u=dbuser&p=password", query)
	resp, err := libhttp.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")

	for i := 0; i < 2; i++ {
		chunk := make([]byte, 2048, 2048)
		n, err := resp.Body.Read(chunk)

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

func (self *ApiSuite) TestPrettyChunkedQuery(c *C) {
	query := "select * from foo where column_one == 'some_value';"
	query = url.QueryEscape(query)
	addr := self.formatUrl("/db/foo/series?q=%s&chunked=true&u=dbuser&p=password&pretty=true", query)
	resp, err := libhttp.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")

	for i := 0; i < 2; i++ {
		chunk := make([]byte, 2048, 2048)
		n, err := resp.Body.Read(chunk)

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

func (self *ApiSuite) TestWriteDataWithInvalidTime(c *C) {
	data := `
[
  {
    "points": [
				["foo", "1"]
    ],
    "name": "foo",
    "columns": ["time", "column_one"]
  }
]
`

	addr := self.formatUrl("/db/foo/series?u=dbuser&p=password")
	resp, err := libhttp.Post(addr, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusBadRequest)
}

func (self *ApiSuite) TestWriteDataWithNull(c *C) {
	data := `
[
  {
    "points": [
				["1", 1, 1.0, true],
				["2", 2, 2.0, false],
				["3", 3, 3.0, null]
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
	c.Assert(*series.Points[2].Values[0].StringValue, Equals, "3")
	c.Assert(*series.Points[2].Values[1].Int64Value, Equals, int64(3))
	c.Assert(*series.Points[2].Values[2].DoubleValue, Equals, 3.0)
	c.Assert(series.Points[2].Values[3].GetIsNull(), Equals, true)
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
	c.Assert(*series.Points[0].Values[2].DoubleValue, Equals, 1.0)
	c.Assert(*series.Points[0].Values[3].BoolValue, Equals, true)
}

func (self *ApiSuite) TestWriteDataAsClusterAdmin(c *C) {
	data := `
[
  {
    "points": [
				["1", true]
    ],
    "name": "foo",
    "columns": ["column_one", "column_two"]
  }
]
`

	addr := self.formatUrl("/db/foo/series?u=root&p=root")
	resp, err := libhttp.Post(addr, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
}

func (self *ApiSuite) TestCreateDatabase(c *C) {
	data := `{"name": "foo", "apiKey": "bar"}`
	addr := self.formatUrl("/db?api_key=asdf&u=root&p=root")
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
	resp, err := libhttp.Post(url, "", bytes.NewBufferString(`{"name":"", "password": "new_pass"}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusBadRequest)

	url = self.formatUrl("/cluster_admins?u=root&p=root")
	resp, err = libhttp.Post(url, "", bytes.NewBufferString(`{"name":"new_user", "password": "new_pass"}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "cluster_admin_add")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
	c.Assert(self.manager.ops[0].password, Equals, "new_pass")
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

	req, _ := libhttp.NewRequest("DELETE", url, nil)
	resp, err = libhttp.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "cluster_admin_del")
	c.Assert(self.manager.ops[0].username, Equals, "new_user")
}

func (self *ApiSuite) TestDbUserOperations(c *C) {
	// create user using the `name` field
	url := self.formatUrl("/db/db1/users?u=root&p=root")
	resp, err := libhttp.Post(url, "", bytes.NewBufferString(`{"name":"dbuser", "password": "password"}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_add")
	c.Assert(self.manager.ops[0].username, Equals, "dbuser")
	c.Assert(self.manager.ops[0].password, Equals, "password")
	self.manager.ops = nil

	url = self.formatUrl("/db/db1/users/dbuser?u=root&p=root")
	resp, err = libhttp.Post(url, "", bytes.NewBufferString(`{"password":"new_password"}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_passwd")
	c.Assert(self.manager.ops[0].username, Equals, "dbuser")
	c.Assert(self.manager.ops[0].password, Equals, "new_password")
	self.manager.ops = nil

	// empty usernames aren't valid
	url = self.formatUrl("/db/db1/users?u=root&p=root")
	resp, err = libhttp.Post(url, "", bytes.NewBufferString(`{"name":"", "password": "password"}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusBadRequest)

	// Fix #477 - Username should support @ character - https://github.com/influxdb/influxdb/issues/447
	url = self.formatUrl("/db/db1/users?u=root&p=root")
	resp, err = libhttp.Post(url, "", bytes.NewBufferString(`{"name":"paul@influxdb.com", "password": "password"}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_add")
	c.Assert(self.manager.ops[0].username, Equals, "paul@influxdb.com")
	c.Assert(self.manager.ops[0].password, Equals, "password")
	self.manager.ops = nil

	// set and unset the db admin flag
	url = self.formatUrl("/db/db1/users/dbuser?u=root&p=root")
	resp, err = libhttp.Post(url, "", bytes.NewBufferString(`{"admin": true}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_admin")
	c.Assert(self.manager.ops[0].username, Equals, "dbuser")
	c.Assert(self.manager.ops[0].isAdmin, Equals, true)
	self.manager.ops = nil
	url = self.formatUrl("/db/db1/users/dbuser?u=root&p=root")
	resp, err = libhttp.Post(url, "", bytes.NewBufferString(`{"admin": false}`))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_admin")
	c.Assert(self.manager.ops[0].username, Equals, "dbuser")
	c.Assert(self.manager.ops[0].isAdmin, Equals, false)
	self.manager.ops = nil

	url = self.formatUrl("/db/db1/users/dbuser?u=root&p=root")
	req, _ := libhttp.NewRequest("DELETE", url, nil)
	resp, err = libhttp.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	c.Assert(self.manager.ops, HasLen, 1)
	c.Assert(self.manager.ops[0].operation, Equals, "db_user_del")
	c.Assert(self.manager.ops[0].username, Equals, "dbuser")
}

func (self *ApiSuite) TestClusterAdminsIndex(c *C) {
	url := self.formatUrl("/cluster_admins?u=root&p=root")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	users := []*ApiUser{}
	err = json.Unmarshal(body, &users)
	c.Assert(err, IsNil)
	c.Assert(users, DeepEquals, []*ApiUser{{"root"}})
}

func (self *ApiSuite) TestPrettyClusterAdminsIndex(c *C) {
	url := self.formatUrl("/cluster_admins?u=root&p=root&pretty=true")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	users := []*ApiUser{}
	err = json.Unmarshal(body, &users)
	c.Assert(err, IsNil)
	c.Assert(users, DeepEquals, []*ApiUser{{"root"}})
}

func (self *ApiSuite) TestDbUsersIndex(c *C) {
	url := self.formatUrl("/db/db1/users?u=root&p=root")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	users := []*UserDetail{}
	err = json.Unmarshal(body, &users)
	c.Assert(err, IsNil)
	c.Assert(users, HasLen, 1)
	c.Assert(users[0], DeepEquals, &UserDetail{"db_user1", false, ".*", ".*"})
}

func (self *ApiSuite) TestPrettyDbUsersIndex(c *C) {
	url := self.formatUrl("/db/db1/users?u=root&p=root&pretty=true")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	users := []*UserDetail{}
	err = json.Unmarshal(body, &users)
	c.Assert(err, IsNil)
	c.Assert(users, HasLen, 1)
	c.Assert(users[0], DeepEquals, &UserDetail{"db_user1", false, ".*", ".*"})
}

func (self *ApiSuite) TestDbUserShow(c *C) {
	url := self.formatUrl("/db/db1/users/db_user1?u=root&p=root")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	userDetail := &UserDetail{}
	err = json.Unmarshal(body, &userDetail)
	c.Assert(err, IsNil)
	c.Assert(userDetail, DeepEquals, &UserDetail{"db_user1", false, ".*", ".*"})
}

func (self *ApiSuite) TestDatabasesIndex(c *C) {
	for _, path := range []string{"/db?u=root&p=root", "/db?u=root&p=root"} {
		url := self.formatUrl(path)
		resp, err := libhttp.Get(url)
		c.Assert(err, IsNil)
		c.Assert(resp.Header.Get("content-type"), Equals, "application/json")
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		databases := []*cluster.Database{}
		err = json.Unmarshal(body, &databases)
		c.Assert(err, IsNil)
		err = json.Unmarshal(body, &databases)
		c.Assert(err, IsNil)
		c.Assert(databases, DeepEquals, []*cluster.Database{{"db1"}, {"db2"}})
	}
}

func (self *ApiSuite) TestBasicAuthentication(c *C) {
	url := self.formatUrl("/db")
	req, err := libhttp.NewRequest("GET", url, nil)
	c.Assert(err, IsNil)
	auth := base64.StdEncoding.EncodeToString([]byte("root:root"))
	req.Header.Add("Authorization", "Basic "+auth)
	resp, err := libhttp.DefaultClient.Do(req)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	databases := []*cluster.Database{}
	c.Assert(err, IsNil)
	err = json.Unmarshal(body, &databases)
	c.Assert(err, IsNil)
	c.Assert(databases, DeepEquals, []*cluster.Database{{"db1"}, {"db2"}})
}

func (self *ApiSuite) TestContinuousQueryOperations(c *C) {
	// verify current continuous query index
	url := self.formatUrl("/db/db1/continuous_queries?u=root&p=root")
	resp, err := libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	queries := []ContinuousQuery{}
	err = json.Unmarshal(body, &queries)
	c.Assert(err, IsNil)
	c.Assert(queries, HasLen, 1)

	c.Assert(queries[0].Id, Equals, int64(1))
	c.Assert(queries[0].Query, Equals, "select * from foo into bar;")

	resp.Body.Close()

	// add a new continuous query
	data := `{"query": "select * from quu into qux;"}`
	url = self.formatUrl("/db/db1/continuous_queries?u=root&p=root")
	resp, err = libhttp.Post(url, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	resp.Body.Close()

	// verify updated continuous query index
	url = self.formatUrl("/db/db1/continuous_queries?u=root&p=root")
	resp, err = libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")
	body, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	queries = []ContinuousQuery{}
	err = json.Unmarshal(body, &queries)
	c.Assert(err, IsNil)

	c.Assert(queries, HasLen, 2)
	c.Assert(queries[0].Id, Equals, int64(1))
	c.Assert(queries[0].Query, Equals, "select * from foo into bar;")
	c.Assert(queries[1].Id, Equals, int64(2))
	c.Assert(queries[1].Query, Equals, "select * from quu into qux;")

	resp.Body.Close()

	// delete the newly-created query
	url = self.formatUrl("/db/db1/continuous_queries/2?u=root&p=root")
	req, err := libhttp.NewRequest("DELETE", url, nil)
	c.Assert(err, IsNil)
	resp, err = libhttp.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, libhttp.StatusOK)
	resp.Body.Close()

	// verify updated continuous query index
	url = self.formatUrl("/db/db1/continuous_queries?u=root&p=root")
	resp, err = libhttp.Get(url)
	c.Assert(err, IsNil)
	c.Assert(resp.Header.Get("content-type"), Equals, "application/json")
	body, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	queries = []ContinuousQuery{}
	err = json.Unmarshal(body, &queries)
	c.Assert(err, IsNil)
	c.Assert(queries, HasLen, 1)
	c.Assert(queries[0].Id, Equals, int64(1))
	c.Assert(queries[0].Query, Equals, "select * from foo into bar;")
	resp.Body.Close()
}
