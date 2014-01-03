package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type ServerSuite struct {
	serverProcesses []*ServerProcess
}

type ServerProcess struct {
	p          *os.Process
	configFile string
	apiPort    int
}

func NewServerProcess(configFile string, apiPort int, c *C) *ServerProcess {
	s := &ServerProcess{configFile: configFile, apiPort: apiPort}
	err := s.Start()
	c.Assert(err, IsNil)
	time.Sleep(time.Second * 4)
	return s
}

func (self *ServerProcess) Start() error {
	if self.p != nil {
		return fmt.Errorf("Server is already running with pid %d", self.p.Pid)
	}

	dir, err := os.Getwd()
	if err != nil {
		return err
	}

	root := filepath.Join(dir, "..", "..")
	filename := filepath.Join(root, "daemon")
	config := filepath.Join(root, "src/integration/", self.configFile)
	p, err := os.StartProcess(filename, []string{filename, "-config", config}, &os.ProcAttr{
		Dir:   root,
		Env:   os.Environ(),
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	})
	if err != nil {
		return err
	}
	self.p = p
	return nil
}

func (self *ServerProcess) Stop() {
	if self.p == nil {
		return
	}

	self.p.Signal(syscall.SIGTERM)
	self.p.Wait()
	self.p = nil
}

func ResultsToSeriesCollection(results []interface{}) *SeriesCollection {
	collection := &SeriesCollection{Members: make([]*Series, 0)}
	for _, result := range results {
		seriesResult := result.(map[string]interface{})
		series := &Series{}
		series.Name = seriesResult["name"].(string)
		columns := seriesResult["columns"].([]interface{})
		series.Columns = make([]string, 0)
		for _, col := range columns {
			series.Columns = append(series.Columns, col.(string))
		}
		points := seriesResult["points"].([]interface{})
		series.Points = make([]*Point, 0)
		for _, point := range points {
			series.Points = append(series.Points, &Point{Values: point.([]interface{})})
		}
		collection.Members = append(collection.Members, series)
	}
	return collection
}

type SeriesCollection struct {
	Members []*Series
}

func (self *SeriesCollection) GetSeries(name string, c *C) *Series {
	for _, s := range self.Members {
		if s.Name == name {
			return s
		}
	}
	c.Errorf("Couldn't find series '%s' in:\n", name, self)
	return nil
}

type Series struct {
	Name    string
	Columns []string
	Points  []*Point
}

func (self *Series) GetValueForPointAndColumn(pointIndex int, columnName string, c *C) interface{} {
	columnIndex := -1
	for index, name := range self.Columns {
		if name == columnName {
			columnIndex = index
		}
	}
	if columnIndex == -1 {
		c.Errorf("Couldn't find column '%s' in series:\n", columnName, self)
		return nil
	}
	if pointIndex > len(self.Points)-1 {
		c.Errorf("Fewer than %d points in series '%s':\n", pointIndex+1, self.Name, self)
	}
	return self.Points[pointIndex].Values[columnIndex]
}

type Point struct {
	Values []interface{}
}

func (self *ServerProcess) Query(database, query string, onlyLocal bool, c *C) *SeriesCollection {
	encodedQuery := url.QueryEscape(query)
	fullUrl := fmt.Sprintf("http://localhost:%d/db/%s/series?u=paul&p=pass&q=%s", self.apiPort, database, encodedQuery)
	if onlyLocal {
		fullUrl = fullUrl + "&force_local=true"
	}
	resp, err := http.Get(fullUrl)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	var js []interface{}
	err = json.Unmarshal(body, &js)
	if err != nil {
		fmt.Println("NOT JSON: ", string(body))
	}
	c.Assert(err, IsNil)
	return ResultsToSeriesCollection(js)
}

func (self *ServerProcess) Post(url, data string, c *C) *http.Response {
	fullUrl := fmt.Sprintf("http://localhost:%d%s", self.apiPort, url)
	resp, err := http.Post(fullUrl, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	time.Sleep(time.Millisecond * 10)
	return resp
}

var _ = Suite(&ServerSuite{})

func (self *ServerSuite) SetUpSuite(c *C) {
	err := os.RemoveAll("/tmp/influxdb/test")
	c.Assert(err, IsNil)
	self.serverProcesses = []*ServerProcess{
		NewServerProcess("test_config1.toml", 60500, c),
		NewServerProcess("test_config2.toml", 60506, c),
		NewServerProcess("test_config3.toml", 60510, c)}
	self.serverProcesses[0].Post("/db?u=root&p=root", "{\"name\":\"full_rep\", \"replicationFactor\":3}", c)
	self.serverProcesses[0].Post("/db?u=root&p=root", "{\"name\":\"test_rep\", \"replicationFactor\":2}", c)
	self.serverProcesses[0].Post("/db?u=root&p=root", "{\"name\":\"single_rep\", \"replicationFactor\":1}", c)
	self.serverProcesses[0].Post("/db/full_rep/users?u=root&p=root", "{\"name\":\"paul\", \"password\":\"pass\", \"isAdmin\": true}", c)
	self.serverProcesses[0].Post("/db/test_rep/users?u=root&p=root", "{\"name\":\"paul\", \"password\":\"pass\", \"isAdmin\": true}", c)
	self.serverProcesses[0].Post("/db/single_rep/users?u=root&p=root", "{\"name\":\"paul\", \"password\":\"pass\", \"isAdmin\": true}", c)
	time.Sleep(300 * time.Millisecond)
}

func (self *ServerSuite) TearDownSuite(c *C) {
	for _, s := range self.serverProcesses {
		s.Stop()
	}
}

// For issue #140 https://github.com/influxdb/influxdb/issues/140
func (self *ServerSuite) TestRestartServers(c *C) {
	data := `
  [{
    "points": [[1]],
    "name": "test_restart",
    "columns": ["val"]
  }]
  `
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)

	collection := self.serverProcesses[0].Query("test_rep", "select * from test_restart", false, c)
	c.Assert(collection.Members, HasLen, 1)
	series := collection.GetSeries("test_restart", c)
	c.Assert(series.Points, HasLen, 1)

	for _, s := range self.serverProcesses {
		s.Stop()
	}
	time.Sleep(time.Second)

	err := self.serverProcesses[0].Start()
	c.Assert(err, IsNil)
	time.Sleep(time.Second)
	err = self.serverProcesses[1].Start()
	c.Assert(err, IsNil)
	err = self.serverProcesses[2].Start()
	time.Sleep(time.Second)

	collection = self.serverProcesses[0].Query("test_rep", "select * from test_restart", false, c)
	c.Assert(collection.Members, HasLen, 1)
	series = collection.GetSeries("test_restart", c)
	c.Assert(series.Points, HasLen, 1)
}

func (self *ServerSuite) TestDataReplication(c *C) {
	data := `
  [{
    "points": [
        ["val1", 2]
    ],
    "name": "test_data_replication",
    "columns": ["val_1", "val_2"]
  }]`
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)

	serversWithPoint := 0
	for _, server := range self.serverProcesses {
		collection := server.Query("test_rep", "select * from test_data_replication", true, c)
		series := collection.GetSeries("test_data_replication", c)
		if len(series.Points) > 0 {
			serversWithPoint += 1
		}
	}
	c.Assert(serversWithPoint, Equals, 2)
}

func (self *ServerSuite) TestDeleteReplication(c *C) {
	data := `
  [{
    "points": [
        ["val1", 2]
    ],
    "name": "test_delete_replication",
    "columns": ["val_1", "val_2"]
  }]`
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)
	collection := self.serverProcesses[0].Query("test_rep", "select count(val_1) from test_delete_replication", false, c)
	series := collection.GetSeries("test_delete_replication", c)
	c.Assert(series.GetValueForPointAndColumn(0, "count", c), Equals, float64(1))

	self.serverProcesses[0].Query("test_rep", "delete from test_delete_replication", false, c)
	collection = self.serverProcesses[0].Query("test_rep", "select count(val_1) from test_delete_replication", false, c)
	c.Assert(collection.Members, HasLen, 0)
}

func (self *ServerSuite) TestListSeries(c *C) {
	self.serverProcesses[0].Post("/db?u=root&p=root", `{"name": "list_series", "replicationFactor": 2}`, c)
	self.serverProcesses[0].Post("/db/list_series/users?u=root&p=root", `{"name": "paul", "password": "pass"}`, c)
	data := `[{
		"name": "cluster_query",
		"columns": ["val1"],
		"points": [[1]]
		}]`
	self.serverProcesses[0].Post("/db/list_series/series?u=paul&p=pass", data, c)
	for _, s := range self.serverProcesses {
		collection := s.Query("list_series", "list series", false, c)
		s := collection.GetSeries("series", c)
		c.Assert(s.GetValueForPointAndColumn(0, "name", c), Equals, "cluster_query")
	}
}

func (self *ServerSuite) TestFailureAndReplicationReplays(c *C) {
	// write data and confirm that it went to all three servers
	data := `
  [{
    "points": [
        [1]
    ],
    "name": "test_failure_replays",
    "columns": ["val"]
  }]`
	self.serverProcesses[0].Post("/db/full_rep/series?u=paul&p=pass", data, c)

	for _, s := range self.serverProcesses {
		collection := s.Query("full_rep", "select sum(val) from test_failure_replays;", true, c)
		series := collection.GetSeries("test_failure_replays", c)
		c.Assert(series.Points, HasLen, 1)
	}

	self.serverProcesses[1].Stop()
	time.Sleep(time.Second)
	data = `
	[{
		"points": [[2]],
		"name": "test_failure_replays",
		"columns": ["val"]
	}]
	`
	self.serverProcesses[0].Post("/db/full_rep/series?u=paul&p=pass", data, c)
	self.serverProcesses[1].Start()
	time.Sleep(time.Second)

	expected := []float64{float64(3), float64(1), float64(3)}
	for i, s := range self.serverProcesses {
		collection := s.Query("full_rep", "select sum(val) from test_failure_replays;", true, c)
		series := collection.GetSeries("test_failure_replays", c)
		c.Assert(series.GetValueForPointAndColumn(0, "sum", c), Equals, expected[i])
	}

	// TODO: fix this. I do this 1k times because there's no way right now to force a replay
	//       on a server other than having a write with the originating server id and owner server id
	//       the same as the write that occured while the server was down. Doing this means it
	//       will almost certainly trigger one (i.e. a request will randomly hash to the org/owner server)
	data = `
	[{
		"points": [[1]],
		"name": "test_failure_replays",
		"columns": ["val"]
	}]
	`
	for i := 0; i < 10; i++ {
		self.serverProcesses[0].Post("/db/full_rep/series?u=paul&p=pass", data, c)
	}

	for _, s := range self.serverProcesses {
		collection := s.Query("full_rep", "select sum(val) from test_failure_replays;", true, c)
		series := collection.GetSeries("test_failure_replays", c)
		c.Assert(series.GetValueForPointAndColumn(0, "sum", c), Equals, float64(13))
	}
}

func (self *ServerSuite) TestFailureAndDeleteReplays(c *C) {
	data := `
  [{
    "points": [
        [1]
    ],
    "name": "test_failure_delete_replays",
    "columns": ["val"]
  }]`
	self.serverProcesses[0].Post("/db/full_rep/series?u=paul&p=pass", data, c)
	for _, s := range self.serverProcesses {
		collection := s.Query("full_rep", "select val from test_failure_delete_replays", true, c)
		series := collection.GetSeries("test_failure_delete_replays", c)
		c.Assert(series.Points, HasLen, 1)
	}
	self.serverProcesses[1].Stop()
	self.serverProcesses[0].Query("full_rep", "delete from test_failure_delete_replays", false, c)
	time.Sleep(time.Second)
	self.serverProcesses[1].Start()
	time.Sleep(time.Second)

	for i, s := range self.serverProcesses {
		collection := s.Query("full_rep", "select sum(val) from test_failure_delete_replays;", true, c)

		if i == 1 {
			series := collection.GetSeries("test_failure_delete_replays", c)
			c.Assert(series.GetValueForPointAndColumn(0, "sum", c), Equals, float64(1))
		} else {
			c.Assert(collection.Members, HasLen, 0)
		}
	}

	for i := 1; i <= 100; i++ {
		data = fmt.Sprintf(`
  [{
    "points": [
        [%d]
    ],
    "name": "test_failure_delete_replays",
    "columns": ["val"]
  }]`, i)
		time.Sleep(time.Millisecond)
		self.serverProcesses[0].Post("/db/full_rep/series?u=paul&p=pass", data, c)
	}

	expectedSum := 100 * 101 / 2

	for _, s := range self.serverProcesses {
		collection := s.Query("full_rep", "select sum(val) from test_failure_delete_replays;", true, c)
		series := collection.GetSeries("test_failure_delete_replays", c)
		c.Assert(series.GetValueForPointAndColumn(0, "sum", c).(float64) <= float64(expectedSum), Equals, true)
	}
}

// For issue #130 https://github.com/influxdb/influxdb/issues/130
func (self *ServerSuite) TestColumnNamesReturnInDistributedQuery(c *C) {
	data := `[{
		"name": "cluster_query_with_columns",
		"columns": ["asdf"],
		"points": [[1]]
		}]`
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)
	for _, s := range self.serverProcesses {
		collection := s.Query("test_rep", "select * from cluster_query_with_columns", false, c)
		series := collection.GetSeries("cluster_query_with_columns", c)
		c.Assert(series.GetValueForPointAndColumn(0, "asdf", c), Equals, float64(1))
	}
}

// For issue #131 https://github.com/influxdb/influxdb/issues/131
func (self *ServerSuite) TestSelectFromRegexInCluster(c *C) {
	data := `[{
		"name": "cluster_regex_query",
		"columns": ["col1", "col2"],
		"points": [[1, "foo"], [23, "bar"]]
		},{
			"name": "cluster_regex_query_number2",
			"columns": ["blah"],
			"points": [[true]]
		}]`
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)
	for _, s := range self.serverProcesses {
		collection := s.Query("test_rep", "select * from /.*/ limit 1", false, c)
		series := collection.GetSeries("cluster_regex_query", c)
		c.Assert(series.GetValueForPointAndColumn(0, "col1", c), Equals, float64(23))
		c.Assert(series.GetValueForPointAndColumn(0, "col2", c), Equals, "bar")
		series = collection.GetSeries("cluster_regex_query_number2", c)
		c.Assert(series.GetValueForPointAndColumn(0, "blah", c), Equals, true)
	}
}
