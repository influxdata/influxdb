package integration

import (
	"bytes"
	"crypto/tls"
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

func NewServerProcess(configFile string, apiPort int, d time.Duration, c *C) *ServerProcess {
	s := &ServerProcess{configFile: configFile, apiPort: apiPort}
	err := s.Start()
	c.Assert(err, IsNil)
	if d > 0 {
		time.Sleep(d)
	}
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
	c.Fatalf("Couldn't find series '%s' in:\n", name, self)
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
	return self.QueryWithUsername(database, query, onlyLocal, c, "paul", "pass")
}

func (self *ServerProcess) QueryAsRoot(database, query string, onlyLocal bool, c *C) *SeriesCollection {
	return self.QueryWithUsername(database, query, onlyLocal, c, "root", "root")
}

func (self *ServerProcess) QueryWithUsername(database, query string, onlyLocal bool, c *C, username, password string) *SeriesCollection {
	encodedQuery := url.QueryEscape(query)
	fullUrl := fmt.Sprintf("http://localhost:%d/db/%s/series?u=%s&p=%s&q=%s", self.apiPort, database, username, password, encodedQuery)
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
	return self.Request("POST", url, data, c)
}

func (self *ServerProcess) Request(method, url, data string, c *C) *http.Response {
	fullUrl := fmt.Sprintf("http://localhost:%d%s", self.apiPort, url)
	req, err := http.NewRequest(method, fullUrl, bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	resp, err := http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	return resp
}

var _ = Suite(&ServerSuite{})

func (self *ServerSuite) SetUpSuite(c *C) {
	err := os.RemoveAll("/tmp/influxdb/test")
	c.Assert(err, IsNil)
	self.serverProcesses = []*ServerProcess{
		NewServerProcess("test_config1.toml", 60500, time.Second*4, c),
		NewServerProcess("test_config2.toml", 60506, time.Second*4, c),
		NewServerProcess("test_config3.toml", 60510, time.Second*4, c)}
	self.serverProcesses[0].Post("/db?u=root&p=root", "{\"name\":\"full_rep\", \"replicationFactor\":3}", c)
	self.serverProcesses[0].Post("/db?u=root&p=root", "{\"name\":\"test_rep\", \"replicationFactor\":2}", c)
	self.serverProcesses[0].Post("/db?u=root&p=root", "{\"name\":\"single_rep\", \"replicationFactor\":1}", c)
	self.serverProcesses[0].Post("/db?u=root&p=root", "{\"name\":\"test_cq\", \"replicationFactor\":3}", c)
	self.serverProcesses[0].Post("/db/full_rep/users?u=root&p=root", "{\"name\":\"paul\", \"password\":\"pass\", \"isAdmin\": true}", c)
	self.serverProcesses[0].Post("/db/test_rep/users?u=root&p=root", "{\"name\":\"paul\", \"password\":\"pass\", \"isAdmin\": true}", c)
	self.serverProcesses[0].Post("/db/single_rep/users?u=root&p=root", "{\"name\":\"paul\", \"password\":\"pass\", \"isAdmin\": true}", c)
	self.serverProcesses[0].Post("/db/test_cq/users?u=root&p=root", "{\"name\":\"paul\", \"password\":\"pass\", \"isAdmin\": true}", c)
	time.Sleep(time.Second)
}

func (self *ServerSuite) TearDownSuite(c *C) {
	for _, s := range self.serverProcesses {
		s.Stop()
	}
}

func (self *ServerSuite) TestRestartAfterCompaction(c *C) {
	data := `
  [{
    "points": [[1]],
    "name": "test_restart_after_compaction",
    "columns": ["val"]
  }]
  `
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)

	collection := self.serverProcesses[0].Query("test_rep", "select * from test_restart_after_compaction", false, c)
	c.Assert(collection.Members, HasLen, 1)
	series := collection.GetSeries("test_restart_after_compaction", c)
	c.Assert(series.Points, HasLen, 1)
	resp := self.serverProcesses[0].Post("/raft/force_compaction?u=root&p=root", "", c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	self.serverProcesses[0].Stop()
	time.Sleep(time.Second)
	self.serverProcesses[0].Start()
	time.Sleep(time.Second * 3)

	collection = self.serverProcesses[0].Query("test_rep", "select * from test_restart_after_compaction", false, c)
	c.Assert(collection.Members, HasLen, 1)
	series = collection.GetSeries("test_restart_after_compaction", c)
	c.Assert(series.Points, HasLen, 1)
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

func (self *ServerSuite) TestWritingNullInCluster(c *C) {
	data := `[{"name":"test_null_in_cluster","columns":["provider","city"],"points":[["foo", "bar"], [null, "baz"]]}]`
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)

	collection := self.serverProcesses[0].Query("test_rep", "select * from test_null_in_cluster", false, c)
	c.Assert(collection.Members, HasLen, 1)
	c.Assert(collection.GetSeries("test_null_in_cluster", c).Points, HasLen, 2)
}

func (self *ServerSuite) TestCountDistinctWithNullValues(c *C) {
	data := `[{"name":"test_null_with_distinct","columns":["column"],"points":[["value1"], [null], ["value2"], ["value1"], [null]]}]`
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)

	collection := self.serverProcesses[0].Query("test_rep", "select count(distinct(column)) from test_null_with_distinct group by time(1m)", false, c)
	c.Assert(collection.Members, HasLen, 1)
	series := collection.GetSeries("test_null_with_distinct", c)
	c.Assert(series.GetValueForPointAndColumn(0, "count", c), Equals, float64(2))
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

// issue #147
func (self *ServerSuite) TestExtraSequenceNumberColumns(c *C) {
	data := `
  [{
    "points": [
        ["foo", 1390852524, 1234]
    ],
    "name": "test_extra_sequence_number_column",
    "columns": ["val_1", "time", "sequence_number"]
  }]`
	resp := self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass&time_precision=s", data, c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	time.Sleep(time.Second)

	collection := self.serverProcesses[0].Query("test_rep", "select * from test_extra_sequence_number_column", false, c)
	series := collection.GetSeries("test_extra_sequence_number_column", c)
	c.Assert(series.Columns, HasLen, 3)
	c.Assert(series.GetValueForPointAndColumn(0, "sequence_number", c), Equals, float64(1234))
}

// issue #206
func (self *ServerSuite) TestUnicodeSupport(c *C) {
	data := `
  [{
    "points": [
        ["山田太郎", "中文", "⚑"]
    ],
    "name": "test_unicode",
    "columns": ["val_1", "val_2", "val_3"]
  }]`
	resp := self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	time.Sleep(time.Second)

	collection := self.serverProcesses[0].Query("test_rep", "select * from test_unicode", false, c)
	series := collection.GetSeries("test_unicode", c)
	c.Assert(series.GetValueForPointAndColumn(0, "val_1", c), Equals, "山田太郎")
	c.Assert(series.GetValueForPointAndColumn(0, "val_2", c), Equals, "中文")
	c.Assert(series.GetValueForPointAndColumn(0, "val_3", c), Equals, "⚑")
}

func (self *ServerSuite) TestSslSupport(c *C) {
	data := `
  [{
    "points": [
        ["val1", 2]
    ],
    "name": "test_sll",
    "columns": ["val_1", "val_2"]
  }]`
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)

	encodedQuery := url.QueryEscape("select * from test_sll")
	fullUrl := fmt.Sprintf("https://localhost:60503/db/test_rep/series?u=paul&p=pass&q=%s", encodedQuery)
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := http.Client{
		Transport: transport,
	}
	resp, err := client.Get(fullUrl)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	var js []interface{}
	err = json.Unmarshal(body, &js)
	c.Assert(err, IsNil)
	collection := ResultsToSeriesCollection(js)
	series := collection.GetSeries("test_sll", c)
	c.Assert(len(series.Points) > 0, Equals, true)
}

func (self *ServerSuite) TestInvalidUserNameAndDbName(c *C) {
	resp := self.serverProcesses[0].Post("/db/dummy_db/users?u=root&p=3rrpl4n3!", "{\"name\":\"foo%bar\", \"password\":\"root\"}", c)
	c.Assert(resp.StatusCode, Not(Equals), http.StatusOK)
	resp = self.serverProcesses[0].Post("/db/dummy%db/users?u=root&p=3rrpl4n3!", "{\"name\":\"foobar\", \"password\":\"root\"}", c)
	c.Assert(resp.StatusCode, Not(Equals), http.StatusOK)
}

func (self *ServerSuite) TestShouldNotResetRootsPassword(c *C) {
	resp := self.serverProcesses[0].Post("/db/dummy_db/users?u=root&p=root", "{\"name\":\"root\", \"password\":\"pass\"}", c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	time.Sleep(time.Second)
	resp = self.serverProcesses[0].Request("GET", "/db/dummy_db/authenticate?u=root&p=pass", "", c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	resp = self.serverProcesses[0].Request("GET", "/cluster_admins/authenticate?u=root&p=root", "", c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
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

// Reported by Alex in the following thread
// https://groups.google.com/forum/#!msg/influxdb/I_Ns6xYiMOc/XilTv6BDgHgJ
func (self *ServerSuite) TestAdminPermissionToDeleteData(c *C) {
	data := `
  [{
    "points": [
        ["val1", 2]
    ],
    "name": "test_delete_admin_permission",
    "columns": ["val_1", "val_2"]
  }]`
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)
	collection := self.serverProcesses[0].QueryAsRoot("test_rep", "select count(val_1) from test_delete_admin_permission", false, c)
	series := collection.GetSeries("test_delete_admin_permission", c)
	c.Assert(series.GetValueForPointAndColumn(0, "count", c), Equals, float64(1))

	self.serverProcesses[0].Query("test_rep", "delete from test_delete_admin_permission", false, c)
	collection = self.serverProcesses[0].Query("test_rep", "select count(val_1) from test_delete_admin_permission", false, c)
	c.Assert(collection.Members, HasLen, 0)
}

func (self *ServerSuite) TestListSeries(c *C) {
	self.serverProcesses[0].Post("/db?u=root&p=root", `{"name": "list_series", "replicationFactor": 2}`, c)
	self.serverProcesses[0].Post("/db/list_series/users?u=root&p=root", `{"name": "paul", "password": "pass"}`, c)
	time.Sleep(time.Second)
	data := `[{
		"name": "cluster_query",
		"columns": ["val1"],
		"points": [[1]]
		}]`
	resp := self.serverProcesses[0].Post("/db/list_series/series?u=paul&p=pass", data, c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	for _, s := range self.serverProcesses {
		collection := s.Query("list_series", "list series", false, c)
		s := collection.GetSeries("cluster_query", c)
		c.Assert(s.Columns, HasLen, 2)
		c.Assert(s.Points, HasLen, 0)
	}
}

func (self *ServerSuite) TestSelectingTimeColumn(c *C) {
	self.serverProcesses[0].Post("/db?u=root&p=root", `{"name": "test_rep", "replicationFactor": 2}`, c)
	self.serverProcesses[0].Post("/db/test_rep/users?u=root&p=root", `{"name": "paul", "password": "pass"}`, c)
	time.Sleep(time.Second)
	data := `[{
		"name": "selecting_time_column",
		"columns": ["val1"],
		"points": [[1]]
		}]`
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)
	for _, s := range self.serverProcesses {
		collection := s.Query("test_rep", "select val1, time from selecting_time_column", false, c)
		s := collection.GetSeries("selecting_time_column", c)
		c.Assert(s.Columns, HasLen, 3)
		c.Assert(s.Points, HasLen, 1)
	}
}

func (self *ServerSuite) TestDropDatabase(c *C) {
	self.serverProcesses[0].Post("/db?u=root&p=root", `{"name": "drop_db", "replicationFactor": 3}`, c)
	self.serverProcesses[0].Post("/db/drop_db/users?u=root&p=root", `{"name": "paul", "password": "pass"}`, c)
	data := `[{
		"name": "cluster_query",
		"columns": ["val1"],
		"points": [[1]]
		}]`
	self.serverProcesses[0].Post("/db/drop_db/series?u=paul&p=pass", data, c)
	time.Sleep(time.Second)
	resp := self.serverProcesses[0].Request("DELETE", "/db/drop_db?u=root&p=root", "", c)
	c.Assert(resp.StatusCode, Equals, http.StatusNoContent)
	time.Sleep(time.Second)
	self.serverProcesses[0].Post("/db?u=root&p=root", `{"name": "drop_db", "replicationFactor": 3}`, c)
	self.serverProcesses[0].Post("/db/drop_db/users?u=root&p=root", `{"name": "paul", "password": "pass"}`, c)
	time.Sleep(time.Second)
	for _, s := range self.serverProcesses {
		fmt.Printf("Running query against: %d\n", s.apiPort)
		collection := s.Query("drop_db", "select * from cluster_query", true, c)
		c.Assert(collection.GetSeries("cluster_query", c).Points, HasLen, 0)
		c.Assert(collection.GetSeries("cluster_query", c).Columns, DeepEquals, []string{"time", "sequence_number"})
	}
}

func (self *ServerSuite) TestDropSeries(c *C) {
	for i := 0; i < 2; i++ {
		self.serverProcesses[0].Post("/db?u=root&p=root", `{"name": "drop_series", "replicationFactor": 3}`, c)
		self.serverProcesses[0].Post("/db/drop_series/users?u=root&p=root", `{"name": "paul", "password": "pass"}`, c)
		data := `[{
		"name": "cluster_query",
		"columns": ["val1"],
		"points": [[1]]
		}]`
		self.serverProcesses[0].Post("/db/drop_series/series?u=paul&p=pass", data, c)
		time.Sleep(time.Second)
		if i == 0 {
			fmt.Printf("Using the http api\n")
			resp := self.serverProcesses[0].Request("DELETE", "/db/drop_series/series/cluster_query?u=root&p=root", "", c)
			c.Assert(resp.StatusCode, Equals, http.StatusNoContent)
		} else {
			fmt.Printf("Using the drop series\n")
			self.serverProcesses[0].Query("drop_series", "drop series cluster_query", false, c)
		}
		time.Sleep(time.Second)
		for _, s := range self.serverProcesses {
			fmt.Printf("Running query against: %d\n", s.apiPort)
			collection := s.Query("drop_series", "select * from cluster_query", true, c)
			c.Assert(collection.GetSeries("cluster_query", c).Points, HasLen, 0)
			c.Assert(collection.GetSeries("cluster_query", c).Columns, DeepEquals, []string{"time", "sequence_number"})
		}
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

	time.Sleep(time.Second) // wait for data to get replicated

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

	expected := []float64{float64(3), 0, float64(3)}
	for i, s := range self.serverProcesses {
		if i == 1 {
			continue
		}

		collection := s.Query("full_rep", "select sum(val) from test_failure_replays;", true, c)
		series := collection.GetSeries("test_failure_replays", c)
		c.Assert(series.GetValueForPointAndColumn(0, "sum", c), Equals, expected[i])
	}

	self.serverProcesses[1].Start()

	for i := 0; i < 3; i++ {
		// wait for the server to startup and the WAL to be synced
		time.Sleep(2 * time.Second)
		collection := self.serverProcesses[1].Query("full_rep", "select sum(val) from test_failure_replays;", true, c)
		series := collection.GetSeries("test_failure_replays", c)
		if series.GetValueForPointAndColumn(0, "sum", c).(float64) == 3 {
			return
		}
	}
	c.Error("write didn't replay properly")
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
	for i, s := range self.serverProcesses {
		if i == 1 {
			continue
		} else {
			collection := s.Query("full_rep", "select sum(val) from test_failure_delete_replays;", true, c)

			c.Assert(collection.Members, HasLen, 0)
		}
	}

	self.serverProcesses[1].Start()
	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Second)
		collection := self.serverProcesses[1].Query("full_rep", "select sum(val) from test_failure_delete_replays;", true, c)
		if len(collection.Members) == 0 {
			return
		}
	}

	c.Error("Delete query didn't replay properly")
}

// For issue #130 https://github.com/influxdb/influxdb/issues/130
func (self *ServerSuite) TestColumnNamesReturnInDistributedQuery(c *C) {
	data := `[{
		"name": "cluster_query_with_columns",
		"columns": ["col1"],
		"points": [[1], [2]]
		}]`
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)
	for _, s := range self.serverProcesses {
		collection := s.Query("test_rep", "select * from cluster_query_with_columns", false, c)
		series := collection.GetSeries("cluster_query_with_columns", c)
		set := map[float64]bool{}
		for idx, _ := range series.Points {
			set[series.GetValueForPointAndColumn(idx, "col1", c).(float64)] = true
		}
		c.Assert(set, DeepEquals, map[float64]bool{1: true, 2: true})
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

func (self *ServerSuite) TestContinuousQueryManagement(c *C) {
	collection := self.serverProcesses[0].QueryAsRoot("test_cq", "list continuous queries;", false, c)
	series := collection.GetSeries("continuous queries", c)
	c.Assert(series.Points, HasLen, 0)

	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from foo into bar;", false, c)

	collection = self.serverProcesses[0].QueryAsRoot("test_cq", "list continuous queries;", false, c)
	series = collection.GetSeries("continuous queries", c)
	c.Assert(series.Points, HasLen, 1)
	c.Assert(series.GetValueForPointAndColumn(0, "id", c), Equals, float64(1))
	c.Assert(series.GetValueForPointAndColumn(0, "query", c), Equals, "select * from foo into bar;")

	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from quu into qux;", false, c)
	collection = self.serverProcesses[0].QueryAsRoot("test_cq", "list continuous queries;", false, c)
	series = collection.GetSeries("continuous queries", c)
	c.Assert(series.Points, HasLen, 2)
	c.Assert(series.GetValueForPointAndColumn(0, "id", c), Equals, float64(1))
	c.Assert(series.GetValueForPointAndColumn(0, "query", c), Equals, "select * from foo into bar;")
	c.Assert(series.GetValueForPointAndColumn(1, "id", c), Equals, float64(2))
	c.Assert(series.GetValueForPointAndColumn(1, "query", c), Equals, "select * from quu into qux;")

	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 1;", false, c)

	collection = self.serverProcesses[0].QueryAsRoot("test_cq", "list continuous queries;", false, c)
	series = collection.GetSeries("continuous queries", c)
	c.Assert(series.Points, HasLen, 1)
	c.Assert(series.GetValueForPointAndColumn(0, "id", c), Equals, float64(2))
	c.Assert(series.GetValueForPointAndColumn(0, "query", c), Equals, "select * from quu into qux;")

	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 2;", false, c)
}

func (self *ServerSuite) TestContinuousQueryFanoutOperations(c *C) {
	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from s1 into d1;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from s2 into d2;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from /s\\d/ into d3;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from silly_name into :series_name.foo;", false, c)
	collection := self.serverProcesses[0].QueryAsRoot("test_cq", "list continuous queries;", false, c)
	series := collection.GetSeries("continuous queries", c)
	c.Assert(series.Points, HasLen, 4)

	data := `[
    {"name": "s1", "columns": ["c1", "c2"], "points": [[1, "a"], [2, "b"]]},
    {"name": "s2", "columns": ["c3"], "points": [[3]]},
    {"name": "silly_name", "columns": ["c4", "c5"], "points": [[4,5]]}
  ]`

	self.serverProcesses[0].Post("/db/test_cq/series?u=paul&p=pass", data, c)

	collection = self.serverProcesses[0].Query("test_cq", "select * from s1;", false, c)
	series = collection.GetSeries("s1", c)
	c.Assert(series.GetValueForPointAndColumn(0, "c1", c), Equals, float64(2))
	c.Assert(series.GetValueForPointAndColumn(0, "c2", c), Equals, "b")
	c.Assert(series.GetValueForPointAndColumn(1, "c1", c), Equals, float64(1))
	c.Assert(series.GetValueForPointAndColumn(1, "c2", c), Equals, "a")

	collection = self.serverProcesses[0].Query("test_cq", "select * from s2;", false, c)
	series = collection.GetSeries("s2", c)
	c.Assert(series.GetValueForPointAndColumn(0, "c3", c), Equals, float64(3))

	collection = self.serverProcesses[0].Query("test_cq", "select * from d1;", false, c)
	series = collection.GetSeries("d1", c)
	c.Assert(series.GetValueForPointAndColumn(0, "c1", c), Equals, float64(2))
	c.Assert(series.GetValueForPointAndColumn(0, "c2", c), Equals, "b")
	c.Assert(series.GetValueForPointAndColumn(1, "c1", c), Equals, float64(1))
	c.Assert(series.GetValueForPointAndColumn(1, "c2", c), Equals, "a")

	collection = self.serverProcesses[0].Query("test_cq", "select * from d2;", false, c)
	series = collection.GetSeries("d2", c)
	c.Assert(series.GetValueForPointAndColumn(0, "c3", c), Equals, float64(3))

	collection = self.serverProcesses[0].Query("test_cq", "select * from d3;", false, c)
	series = collection.GetSeries("d3", c)
	c.Assert(series.GetValueForPointAndColumn(0, "c3", c), Equals, float64(3))
	c.Assert(series.GetValueForPointAndColumn(1, "c1", c), Equals, float64(2))
	c.Assert(series.GetValueForPointAndColumn(1, "c2", c), Equals, "b")
	c.Assert(series.GetValueForPointAndColumn(2, "c1", c), Equals, float64(1))
	c.Assert(series.GetValueForPointAndColumn(2, "c2", c), Equals, "a")

	collection = self.serverProcesses[0].Query("test_cq", "select * from silly_name.foo;", false, c)
	series = collection.GetSeries("silly_name.foo", c)
	c.Assert(series.GetValueForPointAndColumn(0, "c4", c), Equals, float64(4))
	c.Assert(series.GetValueForPointAndColumn(0, "c5", c), Equals, float64(5))

	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 1;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 2;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 3;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 4;", false, c)
}

func (self *ServerSuite) TestContinuousQueryGroupByOperations(c *C) {
	currentTime := time.Now()

	previousTime := currentTime.Truncate(10 * time.Second)
	oldTime := time.Unix(previousTime.Unix()-5, 0).Unix()
	oldOldTime := time.Unix(previousTime.Unix()-10, 0).Unix()

	data := fmt.Sprintf(`[
    {"name": "s3", "columns": ["c1", "c2", "time"], "points": [
      [1, "a", %d],
      [2, "b", %d],
      [3, "c", %d],
      [7, "x", %d],
      [8, "y", %d],
      [9, "z", %d]
    ]}
  ]`, oldTime, oldTime, oldTime, oldOldTime, oldOldTime, oldOldTime)

	fmt.Println(data)

	self.serverProcesses[0].Post("/db/test_cq/series?u=paul&p=pass&time_precision=s", data, c)

	time.Sleep(time.Second)

	self.serverProcesses[0].QueryAsRoot("test_cq", "select mean(c1) from s3 group by time(5s) into d3.mean;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "select count(c2) from s3 group by time(5s) into d3.count;", false, c)

	collection := self.serverProcesses[0].QueryAsRoot("test_cq", "list continuous queries;", false, c)
	series := collection.GetSeries("continuous queries", c)
	c.Assert(series.Points, HasLen, 2)

	time.Sleep(2 * time.Second)

	collection = self.serverProcesses[0].Query("test_cq", "select * from s3;", false, c)
	series = collection.GetSeries("s3", c)
	c.Assert(series.Points, HasLen, 6)

	collection = self.serverProcesses[0].Query("test_cq", "select * from d3.mean;", false, c)
	series = collection.GetSeries("d3.mean", c)
	c.Assert(series.GetValueForPointAndColumn(0, "mean", c), Equals, float64(2))
	c.Assert(series.GetValueForPointAndColumn(1, "mean", c), Equals, float64(8))

	collection = self.serverProcesses[0].Query("test_cq", "select * from d3.count;", false, c)
	series = collection.GetSeries("d3.count", c)
	c.Assert(series.GetValueForPointAndColumn(0, "count", c), Equals, float64(3))
	c.Assert(series.GetValueForPointAndColumn(1, "count", c), Equals, float64(3))

	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 1;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 2;", false, c)
}
