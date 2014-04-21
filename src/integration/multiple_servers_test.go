package integration

import (
	"common"
	"crypto/tls"
	"encoding/json"
	"fmt"
	. "integration/helpers"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	influxdb "github.com/influxdb/influxdb-go"
	. "launchpad.net/gocheck"
)

type ServerSuite struct {
	serverProcesses []*Server
}

var _ = Suite(&ServerSuite{})

func (self *ServerSuite) precreateShards(server *Server, c *C) {
	self.createShards(server, int64(3600), "false", c)
	self.createShards(server, int64(86400), "true", c)
	server.WaitForServerToSync()
}

func (self ServerSuite) createShards(server *Server, bucketSize int64, longTerm string, c *C) {
	serverCount := 3
	nowBucket := time.Now().Unix() / bucketSize * bucketSize
	startIndex := 0

	for i := 0; i <= 50; i++ {
		serverId1 := startIndex%serverCount + 1
		startIndex += 1
		serverId2 := startIndex%serverCount + 1
		startIndex += 1
		data := fmt.Sprintf(`{
			"startTime":%d,
			"endTime":%d,
			"longTerm": %s,
			"shards": [{
				"serverIds": [%d, %d]
			}]
		}`, nowBucket, nowBucket+bucketSize, longTerm, serverId1, serverId2)

		resp := server.Post("/cluster/shards?u=root&p=root", data, c)
		c.Assert(resp.StatusCode, Equals, http.StatusAccepted)
		nowBucket -= bucketSize
	}
}

func (self *ServerSuite) SetUpSuite(c *C) {
	err := os.RemoveAll("/tmp/influxdb/test")
	c.Assert(err, IsNil)
	self.serverProcesses = []*Server{
		NewServer("src/integration/test_config1.toml", c),
		NewServer("src/integration/test_config2.toml", c),
		NewServer("src/integration/test_config3.toml", c),
	}
	self.serverProcesses[0].SetSslOnly(true)
	client := self.serverProcesses[0].GetClient("", c)
	dbs := []string{"full_rep", "test_rep", "single_rep", "test_cq", "test_cq_null", "drop_db"}
	for _, db := range dbs {
		c.Assert(client.CreateDatabase(db), IsNil)
	}
	for _, db := range dbs {
		c.Assert(client.CreateDatabaseUser(db, "paul", "pass"), IsNil)
		c.Assert(client.AlterDatabasePrivilege(db, "paul", true), IsNil)
		c.Assert(client.CreateDatabaseUser(db, "weakpaul", "pass"), IsNil)
	}
	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}
	self.precreateShards(self.serverProcesses[0], c)
}

func (self *ServerSuite) TearDownSuite(c *C) {
	for _, s := range self.serverProcesses {
		s.Stop()
	}
}

func (self *ServerSuite) TestGraphiteInterface(c *C) {
	conn, err := net.Dial("tcp", "localhost:60513")
	c.Assert(err, IsNil)

	now := time.Now().UTC().Truncate(time.Minute)
	data := fmt.Sprintf("some_metric 100 %d\nsome_metric 200.5 %d\n", now.Add(-time.Minute).Unix(), now.Unix())

	_, err = conn.Write([]byte(data))
	c.Assert(err, IsNil)
	conn.Close()

	// there's no easy way to check whether the server started
	// processing this request, unlike http requests which must return a
	// status code
	time.Sleep(time.Second)

	self.serverProcesses[0].WaitForServerToSync()

	collection := self.serverProcesses[0].QueryWithUsername("graphite_db", "select * from some_metric", false, c, "root", "root")
	c.Assert(collection.Members, HasLen, 1)
	series := collection.GetSeries("some_metric", c)
	c.Assert(series.Points, HasLen, 2)
	c.Assert(series.GetValueForPointAndColumn(0, "value", c), Equals, 200.5)
	c.Assert(series.GetValueForPointAndColumn(1, "value", c), Equals, 100.0)
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
	self.serverProcesses[0].WaitForServerToSync()

	collection := self.serverProcesses[0].Query("test_rep", "select * from test_restart_after_compaction", false, c)
	c.Assert(collection.Members, HasLen, 1)
	series := collection.GetSeries("test_restart_after_compaction", c)
	c.Assert(series.Points, HasLen, 1)
	resp := self.serverProcesses[0].Post("/raft/force_compaction?u=root&p=root", "", c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	self.serverProcesses[0].Stop()
	self.serverProcesses[0].Start()
	self.serverProcesses[0].WaitForServerToStart()

	collection = self.serverProcesses[0].Query("test_rep", "select * from test_restart_after_compaction", false, c)
	c.Assert(collection.Members, HasLen, 1)
	series = collection.GetSeries("test_restart_after_compaction", c)
	c.Assert(series.Points, HasLen, 1)
}

func (self *ServerSuite) TestEntireClusterReStartAfterCompaction(c *C) {
	for i := 0; i < 3; i++ {
		resp := self.serverProcesses[i].Post("/raft/force_compaction?u=root&p=root", "", c)
		c.Assert(resp.StatusCode, Equals, http.StatusOK)
	}

	for i := 0; i < 3; i++ {
		self.serverProcesses[i].Stop()
	}

	for i := 0; i < 3; i++ {
		self.serverProcesses[i].Start()
	}

	for i := 0; i < 3; i++ {
		self.serverProcesses[i].WaitForServerToStart()
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

	self.serverProcesses[0].WaitForServerToSync()

	collection := self.serverProcesses[0].Query("test_rep", "select * from test_restart", false, c)
	c.Assert(collection.Members, HasLen, 1)
	series := collection.GetSeries("test_restart", c)
	c.Assert(series.Points, HasLen, 1)

	for _, s := range self.serverProcesses {
		s.Stop()
	}

	for _, s := range self.serverProcesses {
		c.Assert(s.Start(), IsNil)
	}

	for _, s := range self.serverProcesses {
		s.WaitForServerToStart()
	}

	self.serverProcesses[0].WaitForServerToSync()

	collection = self.serverProcesses[0].Query("test_rep", "select * from test_restart", false, c)
	c.Assert(collection.Members, HasLen, 1)
	series = collection.GetSeries("test_restart", c)
	c.Assert(series.Points, HasLen, 1)
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
	var js []*common.SerializedSeries
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
	self.serverProcesses[0].WaitForServerToSync()
	resp = self.serverProcesses[0].Request("GET", "/db/dummy_db/authenticate?u=root&p=pass", "", c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	resp = self.serverProcesses[0].Request("GET", "/cluster_admins/authenticate?u=root&p=root", "", c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}

func (self *ServerSuite) TestDeleteFullReplication(c *C) {
	data := `
  [{
    "points": [
        ["val1", 2]
    ],
    "name": "test_delete_full_replication",
    "columns": ["val_1", "val_2"]
  }]`
	self.serverProcesses[0].Post("/db/full_rep/series?u=paul&p=pass", data, c)
	self.serverProcesses[0].WaitForServerToSync()
	collection := self.serverProcesses[0].Query("full_rep", "select count(val_1) from test_delete_full_replication", true, c)
	series := collection.GetSeries("test_delete_full_replication", c)
	c.Assert(series.GetValueForPointAndColumn(0, "count", c), Equals, float64(1))
	self.serverProcesses[0].Query("full_rep", "delete from test_delete_full_replication", false, c)

	for _, s := range self.serverProcesses {
		collection = s.Query("full_rep", "select count(val_1) from test_delete_full_replication", true, c)
		c.Assert(collection.Members, HasLen, 0)
	}
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
	self.serverProcesses[0].WaitForServerToSync()
	collection := self.serverProcesses[0].Query("test_rep", "select count(val_1) from test_delete_replication", false, c)
	series := collection.GetSeries("test_delete_replication", c)
	c.Assert(series.GetValueForPointAndColumn(0, "count", c), Equals, float64(1))

	for _, s := range self.serverProcesses {
		s.Query("test_rep", "delete from test_delete_replication", false, c)
		collection = self.serverProcesses[0].Query("test_rep", "select count(val_1) from test_delete_replication", false, c)
		c.Assert(collection.Members, HasLen, 0)
	}
}

// Reported by Alex in the following thread
// https://groups.google.com/forum/#!msg/influxdb/I_Ns6xYiMOc/XilTv6BDgHgJ
func (self *ServerSuite) TestDbAdminPermissionToDeleteData(c *C) {
	data := `
  [{
    "points": [
        ["val1", 2]
    ],
    "name": "test_delete_admin_permission",
    "columns": ["val_1", "val_2"]
  }]`
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)
	self.serverProcesses[0].WaitForServerToSync()
	collection := self.serverProcesses[0].QueryAsRoot("test_rep", "select count(val_1) from test_delete_admin_permission", false, c)
	series := collection.GetSeries("test_delete_admin_permission", c)
	c.Assert(series.GetValueForPointAndColumn(0, "count", c), Equals, float64(1))

	self.serverProcesses[0].Query("test_rep", "delete from test_delete_admin_permission", false, c)
	collection = self.serverProcesses[0].Query("test_rep", "select count(val_1) from test_delete_admin_permission", false, c)
	c.Assert(collection.Members, HasLen, 0)
}

func (self *ServerSuite) TestClusterAdminPermissionToDeleteData(c *C) {
	data := `
  [{
    "points": [
        ["val1", 2]
    ],
    "name": "test_delete_admin_permission",
    "columns": ["val_1", "val_2"]
  }]`
	self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)
	self.serverProcesses[0].WaitForServerToSync()
	collection := self.serverProcesses[0].QueryAsRoot("test_rep", "select count(val_1) from test_delete_admin_permission", false, c)
	series := collection.GetSeries("test_delete_admin_permission", c)
	c.Assert(series.GetValueForPointAndColumn(0, "count", c), Equals, float64(1))

	self.serverProcesses[0].QueryAsRoot("test_rep", "delete from test_delete_admin_permission", false, c)
	collection = self.serverProcesses[0].Query("test_rep", "select count(val_1) from test_delete_admin_permission", false, c)
	c.Assert(collection.Members, HasLen, 0)
}

func (self *ServerSuite) TestDropDatabase(c *C) {
	data := `[{
		"name": "cluster_query",
		"columns": ["val1"],
		"points": [[1]]
		}]`
	self.serverProcesses[0].Post("/db/drop_db/series?u=paul&p=pass", data, c)
	self.serverProcesses[0].WaitForServerToSync()
	resp := self.serverProcesses[0].Delete("/db/drop_db?u=root&p=root", "", c)
	c.Assert(resp.StatusCode, Equals, http.StatusNoContent)
	self.serverProcesses[0].Post("/db?u=root&p=root", `{"name": "drop_db", "replicationFactor": 3}`, c)
	self.serverProcesses[0].Post("/db/drop_db/users?u=root&p=root", `{"name": "paul", "password": "pass"}`, c)
	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}
	for _, s := range self.serverProcesses {
		fmt.Printf("Running query against: %d\n", s.ApiPort())
		collection := s.Query("drop_db", "select * from cluster_query", true, c)
		c.Assert(collection.Members, HasLen, 0)
	}
}

func (self *ServerSuite) TestDropSeries(c *C) {
	for i := 0; i < 3; i++ {
		self.serverProcesses[0].Post("/db?u=root&p=root", `{"name": "drop_series", "replicationFactor": 3}`, c)
		self.serverProcesses[0].Post("/db/drop_series/users?u=root&p=root", `{"name": "paul", "password": "pass"}`, c)
		for _, s := range self.serverProcesses {
			s.WaitForServerToSync()
		}
		data := `[{
		"name": "cluster_query.1",
		"columns": ["val1"],
		"points": [[1]]
		}]`
		self.serverProcesses[0].Post("/db/drop_series/series?u=paul&p=pass", data, c)
		self.serverProcesses[0].WaitForServerToSync()
		for _, s := range self.serverProcesses {
			fmt.Printf("Running query against: %d\n", s.ApiPort())
			collection := s.Query("drop_series", "select * from cluster_query.1", true, c)
			c.Assert(collection.Members, HasLen, 1)
			series := collection.GetSeries("cluster_query.1", c)
			c.Assert(series.SerializedSeries.Points, HasLen, 1)
		}
		switch i {
		case 0:
			fmt.Printf("Using the http api\n")
			resp := self.serverProcesses[0].Delete("/db/drop_series/series/cluster_query.1?u=root&p=root", "", c)
			c.Assert(resp.StatusCode, Equals, http.StatusNoContent)
		case 1:
			fmt.Printf("Using the drop series\n")
			self.serverProcesses[0].Query("drop_series", "drop series cluster_query.1", false, c)
		case 2:
			resp := self.serverProcesses[0].Delete("/db/drop_series?u=root&p=root", "", c)
			c.Assert(resp.StatusCode, Equals, http.StatusNoContent)
			self.serverProcesses[0].Post("/db/drop_series/users?u=root&p=root", `{"name": "paul", "password": "pass"}`, c)
		}

		for _, s := range self.serverProcesses {
			s.WaitForServerToSync()
		}

		for _, s := range self.serverProcesses {
			fmt.Printf("Running query against: %d\n", s.ApiPort())
			collection := s.Query("drop_series", "select * from cluster_query.1", true, c)
			c.Assert(collection.Members, HasLen, 0)
		}
	}
}

func (self *ServerSuite) TestRelogging(c *C) {
	// write data and confirm that it went to all three servers
	data := `
  [{
    "points": [
        [1]
    ],
    "name": "test_relogging",
    "columns": ["val"]
  }]`

	self.serverProcesses[0].Post("/db/full_rep/series?u=paul&p=pass", data, c)

	self.serverProcesses[0].WaitForServerToSync()

	self.serverProcesses[0].Query("full_rep", "delete from test_relogging", false, c)

	for _, server := range self.serverProcesses[1:] {
		err := server.DoesWalExist()
		c.Assert(os.IsNotExist(err), Equals, true)
		server.Stop()
		server.Start()
	}

	for _, server := range self.serverProcesses[1:] {
		server.WaitForServerToStart()
		err := server.DoesWalExist()
		c.Assert(os.IsNotExist(err), Equals, true)
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

	self.serverProcesses[0].WaitForServerToSync()

	for _, s := range self.serverProcesses {
		collection := s.Query("full_rep", "select sum(val) from test_failure_replays;", true, c)
		series := collection.GetSeries("test_failure_replays", c)
		c.Assert(series.Points, HasLen, 1)
	}

	self.serverProcesses[1].Stop()
	// wait for the server to be marked down
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
	self.serverProcesses[1].WaitForServerToStart()
	self.serverProcesses[0].WaitForServerToSync()

	for i := 0; i < 3; i++ {
		// wait for the server to Startup and the WAL to be synced
		collection := self.serverProcesses[1].Query("full_rep", "select sum(val) from test_failure_replays;", true, c)
		series := collection.GetSeries("test_failure_replays", c)
		if series.GetValueForPointAndColumn(0, "sum", c).(float64) == 3 {
			return
		}
	}
	c.Error("write didn't replay properly")
}

func generateHttpApiSeries(name string, n int) *common.SerializedSeries {
	points := [][]interface{}{}

	for i := 0; i < n; i++ {
		points = append(points, []interface{}{i})
	}

	return &common.SerializedSeries{
		Name:    name,
		Columns: []string{"value"},
		Points:  points,
	}
}

func (self ServerSuite) RemoveAllContinuousQueries(db string, c *C) {
	client := self.serverProcesses[0].GetClient(db, c)
	queries, err := client.GetContinuousQueries()
	c.Assert(err, IsNil)
	for _, q := range queries {
		c.Assert(client.DeleteContinuousQueries(int(q["id"].(float64))), IsNil)
	}
}

func (self ServerSuite) AssertContinuousQueryCount(db string, count int, c *C) {
	client := self.serverProcesses[0].GetClient(db, c)
	queries, err := client.GetContinuousQueries()
	c.Assert(err, IsNil)
	c.Assert(queries, HasLen, count)
}

func (self *ServerSuite) TestContinuousQueryManagement(c *C) {
	defer self.RemoveAllContinuousQueries("test_cq", c)

	collection := self.serverProcesses[0].QueryAsRoot("test_cq", "list continuous queries;", false, c)
	series := collection.GetSeries("continuous queries", c)
	c.Assert(series.Points, HasLen, 0)

	response := self.serverProcesses[0].VerifyForbiddenQuery("test_cq", "select * from foo into bar;", false, c, "weakpaul", "pass")
	c.Assert(response, Equals, "Insufficient permissions to create continuous query")

	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from foo into bar;", false, c)
	self.serverProcesses[0].WaitForServerToSync()

	collection = self.serverProcesses[0].QueryAsRoot("test_cq", "list continuous queries;", false, c)
	series = collection.GetSeries("continuous queries", c)
	c.Assert(series.Points, HasLen, 1)
	c.Assert(series.GetValueForPointAndColumn(0, "id", c), Equals, float64(1))
	c.Assert(series.GetValueForPointAndColumn(0, "query", c), Equals, "select * from foo into bar;")

	// wait for the continuous query to run
	time.Sleep(time.Second)

	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from quu into qux;", false, c)
	collection = self.serverProcesses[0].QueryAsRoot("test_cq", "list continuous queries;", false, c)
	series = collection.GetSeries("continuous queries", c)
	c.Assert(series.Points, HasLen, 2)
	c.Assert(series.GetValueForPointAndColumn(0, "id", c), Equals, float64(1))
	c.Assert(series.GetValueForPointAndColumn(0, "query", c), Equals, "select * from foo into bar;")
	c.Assert(series.GetValueForPointAndColumn(1, "id", c), Equals, float64(2))
	c.Assert(series.GetValueForPointAndColumn(1, "query", c), Equals, "select * from quu into qux;")

	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 1;", false, c)
	// wait for the continuous query to be dropped
	self.serverProcesses[0].WaitForServerToSync()

	collection = self.serverProcesses[0].QueryAsRoot("test_cq", "list continuous queries;", false, c)
	series = collection.GetSeries("continuous queries", c)
	c.Assert(series.Points, HasLen, 1)
	c.Assert(series.GetValueForPointAndColumn(0, "id", c), Equals, float64(2))
	c.Assert(series.GetValueForPointAndColumn(0, "query", c), Equals, "select * from quu into qux;")

	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 2;", false, c)
}

func (self *ServerSuite) TestContinuousQueryFanoutOperations(c *C) {
	defer self.RemoveAllContinuousQueries("test_cq", c)

	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from s1 into d1;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from s2 into d2;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from /s\\d/ into d3;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from silly_name into :series_name.foo;", false, c)
	defer self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 1;", false, c)
	defer self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 2;", false, c)
	defer self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 3;", false, c)
	defer self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 4;", false, c)
	self.serverProcesses[0].WaitForServerToSync()
	collection := self.serverProcesses[0].QueryAsRoot("test_cq", "list continuous queries;", false, c)
	series := collection.GetSeries("continuous queries", c)
	c.Assert(series.Points, HasLen, 4)

	data := `[
    {"name": "s1", "columns": ["c1", "c2"], "points": [[1, "a"], [2, "b"]]},
    {"name": "s2", "columns": ["c3"], "points": [[3]]},
    {"name": "silly_name", "columns": ["c4", "c5"], "points": [[4,5]]}
  ]`

	self.serverProcesses[0].Post("/db/test_cq/series?u=paul&p=pass", data, c)
	self.serverProcesses[0].WaitForServerToSync()

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
}

func (self *ServerSuite) TestContinuousQueryGroupByOperations(c *C) {
	defer self.RemoveAllContinuousQueries("test_cq", c)

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

	self.serverProcesses[0].Post("/db/test_cq/series?u=paul&p=pass&time_precision=s", data, c)

	self.serverProcesses[0].WaitForServerToSync()

	self.serverProcesses[0].QueryAsRoot("test_cq", "select mean(c1) from s3 group by time(5s) into d3.mean;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "select count(c2) from s3 group by time(5s) into d3.count;", false, c)

	// wait for replication
	self.serverProcesses[0].WaitForServerToSync()
	// wait for the query to run
	time.Sleep(time.Second)

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

func (self *ServerSuite) TestContinuousQueryGroupByOperationsWithNullColumns(c *C) {
	defer self.RemoveAllContinuousQueries("test_cq_null", c)

	client := self.serverProcesses[0].GetClient("test_cq_null", c)
	for i := 0; i < 2; i++ {
		columns := []string{"a"}
		values := []interface{}{1}
		if i == 1 {
			columns = append(columns, "b")
			values = append(values, 2)
		}
		series := []*influxdb.Series{
			&influxdb.Series{
				Name:    "test_null_columns",
				Columns: columns,
				Points: [][]interface{}{
					values,
				},
			},
		}
		c.Assert(client.WriteSeries(series), IsNil)
	}

	self.serverProcesses[0].WaitForServerToSync()

	_, err := client.Query("select count(a), b from test_null_columns group by time(1s), b into test_null_columns.1s")
	c.Assert(err, IsNil)

	// wait for the query to run
	time.Sleep(2 * time.Second)

	self.serverProcesses[0].WaitForServerToSync()

	s, err := client.Query("select count(count) from test_null_columns.1s")
	c.Assert(err, IsNil)
	c.Assert(s, HasLen, 1)
	maps := ToMap(s[0])
	c.Assert(maps, HasLen, 1)
	// make sure the continuous query inserted two points
	c.Assert(maps[0]["count"], Equals, 2.0)
}

func (self *ServerSuite) TestContinuousQueryInterpolation(c *C) {
	defer self.RemoveAllContinuousQueries("test_cq", c)

	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from s1 into :series_name.foo;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from s2 into :series_name.foo.[c3];", false, c)
	/* self.serverProcesses[0].QueryAsRoot("test_cq", "select average(c4), count(c5) from s3 group by time(1h) into [average].[count];", false, c) */
	self.serverProcesses[0].QueryAsRoot("test_cq", "select * from s4 into :series_name.foo.[c6].[c7];", false, c)

	data := `[
    {"name": "s1", "columns": ["c1", "c2"], "points": [[1, "a"], [2, "b"]]},
    {"name": "s2", "columns": ["c3"], "points": [[3]]},
    {"name": "s3", "columns": ["c4", "c5"], "points": [[4,5], [5,6], [6,7]]},
    {"name": "s4", "columns": ["c6", "c7", "c8"], "points": [[1, "a", 10], [2, "b", 11]]}
  ]`

	self.serverProcesses[0].Post("/db/test_cq/series?u=paul&p=pass", data, c)

	// wait for replication
	self.serverProcesses[0].WaitForServerToSync()
	// wait for the query to run
	time.Sleep(time.Second)

	collection := self.serverProcesses[0].QueryAsRoot("test_cq", "list continuous queries;", false, c)
	series := collection.GetSeries("continuous queries", c)
	c.Assert(series.Points, HasLen, 3)

	collection = self.serverProcesses[0].Query("test_cq", "select * from s1;", false, c)
	series = collection.GetSeries("s1", c)
	c.Assert(series.GetValueForPointAndColumn(0, "c1", c), Equals, float64(2))
	c.Assert(series.GetValueForPointAndColumn(0, "c2", c), Equals, "b")
	c.Assert(series.GetValueForPointAndColumn(1, "c1", c), Equals, float64(1))
	c.Assert(series.GetValueForPointAndColumn(1, "c2", c), Equals, "a")

	collection = self.serverProcesses[0].Query("test_cq", "select * from s2;", false, c)
	series = collection.GetSeries("s2", c)
	c.Assert(series.GetValueForPointAndColumn(0, "c3", c), Equals, float64(3))

	collection = self.serverProcesses[0].Query("test_cq", "select * from s1.foo;", false, c)
	series = collection.GetSeries("s1.foo", c)
	c.Assert(series.GetValueForPointAndColumn(0, "c1", c), Equals, float64(2))
	c.Assert(series.GetValueForPointAndColumn(0, "c2", c), Equals, "b")
	c.Assert(series.GetValueForPointAndColumn(1, "c1", c), Equals, float64(1))
	c.Assert(series.GetValueForPointAndColumn(1, "c2", c), Equals, "a")

	collection = self.serverProcesses[0].Query("test_cq", "select * from s2.foo.3;", false, c)
	series = collection.GetSeries("s2.foo.3", c)
	c.Assert(series.GetValueForPointAndColumn(0, "c3", c), Equals, float64(3))

	collection = self.serverProcesses[0].Query("test_cq", "select * from s4.foo.1.a;", false, c)
	series = collection.GetSeries("s4.foo.1.a", c)
	c.Assert(series.GetValueForPointAndColumn(0, "c6", c), Equals, float64(1))
	c.Assert(series.GetValueForPointAndColumn(0, "c7", c), Equals, "a")
	c.Assert(series.GetValueForPointAndColumn(0, "c8", c), Equals, float64(10))

	collection = self.serverProcesses[0].Query("test_cq", "select * from s4.foo.2.b;", false, c)
	series = collection.GetSeries("s4.foo.2.b", c)
	c.Assert(series.GetValueForPointAndColumn(0, "c6", c), Equals, float64(2))
	c.Assert(series.GetValueForPointAndColumn(0, "c7", c), Equals, "b")
	c.Assert(series.GetValueForPointAndColumn(0, "c8", c), Equals, float64(11))

	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 1;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 2;", false, c)
	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 3;", false, c)
	/* self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 4;", false, c) */
}

func (self *ServerSuite) TestContinuousQuerySequenceNumberAssignmentWithInterpolation(c *C) {
	defer self.RemoveAllContinuousQueries("test_cq", c)

	currentTime := time.Now()
	t0 := currentTime.Truncate(10 * time.Second)
	t1 := time.Unix(t0.Unix()-5, 0).Unix()
	t2 := time.Unix(t0.Unix()-10, 0).Unix()

	data := fmt.Sprintf(`[
    { "name": "points",
      "columns": ["c1", "c2", "time"],
      "points": [
        [1, "a", %d],
        [2, "a", %d],
        [3, "a", %d],
        [7, "b", %d],
        [8, "b", %d],
        [9, "b", %d]
    ]}
  ]`, t1, t1, t1, t2, t2, t2)

	self.serverProcesses[0].Post("/db/test_cq/series?u=paul&p=pass", data, c)

	self.serverProcesses[0].QueryAsRoot("test_cq", "select count(c1) from points group by time(5s), c2 into :series_name.count.[c2];", false, c)
	self.AssertContinuousQueryCount("test_cq", 1, c)
	self.serverProcesses[0].WaitForServerToSync()
	time.Sleep(time.Second)

	self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 1;", false, c)

	data = fmt.Sprintf(`[
    { "name": "points",
      "columns": ["c1", "c2", "time"],
      "points": [
        [1, "aa", %d],
        [2, "aa", %d],
        [3, "aa", %d],
        [7, "bb", %d],
        [8, "bb", %d],
        [9, "bb", %d]
    ]}
  ]`, t1, t1, t1, t2, t2, t2)

	self.serverProcesses[0].Post("/db/test_cq/series?u=paul&p=pass", data, c)

	self.serverProcesses[0].QueryAsRoot("test_cq", "select count(c1) from points group by time(5s), c2 into :series_name.count.[c2];", false, c)
	self.AssertContinuousQueryCount("test_cq", 1, c)
	self.serverProcesses[0].WaitForServerToSync()
	time.Sleep(time.Second)

	collection := self.serverProcesses[0].Query("test_cq", "select * from points;", false, c)
	series := collection.GetSeries("points", c)
	c.Assert(series.Points, HasLen, 12)

	collection = self.serverProcesses[0].Query("test_cq", "select * from /points.count.*/;", false, c)
	c.Assert(collection.Members, HasLen, 4)

	subseries := []string{"a", "aa", "b", "bb"}
	for i := range subseries {
		series = collection.GetSeries("points.count."+subseries[i], c)
		c.Assert(series.Points, HasLen, 1)
		c.Assert(series.Points[0][1], Equals, float64(1))
	}
}

func (self *ServerSuite) TestGetServers(c *C) {
	body := self.serverProcesses[0].Get("/cluster/servers?u=root&p=root", c)

	res := make([]interface{}, 0)
	err := json.Unmarshal(body, &res)
	c.Assert(err, IsNil)
	for _, js := range res {
		server := js.(map[string]interface{})
		c.Assert(server["id"], NotNil)
		c.Assert(server["protobufConnectString"], NotNil)
	}
}

func (self *ServerSuite) TestCreateAndGetShards(c *C) {
	// put this far in the future so it doesn't mess up the other tests
	secondsOffset := int64(86400 * 365)
	startSeconds := time.Now().Unix() + secondsOffset
	endSeconds := startSeconds + 3600
	data := fmt.Sprintf(`{
		"startTime":%d,
		"endTime":%d,
		"longTerm": false,
		"shards": [{
			"serverIds": [%d, %d]
		}]
	}`, startSeconds, endSeconds, 2, 3)
	resp := self.serverProcesses[0].Post("/cluster/shards?u=root&p=root", data, c)
	c.Assert(resp.StatusCode, Equals, http.StatusAccepted)
	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}
	for _, s := range self.serverProcesses {
		body := s.Get("/cluster/shards?u=root&p=root", c)
		res := make(map[string]interface{})
		err := json.Unmarshal(body, &res)
		c.Assert(err, IsNil)
		hasShard := false
		for _, s := range res["shortTerm"].([]interface{}) {
			sh := s.(map[string]interface{})
			if sh["startTime"].(float64) == float64(startSeconds) && sh["endTime"].(float64) == float64(endSeconds) {
				servers := sh["serverIds"].([]interface{})
				c.Assert(servers, HasLen, 2)
				c.Assert(servers[0].(float64), Equals, float64(2))
				c.Assert(servers[1].(float64), Equals, float64(3))
				hasShard = true
				break
			}
		}
		c.Assert(hasShard, Equals, true)
	}

}

func (self *ServerSuite) TestDropShard(c *C) {
	// put this far in the future so it doesn't mess up the other tests
	secondsOffset := int64(86400 * 720)
	startSeconds := time.Now().Unix() + secondsOffset
	endSeconds := startSeconds + 3600
	data := fmt.Sprintf(`{
		"startTime":%d,
		"endTime":%d,
		"longTerm": false,
		"shards": [{
			"serverIds": [%d, %d]
		}]
	}`, startSeconds, endSeconds, 1, 2)
	resp := self.serverProcesses[0].Post("/cluster/shards?u=root&p=root", data, c)
	c.Assert(resp.StatusCode, Equals, http.StatusAccepted)

	// now write some data to ensure that the local files get created
	t := (time.Now().Unix() + secondsOffset) * 1000
	data = fmt.Sprintf(`[{"points": [[2, %d]], "name": "test_drop_shard", "columns": ["value", "time"]}]`, t)
	resp = self.serverProcesses[0].Post("/db/test_rep/series?u=paul&p=pass", data, c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}

	// and find the shard id
	body := self.serverProcesses[0].Get("/cluster/shards?u=root&p=root", c)
	res := make(map[string]interface{})
	err := json.Unmarshal(body, &res)
	c.Assert(err, IsNil)
	var shardId int
	for _, s := range res["shortTerm"].([]interface{}) {
		sh := s.(map[string]interface{})
		if sh["startTime"].(float64) == float64(startSeconds) && sh["endTime"].(float64) == float64(endSeconds) {
			shardId = int(sh["id"].(float64))
			break
		}
	}

	// confirm the shard files are there
	shardDirServer1 := fmt.Sprintf("/tmp/influxdb/test/1/db/shard_db/%.5d", shardId)
	shardDirServer2 := fmt.Sprintf("/tmp/influxdb/test/2/db/shard_db/%.5d", shardId)
	exists, _ := dirExists(shardDirServer1)
	c.Assert(exists, Equals, true)
	exists, _ = dirExists(shardDirServer2)
	c.Assert(exists, Equals, true)

	// now drop and confirm they're gone
	data = fmt.Sprintf(`{"serverIds": [%d]}`, 1)

	resp = self.serverProcesses[0].Delete(fmt.Sprintf("/cluster/shards/%d?u=root&p=root", shardId), data, c)
	c.Assert(resp.StatusCode, Equals, http.StatusAccepted)
	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}

	for _, s := range self.serverProcesses {
		body := s.Get("/cluster/shards?u=root&p=root", c)
		res := make(map[string]interface{})
		err := json.Unmarshal(body, &res)
		c.Assert(err, IsNil)
		hasShard := false
		for _, s := range res["shortTerm"].([]interface{}) {
			sh := s.(map[string]interface{})
			if int(sh["id"].(float64)) == shardId {
				hasShard = true
				hasServer := false
				for _, serverId := range sh["serverIds"].([]interface{}) {
					if serverId.(float64) == float64(1) {
						hasServer = true
						break
					}
				}
				c.Assert(hasServer, Equals, false)
			}
		}
		c.Assert(hasShard, Equals, true)
	}

	exists, _ = dirExists(shardDirServer1)
	c.Assert(exists, Equals, false)
	exists, _ = dirExists(shardDirServer2)
	c.Assert(exists, Equals, true)

	// now drop the shard from the last server and confirm it's gone
	data = fmt.Sprintf(`{"serverIds": [%d]}`, 2)

	resp = self.serverProcesses[0].Delete(fmt.Sprintf("/cluster/shards/%d?u=root&p=root", shardId), data, c)
	c.Assert(resp.StatusCode, Equals, http.StatusAccepted)
	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}

	for _, s := range self.serverProcesses {
		body := s.Get("/cluster/shards?u=root&p=root", c)
		res := make(map[string]interface{})
		err := json.Unmarshal(body, &res)
		c.Assert(err, IsNil)
		hasShard := false
		for _, s := range res["shortTerm"].([]interface{}) {
			sh := s.(map[string]interface{})
			if int(sh["id"].(float64)) == shardId {
				hasShard = true
			}
		}
		c.Assert(hasShard, Equals, false)
	}

	exists, _ = dirExists(shardDirServer1)
	c.Assert(exists, Equals, false)
	exists, _ = dirExists(shardDirServer2)
	c.Assert(exists, Equals, false)
}

func dirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (self *ServerSuite) TestContinuousQueryWithMixedGroupByOperations(c *C) {
	defer self.RemoveAllContinuousQueries("test_cq", c)

	data := fmt.Sprintf(`[
  {
    "name": "cqtest",
    "columns": ["time", "reqtime", "url"],
    "points": [
        [0, 8.0, "/login"],
        [0, 3.0, "/list"],
        [0, 4.0, "/register"],
        [5, 9.0, "/login"],
        [5, 4.0, "/list"],
        [5, 5.0, "/register"]
    ]
  }
  ]`)

	self.serverProcesses[0].Post("/db/test_cq/series?u=paul&p=pass&time_precision=s", data, c)
	// wait for the data to get written
	self.serverProcesses[0].WaitForServerToSync()
	// wait for the query to run
	time.Sleep(time.Second)

	self.serverProcesses[0].QueryAsRoot("test_cq", "select mean(reqtime), url from cqtest group by time(10s), url into cqtest.10s", false, c)
	defer self.serverProcesses[0].QueryAsRoot("test_cq", "drop continuous query 1;", false, c)

	// wait for the continuous query to run
	time.Sleep(time.Second)
	// wait for the continuous queries to propagate
	self.serverProcesses[0].WaitForServerToSync()

	collection := self.serverProcesses[0].QueryAsRoot("test_cq", "select * from cqtest.10s", false, c)
	series := collection.GetSeries("cqtest.10s", c)

	c.Assert(series.GetValueForPointAndColumn(0, "mean", c), Equals, float64(8.5))
	c.Assert(series.GetValueForPointAndColumn(0, "url", c), Equals, "/login")
	c.Assert(series.GetValueForPointAndColumn(1, "mean", c), Equals, float64(3.5))
	c.Assert(series.GetValueForPointAndColumn(1, "url", c), Equals, "/list")
	c.Assert(series.GetValueForPointAndColumn(2, "mean", c), Equals, float64(4.5))
	c.Assert(series.GetValueForPointAndColumn(2, "url", c), Equals, "/register")
}

// fix for #305: https://github.com/influxdb/influxdb/issues/305
func (self *ServerSuite) TestShardIdUniquenessAfterReStart(c *C) {
	server := self.serverProcesses[0]
	t := (time.Now().Unix() + 86400*720) * 1000
	data := fmt.Sprintf(`[{"points": [[2, %d]], "name": "test_shard_id_uniqueness", "columns": ["value", "time"]}]`, t)
	server.Post("/db/test_rep/series?u=paul&p=pass", data, c)

	body := server.Get("/cluster/shards?u=root&p=root", c)
	res := make(map[string]interface{})
	err := json.Unmarshal(body, &res)
	c.Assert(err, IsNil)
	shardIds := make(map[float64]bool)
	for _, s := range res["shortTerm"].([]interface{}) {
		sh := s.(map[string]interface{})
		shardId := sh["id"].(float64)
		hasId := shardIds[shardId]
		c.Assert(hasId, Equals, false)
		shardIds[shardId] = true
	}
	c.Assert(len(shardIds) > 0, Equals, true)

	resp := self.serverProcesses[0].Post("/raft/force_compaction?u=root&p=root", "", c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	for _, s := range self.serverProcesses {
		s.Stop()
	}

	for _, s := range self.serverProcesses {
		s.Start()
	}

	for _, s := range self.serverProcesses {
		s.WaitForServerToStart()
	}

	server = self.serverProcesses[0]
	t = (time.Now().Unix() + 86400*720*2) * 1000
	data = fmt.Sprintf(`[{"points": [[2, %d]], "name": "test_shard_id_uniqueness", "columns": ["value", "time"]}]`, t)
	server.Post("/db/test_rep/series?u=paul&p=pass", data, c)

	body = server.Get("/cluster/shards?u=root&p=root", c)
	res = make(map[string]interface{})
	err = json.Unmarshal(body, &res)
	c.Assert(err, IsNil)
	shardIds = make(map[float64]bool)
	for _, s := range res["shortTerm"].([]interface{}) {
		sh := s.(map[string]interface{})
		shardId := sh["id"].(float64)
		hasId := shardIds[shardId]
		c.Assert(hasId, Equals, false)
		shardIds[shardId] = true
	}
	c.Assert(len(shardIds) > 0, Equals, true)
}
