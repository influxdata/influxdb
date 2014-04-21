package integration

import (
	h "api/http"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	. "integration/helpers"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	influxdb "github.com/influxdb/influxdb-go"
	. "launchpad.net/gocheck"
)

const (
	BATCH_SIZE       = 1
	NUMBER_OF_POINTS = 1000000
)

type SingleServerSuite struct {
	server *Server
}

var _ = Suite(&SingleServerSuite{})

func (self *SingleServerSuite) createUser(c *C) {
	client, err := influxdb.NewClient(&influxdb.ClientConfig{})
	c.Assert(err, IsNil)
	c.Assert(client.CreateDatabaseUser("db1", "user", "pass"), IsNil)
	c.Assert(client.AlterDatabasePrivilege("db1", "user", true), IsNil)
}

func (self *SingleServerSuite) SetUpSuite(c *C) {
	self.server = NewServer("config.sample.toml", c)
	self.createUser(c)
}

func (self *SingleServerSuite) TearDownSuite(c *C) {
	self.server.Stop()
}

func (self *SingleServerSuite) TestLargeDeletes(c *C) {
	numberOfPoints := 2 * 1024 * 1024
	data := CreatePoints("test_large_deletes", 1, numberOfPoints)
	self.server.WriteData(data, c)
	data = self.server.RunQuery("select count(column0) from test_large_deletes", "m", c)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Points, HasLen, 1)
	c.Assert(data[0].Points[0][1], Equals, float64(numberOfPoints))

	query := "delete from test_large_deletes"
	_ = self.server.RunQuery(query, "m", c)

	// this shouldn't return any data
	data = self.server.RunQuery("select count(column0) from test_large_deletes", "m", c)
	c.Assert(data, HasLen, 0)
}

func (self *SingleServerSuite) TestDeletesFromCapitalNames(c *C) {
	data := CreatePoints("TestLargeDeletes", 1, 1)
	self.server.WriteData(data, c)
	data = self.server.RunQuery("select count(column0) from TestLargeDeletes", "m", c)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Points, HasLen, 1)
	c.Assert(data[0].Points[0][1], Equals, 1.0)

	query := "delete from TestLargeDeletes"
	_ = self.server.RunQuery(query, "m", c)

	// this shouldn't return any data
	data = self.server.RunQuery("select count(column0) from TestLargeDeletes", "m", c)
	c.Assert(data, HasLen, 0)
}

func (self *SingleServerSuite) TestSslOnly(c *C) {
	self.server.Stop()
	// TODO: don't hard code the path here
	c.Assert(os.RemoveAll("/tmp/influxdb/test/1"), IsNil)
	server := NewSslServer("src/integration/test_ssl_only.toml", c)
	server.WaitForServerToStart()

	defer func() {
		server.Stop()
		self.server = NewServer("config.sample.toml", c)
	}()

	client, err := influxdb.NewClient(&influxdb.ClientConfig{
		IsSecure: true,
		HttpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		},
	})

	c.Assert(err, IsNil)
	c.Assert(client.Ping(), IsNil)

}

// Reported by Alex in the following thread
// https://groups.google.com/forum/#!msg/influxdb/I_Ns6xYiMOc/XilTv6BDgHgJ
func (self *SingleServerSuite) TestAdminPermissionToDeleteData(c *C) {
	data := `
  [{
    "points": [
        ["val1", 2]
    ],
    "name": "test_delete_admin_permission",
    "columns": ["val_1", "val_2"]
  }]`
	fmt.Println("TESTAD writing")
	self.server.WriteData(data, c)
	fmt.Println("TESTAD query root")
	series := self.server.RunQueryAsRoot("select count(val_1) from test_delete_admin_permission", "s", c)
	c.Assert(series[0].Points, HasLen, 1)
	c.Assert(series[0].Points[0][1], Equals, float64(1))

	fmt.Println("TESTAD deleting")
	_ = self.server.RunQueryAsRoot("delete from test_delete_admin_permission", "s", c)
	fmt.Println("TESTAD query")
	series = self.server.RunQueryAsRoot("select count(val_1) from test_delete_admin_permission", "s", c)
	c.Assert(series, HasLen, 0)
}

func (self *SingleServerSuite) TestShortPasswords(c *C) {
	client, err := influxdb.NewClient(&influxdb.ClientConfig{})
	c.Assert(err, IsNil)
	c.Assert(client.CreateDatabaseUser("shahid", "shahid", "1"), Not(ErrorMatches), ".*blowfish.*")

	// should be able to recreate the user
	c.Assert(client.CreateDatabaseUser("shahid", "shahid", "shahid"), IsNil)

	c.Assert(client.AuthenticateDatabaseUser("shahid", "shahid", "shahid"), IsNil)
}

// issue #378
func (self *SingleServerSuite) TestDeletingNewDatabase(c *C) {
	client, err := influxdb.NewClient(&influxdb.ClientConfig{})
	c.Assert(err, IsNil)
	c.Assert(client.CreateDatabase("delete0"), IsNil)

	s := CreatePoints("data_resurrection", 1, 10)
	self.server.WriteData(s, c)
	self.server.WaitForServerToSync()
	fmt.Printf("wrote some data\n")

	for i := 0; i < 2; i++ {
		c.Assert(client.CreateDatabase("delete1"), IsNil)
		c.Assert(client.DeleteDatabase("delete1"), IsNil)
		c.Assert(client.Ping(), IsNil)
	}
}

// issue #432
func (self *SingleServerSuite) TestDataResurrectionAfterRestartWithDeleteQuery(c *C) {
	s := CreatePoints("data_resurrection_with_delete", 1, 10)
	self.server.WriteData(s, c)
	self.server.WaitForServerToSync()
	series := self.server.RunQueryAsRoot("select count(column0) from data_resurrection_with_delete", "s", c)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Points[0][1], Equals, 10.0)
	self.server.RunQueryAsRoot("delete from data_resurrection_with_delete", "s", c)
	series = self.server.RunQueryAsRoot("select count(column0) from data_resurrection_with_delete", "s", c)
	c.Assert(series, HasLen, 0)
	self.server.Stop()
	c.Assert(self.server.Start(), IsNil)
	self.server.WaitForServerToStart()
	series = self.server.RunQueryAsRoot("select count(column0) from data_resurrection_with_delete", "s", c)
	c.Assert(series, HasLen, 0)
}

// issue #342, #371
func (self *SingleServerSuite) TestDataResurrectionAfterRestart(c *C) {
	s := CreatePoints("data_resurrection", 1, 10)
	self.server.WriteData(s, c)
	self.server.WaitForServerToSync()
	fmt.Printf("wrote some data\n")
	series := self.server.RunQuery("select count(column0) from data_resurrection", "s", c)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Points[0][1], Equals, 10.0)
	req, err := http.NewRequest("DELETE", "http://localhost:8086/db/db1?u=root&p=root", nil)
	c.Assert(err, IsNil)
	resp, err := http.DefaultClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNoContent)
	resp, err = http.Post("http://localhost:8086/db?u=root&p=root", "", bytes.NewBufferString("{\"name\":\"db1\", \"replicationFactor\":3}"))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusCreated)
	resp, err = http.Post("http://localhost:8086/db/db1/users?u=root&p=root", "", bytes.NewBufferString("{\"name\":\"user\", \"password\":\"pass\"}"))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	self.server.Stop()
	c.Assert(self.server.Start(), IsNil)
	self.server.WaitForServerToStart()
	series = self.server.RunQuery("select count(column0) from data_resurrection", "s", c)
	c.Assert(series, HasLen, 0)
	series = self.server.RunQuery("list series", "s", c)
	c.Assert(series, HasLen, 0)
}

// issue #360
func (self *SingleServerSuite) TestContinuousQueriesAfterCompaction(c *C) {
	resp, err := http.Post("http://localhost:8086/db/db1/continuous_queries?u=root&p=root", "application/json",
		bytes.NewBufferString(`{"query": "select * from foo into bar"}`))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	resp, err = http.Get("http://localhost:8086/db/db1/continuous_queries?u=root&p=root")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	queries := []*h.ContinuousQuery{}
	err = json.Unmarshal(body, &queries)
	c.Assert(err, IsNil)
	c.Assert(queries, HasLen, 1)

	resp, err = http.Post("http://localhost:8086/raft/force_compaction?u=root&p=root", "", nil)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	self.server.Stop()
	c.Assert(self.server.Start(), IsNil)
	self.server.WaitForServerToStart()

	resp, err = http.Get("http://localhost:8086/db/db1/continuous_queries?u=root&p=root")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	body, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	queries = []*h.ContinuousQuery{}
	err = json.Unmarshal(body, &queries)
	c.Assert(err, IsNil)
	c.Assert(queries, HasLen, 1)
}

func (self *SingleServerSuite) TestDbUserAuthentication(c *C) {
	resp, err := http.Get("http://localhost:8086/db/db1/authenticate?u=root&p=root")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusUnauthorized)

	resp, err = http.Get("http://localhost:8086/db/db2/authenticate?u=root&p=root")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusUnauthorized)
}

// test for issue #30

func (self *SingleServerSuite) verifyWrite(series string, value, sequence interface{}, c *C) interface{} {
	valueString := "null"
	if value != nil {
		valueString = strconv.Itoa(int(value.(float64)))
	}

	columns := `["time", "a"]`
	points := fmt.Sprintf(`[[1386299093602, %s]]`, valueString)
	if sequence != nil {
		columns = `["time", "sequence_number", "a"]`
		points = fmt.Sprintf(`[[1386299093602, %.0f, %s]]`, sequence, valueString)
	}

	payload := fmt.Sprintf(`
[
  {
    "name": "%s",
    "columns": %s,
    "points": %s
  }
]`, series, columns, points)
	self.server.WriteData(payload, c)

	data := self.server.RunQuery("select * from "+series, "m", c)
	if value == nil {
		c.Assert(data, HasLen, 0)
		return nil
	}
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Columns, HasLen, 3)
	c.Assert(data[0].Points, HasLen, 1)
	p := ToMap(data[0])
	c.Assert(p[0]["a"], Equals, value)
	return p[0]["sequence_number"]
	return nil
}

func (self *SingleServerSuite) TestInvalidTimestamp(c *C) {
	value := `[{"name":"test_invalid_timestamp","columns":["time","count"],"points":[[1397727405297,1]]}]`
	resp, err := http.Post("http://localhost:8086/db/db1/series?u=root&p=root&time_precision=s", "", bytes.NewBufferString(value))
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(string(body), Matches, ".*year outside of range.*")
}

// test for issue #41
func (self *SingleServerSuite) TestDbDelete(c *C) {
	client, err := influxdb.NewClient(&influxdb.ClientConfig{})
	c.Assert(err, IsNil)
	self.server.WriteData(`
[
  {
    "name": "test_deletetions",
    "columns": ["val1", "val2"],
    "points":[["v1", 2]]
  }
]`, c, "s")

	data := self.server.RunQuery("select val1 from test_deletetions", "m", c)
	c.Assert(data, HasLen, 1)

	c.Assert(client.DeleteDatabase("db1"), IsNil)

	self.createUser(c)
	// this shouldn't return any data
	data = self.server.RunQuery("select val1 from test_deletetions", "m", c)
	c.Assert(data, HasLen, 0)
}

// test delete query
func (self *SingleServerSuite) TestDeleteQuery(c *C) {
	for _, queryString := range []string{
		"delete from test_delete_query",
		"delete from test_delete_query where time > now() - 1d and time < now()",
		"delete from /.*test_delete_query.*/",
		"delete from /.*TEST_DELETE_QUERY.*/i",
	} {

		fmt.Printf("Running %s\n", queryString)

		self.server.WriteData(`
[
  {
    "name": "test_delete_query",
    "columns": ["val1", "val2"],
    "points":[["v1", 2]]
  }
]`, c)
		data := self.server.RunQuery("select val1 from test_delete_query", "m", c)
		c.Assert(data, HasLen, 1)

		_ = self.server.RunQuery(queryString, "m", c)

		// this shouldn't return any data
		data = self.server.RunQuery("select val1 from test_delete_query", "m", c)
		c.Assert(data, HasLen, 0)
	}
}
