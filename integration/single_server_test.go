package integration

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	influxdb "github.com/influxdb/influxdb/client"
	. "github.com/influxdb/influxdb/integration/helpers"
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
	c.Assert(client.CreateDatabase("db1"), IsNil)
	c.Assert(client.CreateDatabaseUser("db1", "user", "pass"), IsNil)
	c.Assert(client.AlterDatabasePrivilege("db1", "user", true), IsNil)
}

func (self *SingleServerSuite) SetUpSuite(c *C) {
	self.server = NewServer("integration/test_config_single.toml", c)
	self.createUser(c)
}

func (self *SingleServerSuite) TearDownSuite(c *C) {
	self.server.Stop()
}

func (self *SingleServerSuite) TestWritesToNonExistentDb(c *C) {
	client := self.server.GetClient("notexistent", c)
	s := CreatePoints("test", 1, 1)
	c.Assert(client.WriteSeries(s), ErrorMatches, ".*doesn't exist.*")
}

func (self *SingleServerSuite) TestMultiplePoints(c *C) {
	client := self.server.GetClient("test_string_columns", c)
	c.Assert(client.CreateDatabase("test_string_columns"), IsNil)
	err := client.WriteSeries(
		[]*influxdb.Series{
			{
				Name:    "events",
				Columns: []string{"column1", "column2"},
				Points: [][]interface{}{
					{"value1", "value2"},
				},
			},
		},
	)
	c.Assert(err, IsNil)
	s, err := client.Query("select * from events;")
	c.Assert(err, IsNil)
	c.Assert(s, HasLen, 1)
	maps := ToMap(s[0])
	c.Assert(maps, HasLen, 1)
	fmt.Printf("WTF: %#v\n", maps)
	c.Assert(maps[0]["column1"], Equals, "value1")
	c.Assert(maps[0]["column2"], Equals, "value2")
}

func (self *SingleServerSuite) TestAdministrationOperation(c *C) {
	client := self.server.GetClient("", c)
	c.Assert(client.CreateDatabase("test_admin_operations"), IsNil)
	c.Assert(client.CreateDatabaseUser("test_admin_operations", "user", "pass"), IsNil)
	c.Assert(client.AuthenticateDatabaseUser("test_admin_operations", "user", "pass"), IsNil)
	c.Assert(client.ChangeDatabaseUser("test_admin_operations", "user", "pass2", false), IsNil)
	c.Assert(client.AuthenticateDatabaseUser("test_admin_operations", "user", "pass"), NotNil)
	c.Assert(client.AuthenticateDatabaseUser("test_admin_operations", "user", "pass2"), IsNil)
}

// issue #736
func (self *SingleServerSuite) TestDroppingSeries(c *C) {
	client := self.server.GetClient("", c)
	c.Assert(client.CreateDatabase("test_dropping_series"), IsNil)
	c.Assert(client.CreateDatabaseUser("test_dropping_series", "user", "pass"), IsNil)
	user := self.server.GetClientWithUser("test_dropping_series", "user", "pass", c)
	err := user.WriteSeries([]*influxdb.Series{{
		Name:    "foo",
		Columns: []string{"column1"},
		Points:  [][]interface{}{{1}},
	}})
	c.Assert(err, IsNil)
	_, err = user.Query("drop series foo")
	c.Assert(err, NotNil)
	s, err := user.Query("select * from foo")
	c.Assert(err, IsNil)
	c.Assert(s, HasLen, 1)
	maps := ToMap(s[0])
	c.Assert(maps, HasLen, 1)
	c.Assert(maps[0]["column1"], Equals, 1.0)
}

// pr #483
func (self *SingleServerSuite) TestConflictStatusCode(c *C) {
	client := self.server.GetClient("", c)
	c.Assert(client.CreateDatabase("test_conflict"), IsNil)
	c.Assert(client.CreateDatabase("test_conflict"), ErrorMatches, "Server returned \\(409\\).*")
}

// issue #
func (self *SingleServerSuite) TestListSeriesAfterDropSeries(c *C) {
	rootClient := self.server.GetClient("", c)
	c.Assert(rootClient.CreateDatabase("test_drop_series"), IsNil)
	client := self.server.GetClient("test_drop_series", c)
	data := CreatePoints("test_drop_series", 1, 1)
	data[0].Columns = append(data[0].Columns, "time")
	data[0].Points[0] = append(data[0].Points[0], 1382819388)
	c.Assert(client.WriteSeriesWithTimePrecision(data, "s"), IsNil)
	series, err := client.Query("list series")
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "list_series_result")

	hasSeries := false
	for _, p := range series[0].Points {
		if p[1].(string) == "test_drop_series" {
			hasSeries = true
			break
		}
	}
	c.Assert(hasSeries, Equals, true)

	_, err = client.Query("drop series test_drop_series")
	c.Assert(err, IsNil)
	series, err = client.Query("list series")

	hasSeries = false
	for _, p := range series[0].Points {
		if p[1].(string) == "test_drop_series" {
			hasSeries = true
			break
		}
	}
	c.Assert(hasSeries, Equals, false)
}

// issue #497
func (self *SingleServerSuite) TestInvalidPercentile(c *C) {
	client := self.server.GetClient("db1", c)
	series := &influxdb.Series{
		Name:    "test_invalid_percentile",
		Columns: []string{"foo", "bar"},
		Points: [][]interface{}{
			{1.0, 2.0},
		},
	}
	c.Assert(client.WriteSeries([]*influxdb.Series{series}), IsNil)
	_, err := client.Query("select percentile(*,95) from test_invalid_percentile")
	c.Assert(err, ErrorMatches, ".*wildcard.*")
}

// issue #565
func (self *SingleServerSuite) TestInvalidSeriesName(c *C) {
	client := self.server.GetClient("db1", c)
	series := &influxdb.Series{
		Name:    "",
		Columns: []string{"foo", "bar"},
		Points: [][]interface{}{
			{1.0, 2.0},
		},
	}
	c.Assert(client.WriteSeries([]*influxdb.Series{series}), ErrorMatches, ".*\\(400\\).*empty.*")
}

// issue #497
func (self *SingleServerSuite) TestInvalidDataWrite(c *C) {
	client := self.server.GetClient("db1", c)
	series := &influxdb.Series{
		Name:    "test_invalid_data",
		Columns: []string{"foo", "bar"},
		Points: [][]interface{}{
			{1.0},
		},
	}
	c.Assert(client.WriteSeries([]*influxdb.Series{series}), ErrorMatches, ".*\\(400\\).*invalid.*")
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
	c.Assert(os.RemoveAll("/tmp/influxdb/development"), IsNil)
	server := NewSslServer("integration/test_ssl_only.toml", c)
	server.WaitForServerToStart()

	defer func() {
		server.Stop()
		self.server = NewServer("integration/test_config_single.toml", c)
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

func (self *SingleServerSuite) TestSingleServerHostnameChange(c *C) {
	self.server.Stop()
	// TODO: don't hard code the path here
	c.Assert(os.RemoveAll("/tmp/influxdb/development"), IsNil)
	server := NewServerWithArgs("integration/test_config_single.toml", c, "-hostname", "foo")
	server.WaitForServerToStart()
	server.Stop()
	server = NewServerWithArgs("integration/test_config_single.toml", c, "-hostname", "bar")
	server.WaitForServerToStart()

	defer func() {
		server.Stop()
		self.server = NewServer("integration/test_config_single.toml", c)
	}()

	client, err := influxdb.NewClient(&influxdb.ClientConfig{})
	c.Assert(err, IsNil)
	c.Assert(client.Ping(), IsNil)
}

func (self *SingleServerSuite) TestUserWritePermissions(c *C) {
	rootUser := self.server.GetClient("", c)

	verifyPermissions := func(db string, name string, readFrom string, writeTo string) {
		users, _ := rootUser.GetDatabaseUserList(db)
		matched := false
		for _, user := range users {
			if user["name"] == name {
				c.Assert(user["readFrom"], DeepEquals, readFrom)
				c.Assert(user["writeTo"], DeepEquals, writeTo)
				matched = true
			}
		}
		c.Assert(matched, Equals, true)
	}

	// create two users one that can only read and one that can only write. both can access test_should_read
	// series only
	rootUser.CreateDatabase("db1")
	c.Assert(rootUser.CreateDatabaseUser("db1", "limited_user", "pass", "^$", "^$"), IsNil)
	verifyPermissions("db1", "limited_user", "^$", "^$")

	config := &influxdb.ClientConfig{
		Username: "limited_user",
		Password: "pass",
		Database: "db1",
	}
	user, err := influxdb.NewClient(config)
	c.Assert(err, IsNil)

	data := `
[
  {
    "points": [
        [1]
    ],
    "name": "test_should_write",
    "columns": ["value"]
  }
]`
	invalidData := `
[
  {
    "points": [
        [2]
    ],
    "name": "test_should_not_write",
    "columns": ["value"]
  }
]`
	series := []*influxdb.Series{}
	c.Assert(json.Unmarshal([]byte(data), &series), IsNil)
	// readUser shouldn't be able to write
	c.Assert(user.WriteSeries(series), NotNil)
	actualSeries, err := rootUser.Query("select * from test_should_write", "s")
	// if this test ran by itself there will be no shards to query,
	// therefore no error will be returned
	if err != nil {
		c.Assert(err, ErrorMatches, ".*Couldn't look up.*")
	} else {
		c.Assert(actualSeries, HasLen, 0)
	}
	rootUser.ChangeDatabaseUser("db1", "limited_user", "pass", false, "^$", "test_should_write")
	verifyPermissions("db1", "limited_user", "^$", "test_should_write")
	// write the data to test the write permissions
	c.Assert(user.WriteSeries(series), IsNil)
	content := self.server.RunQueryAsRoot("select * from test_should_write", "m", c)
	c.Assert(content, HasLen, 1)
	invalidSeries := []*influxdb.Series{}
	c.Assert(json.Unmarshal([]byte(invalidData), &invalidSeries), IsNil)
	c.Assert(user.WriteSeries(invalidSeries), NotNil)
	self.server.WaitForServerToSync()
	content, err = rootUser.Query("select * from test_should_not_write", "m")
	c.Assert(content, HasLen, 0)
	rootUser.ChangeDatabaseUser("db1", "limited_user", "pass", false, "^$", "test_.*")
	verifyPermissions("db1", "limited_user", "^$", "test_.*")
	c.Assert(user.WriteSeries(invalidSeries), IsNil)
	self.server.WaitForServerToSync()
	content = self.server.RunQueryAsRoot("select * from test_should_not_write", "m", c)
	c.Assert(content, HasLen, 1)
}

func (self *SingleServerSuite) TestUserReadPermissions(c *C) {
	rootUser := self.server.GetClient("", c)

	// this can fail if db1 is already created
	rootUser.CreateDatabase("db1")
	// create two users one that can only read and one that can only write. both can access test_should_read
	// series only
	c.Assert(rootUser.CreateDatabaseUser("db1", "limited_user2", "pass", "test_should_read", "^$"), IsNil)

	data := `
[
  {
    "points": [
        [1]
    ],
    "name": "test_should_read",
    "columns": ["value"]
  },
  {
    "points": [
        [2]
    ],
    "name": "test_should_not_read",
    "columns": ["value"]
  }
]`
	self.server.WriteData(data, c)
	self.server.WaitForServerToSync()

	// test all three cases, read from one series that the user has read access to, one that the user doesn't have
	// access to and a regex
	content := self.server.RunQueryAsUser("select value from test_should_read", "s", "limited_user2", "pass", true, c)
	c.Assert(content, HasLen, 1)
	c.Assert(content[0].Points, HasLen, 1)
	c.Assert(content[0].Points[0][2], Equals, float64(1))
	// the following query should return an error
	_ = self.server.RunQueryAsUser("select value from test_should_not_read", "s", "limited_user2", "pass", false, c)
	series := self.server.RunQueryAsUser("select value from /.*/", "s", "limited_user2", "pass", true, c)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "test_should_read")

	rootUser.UpdateDatabaseUserPermissions("db1", "limited_user2", ".*", ".*")
	self.server.WaitForServerToSync()
	series = self.server.RunQueryAsUser("select value from /.*/", "s", "limited_user2", "pass", true, c)
	c.Assert(series, HasLen, 2)
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
	self.server.WriteData(data, c)
	series := self.server.RunQueryAsRoot("select count(val_1) from test_delete_admin_permission", "s", c)
	c.Assert(series[0].Points, HasLen, 1)
	c.Assert(series[0].Points[0][1], Equals, float64(1))

	_ = self.server.RunQueryAsRoot("delete from test_delete_admin_permission", "s", c)
	series = self.server.RunQueryAsRoot("select count(val_1) from test_delete_admin_permission", "s", c)
	for _, s := range series {
		fmt.Println("** ", s)
	}
	c.Assert(series, HasLen, 0)
}

func (self *SingleServerSuite) TestShortPasswords(c *C) {
	client, err := influxdb.NewClient(&influxdb.ClientConfig{})
	c.Assert(err, IsNil)

	c.Assert(client.CreateDatabase("shahid"), IsNil)
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
	error, _ := self.server.GetErrorBody("db1", "select count(column0) from data_resurrection", "user", "pass", true, c)
	c.Assert(error, Matches, ".*Couldn't look up.*")
	series = self.server.RunQuery("list series", "s", c)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Points, HasLen, 0)
}

// issue https://github.com/influxdb/influxdb/issues/702. Dropping shards can cause server crash
// Two cases here. First is they try to drop the same shard multiple times. Second is that
// they drop a shard and the server gets restarted so the raft log replays and tries to drop it again.
func (self *SingleServerSuite) TestDropingShardBeforeRestart(c *C) {
	s := CreatePoints("data_resurrection", 1, 1)
	self.server.WriteData(s, c)
	self.server.WaitForServerToSync()
	client := self.server.GetClient("", c)
	shards, err := client.GetShards()

	ids := make(map[uint32]bool)
	for _, s := range shards.All {
		hasId := ids[s.Id]
		if hasId {
			c.Error("Shard id shows up twice: ", s.Id)
		}
		ids[s.Id] = true
	}

	oldShardCount := len(shards.All)
	oldShardId := shards.All[0].Id

	client.DropShard(oldShardId, []uint32{uint32(1)})
	shards, err = client.GetShards()
	c.Assert(err, IsNil)
	c.Assert(len(shards.All), Equals, oldShardCount-1)

	// drop again and don't crash
	client.DropShard(oldShardId, []uint32{uint32(1)})
	shards, err = client.GetShards()
	c.Assert(err, IsNil)
	c.Assert(len(shards.All), Equals, oldShardCount-1)

	// now try to restart
	self.server.Stop()
	c.Assert(self.server.Start(), IsNil)
	self.server.WaitForServerToStart()
	shards, err = client.GetShards()
	c.Assert(err, IsNil)
	c.Assert(len(shards.All), Equals, oldShardCount-1)
}

// issue #360
func (self *SingleServerSuite) TestContinuousQueriesAfterCompaction(c *C) {
	defer self.server.RemoveAllContinuousQueries("db1", c)
	resp, err := http.Post("http://localhost:8086/db/db1/continuous_queries?u=root&p=root", "application/json",
		bytes.NewBufferString(`{"query": "select * from foo into bar"}`))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	self.server.AssertContinuousQueryCount("db1", 1, c)

	resp, err = http.Post("http://localhost:8086/raft/force_compaction?u=root&p=root", "", nil)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	self.server.Stop()
	c.Assert(self.server.Start(), IsNil)
	self.server.WaitForServerToStart()

	self.server.AssertContinuousQueryCount("db1", 1, c)
}

// issue #469
func (self *SingleServerSuite) TestContinuousQueriesAfterDroppingDatabase(c *C) {
	defer self.server.RemoveAllContinuousQueries("db2", c)
	self.server.AssertContinuousQueryCount("db2", 0, c)
	client := self.server.GetClient("", c)
	c.Assert(client.CreateDatabase("db2"), IsNil)
	self.server.WaitForServerToSync()
	resp, err := http.Post("http://localhost:8086/db/db2/continuous_queries?u=root&p=root", "application/json",
		bytes.NewBufferString(`{"query": "select * from foo into bar"}`))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	self.server.AssertContinuousQueryCount("db2", 1, c)
	c.Assert(client.DeleteDatabase("db2"), IsNil)
	self.server.AssertContinuousQueryCount("db2", 0, c)
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
	error, _ := self.server.GetErrorBody("db1", "select val1 from test_deletetions", "root", "root", true, c)
	c.Assert(error, Matches, ".*Couldn't look up.*")
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

func (self *SingleServerSuite) TestInvalidDbUserCreation(c *C) {
	client, err := influxdb.NewClient(&influxdb.ClientConfig{})
	c.Assert(err, IsNil)

	c.Assert(client.CreateDatabaseUser("db999", "user", "pass"), NotNil)
}

// fix for #640 https://github.com/influxdb/influxdb/issues/640 - duplicate shards
func (self *SingleServerSuite) TestDuplicateShardsNotCreatedWhenOldShardDropped(c *C) {
	self.server.WriteData(`
[
  {
    "name": "test_duplicate_shards",
    "columns": ["time", "val"],
    "points":[[1307997668000, 1], [1339533664000, 1], [1402605633000, 1], [1371069620000, 1]]
  }
]`, c)
	client := self.server.GetClient("", c)
	shards, err := client.GetShards()
	c.Assert(err, IsNil)
	c.Assert(len(shards.All) > 1, Equals, true)

	ids := make(map[uint32]bool)
	for _, s := range shards.All {
		hasId := ids[s.Id]
		if hasId {
			c.Error("Shard id shows up twice: ", s.Id)
		}
		ids[s.Id] = true
	}

	oldShardCount := len(shards.All)
	client.DropShard(shards.All[0].Id, []uint32{uint32(1)})
	shards, err = client.GetShards()
	c.Assert(err, IsNil)
	c.Assert(len(shards.All), Equals, oldShardCount-1)
	self.server.WriteData(`
[
  {
    "name": "test_duplicate_shards",
    "columns": ["time", "val"],
    "points":[[130723342, 1]]
  }
]`, c)
	shards, err = client.GetShards()
	c.Assert(err, IsNil)
	c.Assert(len(shards.All), Equals, oldShardCount)

	ids = make(map[uint32]bool)
	for _, s := range shards.All {
		hasId := ids[s.Id]
		if hasId {
			c.Error("Shard id shows up twice: ", s.Id)
		}
		ids[s.Id] = true
	}
}

func (self *SingleServerSuite) TestShardSpaceRegex(c *C) {
	client := self.server.GetClient("", c)
	space := &influxdb.ShardSpace{Name: "test_regex", RetentionPolicy: "30d", Database: "db1", Regex: "/^metric\\./"}
	err := client.CreateShardSpace(space)
	c.Assert(err, IsNil)

	self.server.WriteData(`
[
  {
    "name": "metric.foobar",
    "columns": ["time", "val"],
    "points":[[1307997668000, 1]]
  }
]`, c)
	spaces, err := client.GetShardSpaces()
	c.Assert(err, IsNil)
	c.Assert(self.getSpace("db1", "test_regex", "/^metric\\./", spaces), NotNil)
	shards, err := client.GetShards()
	c.Assert(err, IsNil)
	spaceShards := self.getShardsForSpace("test_regex", shards.All)
	c.Assert(spaceShards, HasLen, 1)
}

func (self *SingleServerSuite) TestCreateShardSpace(c *C) {
	// creates a default space
	self.server.WriteData(`
[
  {
    "name": "test_create_shard_space",
    "columns": ["time", "val"],
    "points":[[1307997668000, 1]]
  }
]`, c)
	client := self.server.GetClient("", c)
	spaces, err := client.GetShardSpaces()
	c.Assert(err, IsNil)
	c.Assert(spaces, HasLen, 1)
	c.Assert(spaces[0].Name, Equals, "default")

	space := &influxdb.ShardSpace{Name: "month", RetentionPolicy: "30d", Database: "db1", Regex: "/^the_dude_abides/"}
	err = client.CreateShardSpace(space)
	c.Assert(err, IsNil)

	self.server.WriteData(`
[
  {
    "name": "the_dude_abides",
    "columns": ["time", "val"],
    "points":[[1307997668000, 1]]
  }
]`, c)
	spaces, err = client.GetShardSpaces()
	c.Assert(err, IsNil)
	c.Assert(self.getSpace("db1", "month", "/^the_dude_abides/", spaces), NotNil)
	shards, err := client.GetShards()
	c.Assert(err, IsNil)
	spaceShards := self.getShardsForSpace("month", shards.All)
	c.Assert(spaceShards, HasLen, 1)
}

func (self *SingleServerSuite) getShardsForSpace(name string, shards []*influxdb.Shard) []*influxdb.Shard {
	filteredShards := make([]*influxdb.Shard, 0)
	for _, s := range shards {
		if s.SpaceName == name {
			filteredShards = append(filteredShards, s)
		}
	}
	return filteredShards
}

func (self *SingleServerSuite) getSpace(database, name string, regex string, spaces []*influxdb.ShardSpace) *influxdb.ShardSpace {
	for _, s := range spaces {
		if s.Name == name && s.Database == database && s.Regex == regex {
			return s
		}
	}
	return nil
}

func (self *SingleServerSuite) TestDropShardSpace(c *C) {
	client := self.server.GetClient("", c)
	spaceName := "test_drop"
	space := &influxdb.ShardSpace{Name: spaceName, RetentionPolicy: "30d", Database: "db1", Regex: "/^dont_drop_me_bro/"}
	err := client.CreateShardSpace(space)
	c.Assert(err, IsNil)

	self.server.WriteData(`
[
  {
    "name": "dont_drop_me_bro",
    "columns": ["time", "val"],
    "points":[[1307997668000, 1]]
  }
]`, c)
	spaces, err := client.GetShardSpaces()
	c.Assert(err, IsNil)
	c.Assert(self.getSpace("db1", spaceName, "/^dont_drop_me_bro/", spaces), NotNil)
	err = client.DropShardSpace("db1", spaceName)
	c.Assert(err, IsNil)
	spaces, err = client.GetShardSpaces()
	c.Assert(err, IsNil)
	c.Assert(self.getSpace("db1", spaceName, "/^dont_drop_me_bro/", spaces), IsNil)
}

func (self *SingleServerSuite) TestLoadDatabaseConfig(c *C) {
	contents, err := ioutil.ReadFile("database_conf.json")
	c.Assert(err, IsNil)
	resp, err := http.Post("http://localhost:8086/cluster/database_configs/test_db_conf_db?u=root&p=root", "application/json", bytes.NewBuffer(contents))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusCreated)

	client := self.server.GetClient("test_db_conf_db", c)
	spaces, err := client.GetShardSpaces()
	c.Assert(err, IsNil)
	c.Assert(self.getSpace("test_db_conf_db", "everything", "/.*/", spaces), NotNil)
	space := self.getSpace("test_db_conf_db", "specific", "/^something_specfic/", spaces)
	c.Assert(space, NotNil)
	c.Assert(space.Split, Equals, uint32(3))
	c.Assert(space.ReplicationFactor, Equals, uint32(2))

	series, err := client.Query("list continuous queries;")
	c.Assert(err, IsNil)
	queries := series[0].Points
	c.Assert(queries, HasLen, 2)
	c.Assert(queries[0][2], Equals, "select * from events into events.[id]")
	c.Assert(queries[1][2], Equals, "select count(value) from events group by time(5m) into 5m.count.events")
}
