package integration

import (
	h "api/http"
	"bytes"
	"checkers"
	"crypto/tls"
	"encoding/json"
	"engine"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	influxdb "github.com/influxdb/influxdb-go"
	. "launchpad.net/gocheck"
)

const (
	BATCH_SIZE       = 1
	NUMBER_OF_POINTS = 1000000
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

var benchmark = flag.Bool("benchmark", false, "Run the benchmarks")
var batchSize = flag.Int("batch-size", BATCH_SIZE, "The batch size per write")

type IntegrationSuite struct {
	server *Server
}

var _ = Suite(&IntegrationSuite{})

type Server struct {
	p          *os.Process
	configFile string
}

func (self *Server) WriteData(data interface{}, c *C, precision ...influxdb.TimePrecision) {
	client, err := influxdb.NewClient(&influxdb.ClientConfig{
		Username: "user",
		Password: "pass",
		Database: "db1",
	})
	c.Assert(err, IsNil)
	var series []*influxdb.Series
	switch x := data.(type) {
	case string:
		c.Assert(json.Unmarshal([]byte(x), &series), IsNil)
	case []*influxdb.Series:
		series = x
	default:
		c.Fatalf("Unknown type: %T", x)
	}

	if len(precision) == 0 {
		err = client.WriteSeries(series)
	} else {
		err = client.WriteSeriesWithTimePrecision(series, precision[0])
	}
	c.Assert(err, IsNil)
	time.Sleep(time.Second)
}

func (self *Server) RunQuery(query string, precision influxdb.TimePrecision) ([]*influxdb.Series, error) {
	return self.RunQueryAsUser(query, precision, "user", "pass")
}

func (self *Server) RunQueryAsRoot(query string, precision influxdb.TimePrecision) ([]*influxdb.Series, error) {
	return self.RunQueryAsUser(query, precision, "root", "root")
}

func (self *Server) RunQueryAsUser(query string, precision influxdb.TimePrecision, username, password string) ([]*influxdb.Series, error) {
	client, err := influxdb.NewClient(&influxdb.ClientConfig{
		Username: username,
		Password: password,
		Database: "db1",
	})
	if err != nil {
		return nil, err
	}

	return client.Query(query, precision)
}

func (self *Server) start() error {
	if self.p != nil {
		return fmt.Errorf("Server is already running with pid %d", self.p.Pid)
	}

	fmt.Printf("Starting server")
	dir, err := os.Getwd()
	if err != nil {
		return err
	}

	root := filepath.Join(dir, "..", "..")
	filename := filepath.Join(root, "daemon")
	if self.configFile == "" {
		self.configFile = "config.toml.sample"
	}
	p, err := os.StartProcess(filename, []string{filename, "-config", self.configFile}, &os.ProcAttr{
		Dir:   root,
		Env:   os.Environ(),
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	})
	if err != nil {
		return err
	}
	self.p = p
	time.Sleep(6 * time.Second)
	return nil
}

func (self *Server) stop() {
	if self.p == nil {
		return
	}

	self.p.Signal(syscall.SIGTERM)
	self.p.Wait()
	self.p = nil
}

func (self *IntegrationSuite) createUser(c *C) {
	client, err := influxdb.NewClient(&influxdb.ClientConfig{})
	c.Assert(err, IsNil)
	c.Assert(client.CreateDatabaseUser("db1", "user", "pass"), IsNil)
	c.Assert(client.AlterDatabasePrivilege("db1", "user", true), IsNil)
}

var noCleanData = flag.Bool("no-clean-data", false, "Clean data before running the benchmark tests")
var wroteData = true

func (self *IntegrationSuite) SetUpSuite(c *C) {

	if !*noCleanData {
		wroteData = false
		err := os.RemoveAll("/tmp/influxdb")
		c.Assert(err, IsNil)
	}

	self.server = &Server{}
	err := self.server.start()
	c.Assert(err, IsNil)

	if !*noCleanData {
		self.createUser(c)
	}
}

func (self *IntegrationSuite) TearDownSuite(c *C) {
	self.server.stop()
}

func (self *IntegrationSuite) createPoints(name string, numOfColumns, numOfPoints int) []*influxdb.Series {
	return self.createPointsFromFunc(name, numOfColumns, numOfPoints, func(int) float64 { return rand.Float64() })
}

func (self *IntegrationSuite) createPointsFromFunc(name string, numOfColumns, numOfPoints int, f func(int) float64) []*influxdb.Series {
	series := &influxdb.Series{}

	series.Name = name
	for i := 0; i < numOfColumns; i++ {
		series.Columns = append(series.Columns, fmt.Sprintf("column%d", i))
	}

	for i := 0; i < numOfPoints; i++ {
		point := []interface{}{}
		for j := 0; j < numOfColumns; j++ {
			point = append(point, f(i))
		}
		series.Points = append(series.Points, point)
	}

	return []*influxdb.Series{series}
}

func (self *IntegrationSuite) writeData(c *C) {
	if wroteData {
		return
	}

	for _, numberOfColumns := range []int{1, 5, 10} {
		startTime := time.Now()
		seriesName := fmt.Sprintf("foo%d", numberOfColumns)

		bSize := *batchSize
		for batch := 0; batch < NUMBER_OF_POINTS/bSize; batch++ {
			self.server.WriteData(self.createPoints(seriesName, numberOfColumns, bSize), c)
			if (batch*bSize+bSize)%1000 == 0 {
				fmt.Print(".")
			}
		}
		fmt.Println()
		fmt.Printf("Writing %d points (containing %d columns) in %d batches took %s\n", NUMBER_OF_POINTS, numberOfColumns, BATCH_SIZE,
			time.Now().Sub(startTime))
	}
	wroteData = true
}

func (self *IntegrationSuite) TestSslOnly(c *C) {
	self.server.stop()
	time.Sleep(time.Second)
	self.server.configFile = "src/integration/test_ssl_only.toml"
	self.server.start()

	defer func() {
		self.server.stop()
		time.Sleep(time.Second)
		self.server.configFile = ""
		self.server.start()
		time.Sleep(10 * time.Second)
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

func (self *IntegrationSuite) TestWriting(c *C) {
	if !*benchmark {
		c.Skip("Benchmarks are disabled")
	}

	self.writeData(c)
}

// Reported by Alex in the following thread
// https://groups.google.com/forum/#!msg/influxdb/I_Ns6xYiMOc/XilTv6BDgHgJ
func (self *IntegrationSuite) TestAdminPermissionToDeleteData(c *C) {
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
	series, err := self.server.RunQueryAsRoot("select count(val_1) from test_delete_admin_permission", "s")
	c.Assert(err, IsNil)
	c.Assert(series[0].Points, HasLen, 1)
	c.Assert(series[0].Points[0][1], Equals, float64(1))

	fmt.Println("TESTAD deleting")
	_, err = self.server.RunQueryAsRoot("delete from test_delete_admin_permission", "s")
	c.Assert(err, IsNil)
	fmt.Println("TESTAD query")
	series, err = self.server.RunQueryAsRoot("select count(val_1) from test_delete_admin_permission", "s")
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 0)
}

func (self *IntegrationSuite) TestShortPasswords(c *C) {
	client, err := influxdb.NewClient(&influxdb.ClientConfig{})
	c.Assert(err, IsNil)
	c.Assert(client.CreateDatabaseUser("shahid", "shahid", "1"), Not(ErrorMatches), ".*blowfish.*")

	// should be able to recreate the user
	c.Assert(client.CreateDatabaseUser("shahid", "shahid", "shahid"), IsNil)

	c.Assert(client.AuthenticateDatabaseUser("shahid", "shahid", "shahid"), IsNil)
}

// issue #389
func (self *IntegrationSuite) TestFilteringShouldNotStopIfAllPointsDontMatch(c *C) {
	// make sure we exceed the pointBatchSize, so we force a yield to
	// the filtering engine
	numberOfPoints := 1000000
	serieses := self.createPointsFromFunc("test_filtering_shouldnt_stop", 1, numberOfPoints, func(i int) float64 { return float64(numberOfPoints - i) })
	series := serieses[0]
	series.Columns = append(series.Columns, "time")
	points := series.Points
	series.Points = nil
	now := time.Now()
	for idx, point := range points {
		point = append(point, float64(now.Add(time.Duration(-idx)*time.Second).Unix()))
		series.Points = append(series.Points, point)
	}
	self.server.WriteData([]*influxdb.Series{series}, c, influxdb.Second)
	time.Sleep(5 * time.Second)
	serieses, err := self.server.RunQuery("select column0 from test_filtering_shouldnt_stop where column0 < 10", "m")
	c.Assert(err, IsNil)
	c.Assert(serieses, HasLen, 1)
}

func (self *IntegrationSuite) TestExplainsWithPassthrough(c *C) {
	data := `
  [{
    "points": [
        ["val1", 2],
        ["val1", 3]
    ],
    "name": "test_explain_passthrough",
    "columns": ["val_1", "val_2"]
  }]`
	self.server.WriteData(data, c)
	series, err := self.server.RunQuery("explain select val_1 from test_explain_passthrough where time > now() - 1h", "m")
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "explain query")
	c.Assert(series[0].Columns, HasLen, 7) // 6 columns plus the time column
	c.Assert(series[0].Points, HasLen, 1)
	c.Assert(series[0].Points[0][1], Equals, "QueryEngine")
	c.Assert(series[0].Points[0][5], Equals, float64(2.0))
	c.Assert(series[0].Points[0][6], Equals, float64(2.0))
}

// issue #378
func (self *IntegrationSuite) TestDeletingNewDatabase(c *C) {
	client, err := influxdb.NewClient(&influxdb.ClientConfig{})
	c.Assert(err, IsNil)
	c.Assert(client.CreateDatabase("delete0"), IsNil)

	s := self.createPoints("data_resurrection", 1, 10)
	self.server.WriteData(s, c)
	time.Sleep(time.Second)
	fmt.Printf("wrote some data\n")

	for i := 0; i < 2; i++ {
		c.Assert(client.CreateDatabase("delete1"), IsNil)
		c.Assert(client.DeleteDatabase("delete1"), IsNil)
		c.Assert(client.Ping(), IsNil)
	}
}

// issue #342, #371
func (self *IntegrationSuite) TestDataResurrectionAfterRestart(c *C) {
	s := self.createPoints("data_resurrection", 1, 10)
	self.server.WriteData(s, c)
	time.Sleep(time.Second)
	fmt.Printf("wrote some data\n")
	series, err := self.server.RunQuery("select count(column0) from data_resurrection", "s")
	c.Assert(err, IsNil)
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
	self.server.stop()
	time.Sleep(time.Second)
	c.Assert(self.server.start(), IsNil)
	time.Sleep(5 * time.Second)
	series, err = self.server.RunQuery("select count(column0) from data_resurrection", "s")
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 0)
	series, err = self.server.RunQuery("list series", "s")
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 0)
}

// issue #360
func (self *IntegrationSuite) TestContinuousQueriesAfterCompaction(c *C) {
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

	self.server.stop()
	time.Sleep(time.Second)
	c.Assert(self.server.start(), IsNil)
	time.Sleep(5 * time.Second)

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

func (self *IntegrationSuite) TestExplainsWithPassthroughAndLimit(c *C) {
	points := []string{}
	for i := 0; i < 101; i++ {
		points = append(points, fmt.Sprintf(`["val1", %d]`, i))
	}

	data := fmt.Sprintf(`
  [{
    "points": [%s],
    "name": "test_explain_passthrough_limit",
    "columns": ["val_1", "val_2"]
  }]`, strings.Join(points, ","))

	self.server.WriteData(data, c)
	series, err := self.server.RunQuery("explain select val_1 from test_explain_passthrough_limit where time > now() - 1h limit 1", "m")
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "explain query")
	c.Assert(series[0].Columns, HasLen, 7) // 6 columns plus the time column
	c.Assert(series[0].Points, HasLen, 1)
	c.Assert(series[0].Points[0][1], Equals, "QueryEngine")

	// we can read at most point-batch-size points, which is set to 100
	// by default
	c.Assert(series[0].Points[0][5], Equals, float64(100.0))
	c.Assert(series[0].Points[0][6], Equals, float64(1.0))
}

func (self *IntegrationSuite) TestExplainsWithNonLocalAggregator(c *C) {
	data := `
  [{
    "points": [
        ["val1", 2],
        ["val1", 3],
        ["val1", 4]
    ],
    "name": "test_explain_non_local",
    "columns": ["val_1", "val_2"]
  }]`
	self.server.WriteData(data, c)
	series, err := self.server.RunQuery("explain select count(val_1) from test_explain_non_local where time > now() - 1h", "m")
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "explain query")
	c.Assert(series[0].Columns, HasLen, 7) // 6 columns plus the time column
	c.Assert(series[0].Points, HasLen, 1)
	c.Assert(series[0].Points[0][1], Equals, "QueryEngine")
	c.Assert(series[0].Points[0][5], Equals, float64(3.0))
	c.Assert(series[0].Points[0][6], Equals, float64(1.0))
}

func (self *IntegrationSuite) TestDistinctWithLimit(c *C) {
	data := self.createPoints("test_count_distinct_limit", 1, 1000)
	self.server.WriteData(data, c)
	series, err := self.server.RunQuery("select distinct(column0) from test_count_distinct_limit limit 10", "m")
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Columns, HasLen, 2) // 6 columns plus the time column
	c.Assert(series[0].Points, HasLen, 10)
}

func (self *IntegrationSuite) TestExplainsWithNonLocalAggregatorAndRegex(c *C) {
	data := `
  [{
    "points": [
        ["val1", 2],
        ["val1", 3],
        ["val1", 4]
    ],
    "name": "test_explain_non_local_regex",
    "columns": ["val_1", "val_2"]
  }]`
	self.server.WriteData(data, c)
	series, err := self.server.RunQuery("explain select count(val_1) from /.*test_explain_non_local_regex.*/ where time > now() - 1h", "m")
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "explain query")
	c.Assert(series[0].Columns, HasLen, 7) // 6 columns plus the time column
	c.Assert(series[0].Points, HasLen, 1)
	c.Assert(series[0].Points[0][1], Equals, "QueryEngine")
	c.Assert(series[0].Points[0][5], Equals, float64(3.0))
	c.Assert(series[0].Points[0][6], Equals, float64(1.0))
}

func (self *IntegrationSuite) TestExplainsWithLocalAggregator(c *C) {
	data := `
  [{
    "points": [
        ["val1", 2],
        ["val1", 3],
        ["val1", 4]
    ],
    "name": "test_local_aggregator",
    "columns": ["val_1", "val_2"]
  }]`
	self.server.WriteData(data, c)
	series, err := self.server.RunQuery("explain select count(val_1) from test_local_aggregator group by time(1h) where time > now() - 1h", "m")
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "explain query")
	c.Assert(series[0].Columns, HasLen, 7) // 6 columns plus the time column
	c.Assert(series[0].Points, HasLen, 1)
	c.Assert(series[0].Points[0][1], Equals, "QueryEngine")
	c.Assert(series[0].Points[0][5], Equals, float64(3.0))
	c.Assert(series[0].Points[0][6], Equals, float64(1.0))
}

func (self *IntegrationSuite) TestExplainsWithLocalAggregatorAndRegex(c *C) {
	data := `
[
  {
    "points": [
        ["val1", 2],
        ["val1", 3],
        ["val1", 4]
    ],
    "name": "test_local_aggregator_regex_1",
    "columns": ["val_1", "val_2"]
  },
  {
    "points": [
        ["val1", 2],
        ["val1", 3],
        ["val1", 4]
    ],
    "name": "test_local_aggregator_regex_2",
    "columns": ["val_1", "val_2"]
  }
]`
	self.server.WriteData(data, c)
	series, err := self.server.RunQuery("explain select count(val_1) from /.*test_local_aggregator_regex.*/ group by time(1h) where time > now() - 1h", "m")
	c.Assert(err, IsNil)
	c.Assert(series, HasLen, 1)
	c.Assert(series[0].Name, Equals, "explain query")
	c.Assert(series[0].Columns, HasLen, 7) // 6 columns plus the time column
	c.Assert(series[0].Points, HasLen, 1)
	c.Assert(series[0].Points[0][1], Equals, "QueryEngine")
	c.Assert(series[0].Points[0][5], Equals, float64(6.0))
	c.Assert(series[0].Points[0][6], Equals, float64(2.0))
}

func (self *IntegrationSuite) TestMedians(c *C) {
	for i := 0; i < 3; i++ {
		self.server.WriteData(fmt.Sprintf(`
[
  {
     "name": "test_medians",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	data, err := self.server.RunQuery("select median(cpu) from test_medians group by host;", "m")
	c.Assert(err, IsNil)
	c.Assert(data[0].Name, Equals, "test_medians")
	c.Assert(data[0].Columns, HasLen, 3)
	c.Assert(data[0].Points, HasLen, 2)
	medians := map[float64]string{}
	for _, point := range data[0].Points {
		medians[point[1].(float64)] = point[2].(string)
	}
	c.Assert(medians, DeepEquals, map[float64]string{70.0: "hosta", 80.0: "hostb"})
}

func (self *IntegrationSuite) TestDbUserAuthentication(c *C) {
	resp, err := http.Get("http://localhost:8086/db/db1/authenticate?u=root&p=root")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusUnauthorized)

	resp, err = http.Get("http://localhost:8086/db/db2/authenticate?u=root&p=root")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusUnauthorized)
}

func (self *IntegrationSuite) TestSeriesListing(c *C) {
	self.server.WriteData(`
[
  {
     "name": "test_series_listing",
     "columns": ["cpu", "host"],
     "points": [[99.2, "hosta"], [55.6, "hostb"]]
  }
]
`, c)

	data, err := self.server.RunQuery("list series", "m")
	c.Assert(err, IsNil)
	names := map[string]bool{}
	for _, series := range data {
		names[series.Name] = true
	}
	c.Assert(names["test_series_listing"], Equals, true)
}

func (self *IntegrationSuite) TestArithmeticOperations(c *C) {
	queries := map[string][9]float64{
		"select input + output from test_arithmetic_3.0;":       [9]float64{1, 2, 3, 4, 5, 9, 6, 7, 13},
		"select input - output from test_arithmetic_-1.0;":      [9]float64{1, 2, -1, 4, 5, -1, 6, 7, -1},
		"select input * output from test_arithmetic_2.0;":       [9]float64{1, 2, 2, 4, 5, 20, 6, 7, 42},
		"select 1.0 * input / output from test_arithmetic_0.5;": [9]float64{1, 2, 0.5, 4, 5, 0.8, 6, 8, 0.75},
	}

	for query, values := range queries {

		fmt.Printf("Running query %s\n", query)

		for i := 0; i < 3; i++ {

			data := fmt.Sprintf(`
        [
          {
             "name": "test_arithmetic_%.1f",
             "columns": ["input", "output"],
             "points": [[%f, %f]]
          }
        ]
      `, values[2], values[3*i], values[3*i+1])
			self.server.WriteData(data, c)
		}
		data, err := self.server.RunQuery(query, "m")
		c.Assert(err, IsNil)
		c.Assert(data, HasLen, 1)
		c.Assert(data[0].Columns, HasLen, 3)
		c.Assert(data[0].Points, HasLen, 3)
		for i, p := range data[0].Points {
			idx := 2 - i
			c.Assert(p[2], Equals, values[3*idx+2])
		}
	}
}

// issue #34
func (self *IntegrationSuite) TestAscendingQueries(c *C) {
	self.server.WriteData(`
[
  {
     "name": "test_ascending",
     "columns": ["host"],
     "points": [["hosta"]]
  }
]`, c)
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		self.server.WriteData(fmt.Sprintf(`
[
  {
     "name": "test_ascending",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
		time.Sleep(1 * time.Second)
	}
	data, err := self.server.RunQuery("select host, cpu from test_ascending order asc", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Name, Equals, "test_ascending")
	c.Assert(data[0].Columns, HasLen, 4)
	c.Assert(data[0].Points, HasLen, 7)
	for i := 1; i < 7; i++ {
		c.Assert(data[0].Points[i][3], NotNil)
	}
}

// issue #55
func (self *IntegrationSuite) TestFilterWithLimit(c *C) {
	for i := 0; i < 3; i++ {
		self.server.WriteData(fmt.Sprintf(`
[
  {
     "name": "test_ascending",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	data, err := self.server.RunQuery("select host, cpu from test_ascending where host = 'hostb' order asc limit 1", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Name, Equals, "test_ascending")
	c.Assert(data[0].Columns, HasLen, 4)
	c.Assert(data[0].Points, HasLen, 1)
}

// issue #81
func (self *IntegrationSuite) TestFilterWithInClause(c *C) {
	for i := 0; i < 3; i++ {
		self.server.WriteData(fmt.Sprintf(`
[
  {
     "name": "test_in_clause",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	data, err := self.server.RunQuery("select host, cpu from test_in_clause where host in ('hostb') order asc limit 1", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Name, Equals, "test_in_clause")
	c.Assert(data[0].Columns, HasLen, 4)
	c.Assert(data[0].Points, HasLen, 1)
}

// issue #85
// querying a time series shouldn't add non existing columns
func (self *IntegrationSuite) TestIssue85(c *C) {
	for i := 0; i < 3; i++ {
		self.server.WriteData(fmt.Sprintf(`
[
  {
     "name": "test_issue_85",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	_, err := self.server.RunQuery("select new_column from test_issue_85", "m")
	c.Assert(err, IsNil)
	data, err := self.server.RunQuery("select * from test_issue_85", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Columns, HasLen, 4)
}

func toMap(series *influxdb.Series) []map[string]interface{} {
	points := make([]map[string]interface{}, 0, len(series.Points))
	for _, p := range series.Points {
		point := map[string]interface{}{}
		for idx, column := range series.Columns {
			point[column] = p[idx]
		}
		points = append(points, point)
	}
	return points
}

// issue #92
// grouping my multiple columns fails
// Assuming the following sample data
//
// time        | fr      | to       | app        | kb
// -----------------------------------------------------
//  now() - 1hr | home    | office   | ssl        | 10
//  now() - 1hr | home    | office   | ssl        | 20
//  now() - 1hr | home    | internet | http       | 30
//  now() - 1hr | home    | office   | http       | 40
//  now()       | home    | internet | skype      | 50
//  now()       | home    | office   | lotus      | 60
//  now()       | home    | internet | skype      | 70
//
// the query `select sum(kb) from test group by time(1h), to, app`
// will cause an index out of range
func (self *IntegrationSuite) TestIssue92(c *C) {
	hourAgo := time.Now().Add(-1 * time.Hour).Unix()
	now := time.Now().Unix()

	self.server.WriteData(fmt.Sprintf(`
[
  {
     "name": "test_issue_92",
     "columns": ["time", "fr", "to", "app", "kb"],
     "points": [
			 [%d, "home", "office", "ssl", 10],
			 [%d, "home", "office", "ssl", 20],
			 [%d, "home", "internet", "http", 30],
			 [%d, "home", "office", "http", 40],
			 [%d, "home", "internet", "skype", 50],
			 [%d, "home", "office", "lotus", 60],
			 [%d, "home", "internet", "skype", 70]
		 ]
  }
]
`, hourAgo, hourAgo, hourAgo, hourAgo, now, now, now), c, "s")
	data, err := self.server.RunQuery("select sum(kb) from test_issue_92 group by time(1h), to, app", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	points := toMap(data[0])
	// use a map since the order isn't guaranteed
	sumToPointsMap := map[float64][]map[string]interface{}{}
	for _, point := range points {
		sum := point["sum"].(float64)
		sumToPointsMap[sum] = append(sumToPointsMap[sum], point)
	}
	c.Assert(sumToPointsMap[120.0], HasLen, 1)
	c.Assert(sumToPointsMap[120.0][0]["to"], Equals, "internet")
	c.Assert(sumToPointsMap[120.0][0]["app"], Equals, "skype")
	c.Assert(sumToPointsMap[60.0], HasLen, 1)
	c.Assert(sumToPointsMap[40.0], HasLen, 1)
	c.Assert(sumToPointsMap[30.0], HasLen, 2)
}

// issue #89
// Group by combined with where clause doesn't work
//
// a | b | c
// ---------
// x | y | 10
// x | y | 20
// y | z | 30
// x | z | 40
//
// `select sum(c) from test group by b where a = 'x'` should return the following:
//
// time | sum | b
// --------------
// tttt | 30  | y
// tttt | 40  | z
func (self *IntegrationSuite) TestIssue89(c *C) {
	self.server.WriteData(`
[
  {
     "name": "test_issue_89",
     "columns": ["a", "b", "c"],
     "points": [
			 ["x", "y", 10],
			 ["x", "y", 20],
			 ["y", "z", 30],
			 ["x", "z", 40]
		 ]
  }
]`, c)
	data, err := self.server.RunQuery("select sum(c) from test_issue_89 group by b where a = 'x'", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	points := toMap(data[0])
	c.Assert(points, HasLen, 2)
	sums := map[string]float64{}
	for _, p := range points {
		sums[p["b"].(string)] = p["sum"].(float64)
	}
	c.Assert(sums, DeepEquals, map[string]float64{"y": 30.0, "z": 40.0})
}

// issue #306
func (self *IntegrationSuite) TestNegativeTimeInterval(c *C) {
	self.server.WriteData(`
[
  {
     "name": "test_negative_interval",
     "columns": ["cpu", "host", "time"],
     "points": [[60, "hosta", -1], [70, "hostb", -2]]
  }
]
`, c)
	data, err := self.server.RunQuery("select count(cpu) from test_negative_interval", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Name, Equals, "test_negative_interval")
	c.Assert(data[0].Columns, HasLen, 2)
	c.Assert(data[0].Points, HasLen, 1)
	// count should be 3
	c.Assert(data[0].Points[0][1], Equals, 2.0)
}

// issue #306
func (self *IntegrationSuite) TestShardBoundaries(c *C) {
	d := `
[
  {
     "name": "test_end_time_of_shard_is_exclusive",
     "columns": ["cpu", "host", "time"],
     "points": [[60, "hosta", -1], [70, "hostb", 0]]
  }
]
`
	self.server.WriteData(d, c, "s")
	for _, query := range []string{
		"select count(cpu) from test_end_time_of_shard_is_exclusive where time > 0s",
		"select count(cpu) from test_end_time_of_shard_is_exclusive where time < 0s",
	} {
		fmt.Printf("Running query: %s\n", query)
		data, err := self.server.RunQuery(query, "s")
		c.Assert(err, IsNil)
		c.Assert(data, HasLen, 1)
		c.Assert(data[0].Name, Equals, "test_end_time_of_shard_is_exclusive")
		c.Assert(data[0].Columns, HasLen, 2)
		c.Assert(data[0].Points, HasLen, 1)
		c.Assert(data[0].Points[0][1], Equals, 1.0)
	}
}

// see https://groups.google.com/forum/#!searchin/influxdb/crash/influxdb/5d8s_zH_KdM/xo5B8qoeduoJ
func (self *IntegrationSuite) TestCountWithInvalidInterval(c *C) {
	self.server.WriteData(`
[
  {
     "name": "test_count_with_invalid_interval",
     "columns": ["cpu", "host"],
     "points": [[60, "hosta"], [70, "hostb"]]
  }
]
`, c)
	_, err := self.server.RunQuery("select count(cpu) from test_count_with_invalid_interval group by time(5)", "m")
	c.Assert(err, ErrorMatches, ".*invalid argument.*")
}

// make sure aggregation when happen locally at the shard level don't
// get repeated at the coordinator level, otherwise unexpected
// behavior will happen
func (self *IntegrationSuite) TestCountWithGroupByTimeAndLimit(c *C) {
	for i := 0; i < 1; i++ {
		self.server.WriteData(fmt.Sprintf(`
[
  {
     "name": "test_count_with_groupby_and_limit",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	data, err := self.server.RunQuery("select count(cpu) from test_count_with_groupby_and_limit group by time(5m) limit 10", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Name, Equals, "test_count_with_groupby_and_limit")
	c.Assert(data[0].Columns, HasLen, 2)
	c.Assert(data[0].Points, HasLen, 1)
	// count should be 3
	c.Assert(data[0].Points[0][1], Equals, 2.0)
}

func (self *IntegrationSuite) TestCountWithGroupBy(c *C) {
	for i := 0; i < 20; i++ {
		self.server.WriteData(fmt.Sprintf(`
[
  {
     "name": "test_count",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	data, err := self.server.RunQuery("select count(cpu) from test_count group by host limit 10", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Name, Equals, "test_count")
	c.Assert(data[0].Columns, HasLen, 3)
	c.Assert(data[0].Points, HasLen, 2)
	// count should be 3
	c.Assert(data[0].Points[0][1], Equals, 20.0)
	c.Assert(data[0].Points[1][1], Equals, 20.0)
}

func (self *IntegrationSuite) TestCountWithAlias(c *C) {
	for i := 0; i < 5; i++ {
		self.server.WriteData(fmt.Sprintf(`
[
  {
     "name": "test_aliasing",
     "columns": ["cpu", "host"],
     "points": [[%d, "hosta"], [%d, "hostb"]]
  }
]
`, 60+i*10, 70+i*10), c)
	}
	for _, name := range engine.GetRegisteredAggregators() {
		query := fmt.Sprintf("select %s(cpu) as some_alias from test_aliasing", name)
		if name == "percentile" {
			query = "select percentile(cpu, 90) as some_alias from test_aliasing"
		}
		fmt.Printf("query: %s\n", query)
		data, err := self.server.RunQuery(query, "m")
		c.Assert(err, IsNil)
		c.Assert(data, HasLen, 1)
		c.Assert(data[0].Name, Equals, "test_aliasing")
		if name == "histogram" {
			c.Assert(data[0].Columns, DeepEquals, []string{"time", "some_alias_bucket_start", "some_alias_count"})
			continue
		}
		c.Assert(data[0].Columns, DeepEquals, []string{"time", "some_alias"})
	}
}

// test for issue #30
func (self *IntegrationSuite) TestHttpPostWithTime(c *C) {
	now := time.Now().Add(-10 * 24 * time.Hour)
	self.server.WriteData(fmt.Sprintf(`
[
  {
    "name": "test_post_with_time",
    "columns": ["time", "val1", "val2"],
    "points":[[%d, "v1", 2]]
  }
]`, now.Unix()), c, "s")
	data, err := self.server.RunQuery("select * from test_post_with_time where time > now() - 20d", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Name, Equals, "test_post_with_time")
	c.Assert(data[0].Columns, HasLen, 4)
	c.Assert(data[0].Points, HasLen, 1)
	// count should be 3
	values := make(map[string]interface{})
	for idx, value := range data[0].Points[0] {
		values[data[0].Columns[idx]] = value
	}
	c.Assert(values["val1"], Equals, "v1")
	c.Assert(values["val2"], Equals, 2.0)
}

// test limit when getting data from multiple shards
func (self *IntegrationSuite) TestLimitMultipleShards(c *C) {
	self.server.WriteData(`
[
  {
    "name": "test_limit_with_multiple_shards",
    "columns": ["time", "a"],
    "points":[
		  [1393577978000, 1],
		  [1383577978000, 2],
		  [1373577978000, 2],
		  [1363577978000, 2],
		  [1353577978000, 2],
		  [1343577978000, 2],
		  [1333577978000, 2],
		  [1323577978000, 2],
		  [1313577978000, 2]
	  ]
  }
]`, c, "m")
	data, err := self.server.RunQuery("select * from test_limit_with_multiple_shards limit 1", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Points, HasLen, 1)
}

// test for issue #106
func (self *IntegrationSuite) TestIssue106(c *C) {
	self.server.WriteData(`
[
  {
    "name": "test_issue_106",
    "columns": ["time", "a"],
    "points":[
		  [1386262529794, 2]
	  ]
  }
]`, c, "m")
	data, err := self.server.RunQuery("select derivative(a) from test_issue_106", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 0)
}

func (self *IntegrationSuite) TestIssue105(c *C) {
	self.server.WriteData(`
[
  {
    "name": "test_issue_105",
    "columns": ["time", "a", "b"],
    "points":[
		  [1386262529794, 2, 1],
		  [1386262529794, 2, null]
	  ]
  }
]`, c, "m")

	data, err := self.server.RunQuery("select a, b from test_issue_105 where b > 0", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Columns, HasLen, 4)
	c.Assert(data[0].Points, HasLen, 1)
	c.Assert(data[0].Points[0][3], Equals, 1.0)
}

func (self *IntegrationSuite) TestWhereConditionWithExpression(c *C) {
	self.server.WriteData(`
[
  {
    "name": "test_where_expression",
    "columns": ["time", "a", "b"],
    "points":[
		  [1386262529794, 2, 1],
		  [1386262529794, 2, 0]
	  ]
  }
]`, c, "m")
	data, err := self.server.RunQuery("select a, b from test_where_expression where a + b >= 3", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Columns, HasLen, 4)
	c.Assert(data[0].Points, HasLen, 1)
	c.Assert(data[0].Points[0][3], Equals, 1.0)
}

func (self *IntegrationSuite) TestAggregateWithExpression(c *C) {
	self.server.WriteData(`
[
  {
    "name": "test_aggregate_expression",
    "columns": ["time", "a", "b"],
    "points":[
		  [1386262529794, 1, 1],
		  [1386262529794, 2, 2]
	  ]
  }
]`, c, "m")
	data, err := self.server.RunQuery("select mean(a + b) from test_aggregate_expression", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Columns, HasLen, 2)
	c.Assert(data[0].Points, HasLen, 1)
	c.Assert(data[0].Points[0][1], Equals, 3.0)
}

func (self *IntegrationSuite) verifyWrite(series string, value, sequence interface{}, c *C) interface{} {
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

	data, err := self.server.RunQuery("select * from "+series, "m")
	c.Assert(err, IsNil)
	if value == nil {
		c.Assert(data, HasLen, 0)
		return nil
	}
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Columns, HasLen, 3)
	c.Assert(data[0].Points, HasLen, 1)
	p := toMap(data[0])
	c.Assert(p[0]["a"], Equals, value)
	return p[0]["sequence_number"]
	return nil
}

func (self *IntegrationSuite) TestUpdatePoint(c *C) {
	sequence := self.verifyWrite("test_updating_point", 1.0, nil, c)
	self.verifyWrite("test_updating_point", 2.0, sequence, c)
}

func (self *IntegrationSuite) TestDeletePoint(c *C) {
	sequence := self.verifyWrite("test_deleting_point", 1.0, nil, c)
	self.verifyWrite("test_deleting_point", nil, sequence, c)
}

// test for issue #41
func (self *IntegrationSuite) TestDbDelete(c *C) {
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

	data, err := self.server.RunQuery("select val1 from test_deletetions", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)

	c.Assert(client.DeleteDatabase("db1"), IsNil)
	self.createUser(c)

	// this shouldn't return any data
	data, err = self.server.RunQuery("select val1 from test_deletetions", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 0)
}

func (self *IntegrationSuite) TestInvalidDeleteQuery(c *C) {
	self.server.WriteData(`
[
  {
    "name": "test_invalid_delete_query",
    "columns": ["val1", "val2"],
    "points":[["v1", 2]]
  }
]`, c)
	data, err := self.server.RunQuery("select val1 from test_invalid_delete_query", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Points, HasLen, 1)

	_, err = self.server.RunQuery("delete from test_invalid_delete_query where foo = 'bar'", "m")
	c.Assert(err, ErrorMatches, ".*don't reference time.*")

	// this shouldn't return any data
	data, err = self.server.RunQuery("select val1 from test_invalid_delete_query", "m")
	c.Assert(err, IsNil)
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Points, HasLen, 1)
}

// test delete query
func (self *IntegrationSuite) TestDeleteQuery(c *C) {
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
		data, err := self.server.RunQuery("select val1 from test_delete_query", "m")
		c.Assert(err, IsNil)
		c.Assert(data, HasLen, 1)

		_, err = self.server.RunQuery(queryString, "m")
		c.Assert(err, IsNil)

		// this shouldn't return any data
		data, err = self.server.RunQuery("select val1 from test_delete_query", "m")
		c.Assert(err, IsNil)
		c.Assert(data, HasLen, 0)
	}
}

func (self *IntegrationSuite) TestLargeDeletes(c *C) {
	numberOfPoints := 2 * 1024 * 1024
	points := []interface{}{}
	for i := 0; i < numberOfPoints; i++ {
		points = append(points, []interface{}{i})
	}
	pointsString, _ := json.Marshal(points)
	self.server.WriteData(fmt.Sprintf(`
[
  {
    "name": "test_large_deletes",
    "columns": ["val1"],
    "points":%s
  }
]`, string(pointsString)), c)
	time.Sleep(5 * time.Second)
	data, err := self.server.RunQuery("select count(val1) from test_large_deletes", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Points, HasLen, 1)
	c.Assert(data[0].Points[0][1], Equals, float64(numberOfPoints))

	query := "delete from test_large_deletes"
	_, err = self.server.RunQuery(query, "m")
	c.Assert(err, IsNil)

	time.Sleep(5 * time.Second)

	// this shouldn't return any data
	data, err = self.server.RunQuery("select count(val1) from test_large_deletes", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 0)
}

func (self *IntegrationSuite) TestReading(c *C) {
	if !*benchmark {
		c.Skip("Benchmarking is disabled")
	}

	queries := map[string][]int{
		"select column0 from foo1 where column0 > 0.5 and column0 < 0.6;":                                     []int{1, 80000, 120000},
		"select column0 from foo1;":                                                                           []int{1, 1000000 - 1, 1000000 + 1},
		"select column0 from foo5 where column0 > 0.5 and column0 < 0.6 and column1 > 0.5 and column1 < 0.6;": []int{1, 8000, 12000},
		"select column0, column1, column2, column3, column4 from foo5;":                                       []int{5, 1000000 - 1, 1000000 + 1},
	}

	for q, r := range queries {
		self.writeData(c)

		startTime := time.Now()
		data, err := self.server.RunQuery(q, "m")
		c.Assert(err, IsNil)
		elapsedTime := time.Now().Sub(startTime)

		c.Assert(err, IsNil)

		c.Assert(data, HasLen, 1)
		c.Assert(data[0].Columns, HasLen, r[0]+2)                   // time, sequence number and the requested columns
		c.Assert(len(data[0].Points), checkers.InRange, r[1], r[2]) // values between 0.5 and 0.65 should be about 100,000

		fmt.Printf("Took %s to execute %s\n", elapsedTime, q)
	}
}

func (self *IntegrationSuite) TestReadingWhenColumnHasDot(c *C) {
	self.server.WriteData(`
[
  {
     "name": "test_column_names_with_dots",
     "columns": ["first.name", "last.name"],
     "points": [["paul", "dix"], ["john", "shahid"]]
  }
]`, c)

	for name, expected := range map[string]map[string]bool{
		"first.name": map[string]bool{"paul": true, "john": true},
		"last.name":  map[string]bool{"dix": true, "shahid": true},
	} {
		q := fmt.Sprintf("select %s from test_column_names_with_dots", name)

		data, err := self.server.RunQuery(q, "m")
		c.Assert(err, IsNil)
		c.Assert(data, HasLen, 1)
		c.Assert(data[0].Columns, HasLen, 3) // time, sequence number and the requested columns
		c.Assert(data[0].Columns[2], Equals, name)
		names := map[string]bool{}
		for _, p := range data[0].Points {
			names[p[2].(string)] = true
		}
		c.Assert(names, DeepEquals, expected)
	}
}

func (self *IntegrationSuite) TestSinglePointSelect(c *C) {
	self.server.WriteData(`
[
  {
     "name": "test_single_points",
     "columns": ["name", "age"],
     "points": [["paul", 50], ["todd", 33]]
  }
]`, c)

	query := "select * from test_single_points;"
	data, err := self.server.RunQuery(query, "u")
	c.Assert(err, IsNil)
	c.Assert(data[0].Points, HasLen, 2)

	for _, point := range data[0].Points {
		query := fmt.Sprintf("select * from test_single_points where time = %.0f and sequence_number = %0.f;", point[0].(float64), point[1])
		data, err := self.server.RunQuery(query, "u")
		c.Assert(err, IsNil)
		c.Assert(data, HasLen, 1)
		c.Assert(data[0].Points, HasLen, 1)
		c.Assert(data[0].Points[0], HasLen, 4)
		c.Assert(data[0].Points[0][2], Equals, point[2])
		c.Assert(data[0].Points[0][3], Equals, point[3])
	}
}

func (self *IntegrationSuite) TestSinglePointSelectWithNullValues(c *C) {
	self.server.WriteData(`
[
  {
     "name": "test_single_points_with_nulls",
     "columns": ["name", "age"],
     "points": [["paul", 50],["john", null]]
  }
]`, c)

	query := "select * from test_single_points_with_nulls where name='john';"
	data, err := self.server.RunQuery(query, "u")
	c.Assert(err, IsNil)
	c.Assert(data[0].Points, HasLen, 1)

	for _, point := range data[0].Points {
		query := fmt.Sprintf("select * from test_single_points_with_nulls where time = %.0f and sequence_number = %0.f;", point[0].(float64), point[1])
		data, err := self.server.RunQuery(query, "u")
		c.Assert(err, IsNil)
		c.Assert(data, HasLen, 1)
		c.Assert(data[0].Points, HasLen, 1)
		c.Assert(data[0].Points[0], HasLen, 3)
		c.Assert(data[0].Points[0][2], Equals, point[3])
	}
}

func (self *IntegrationSuite) TestBooleanColumnsWorkWithWhereQuery(c *C) {
	self.server.WriteData(`
[
  {
    "name": "test_boolean_columns_where",
    "columns": ["a"],
    "points":[[true], [false], [true]]
  }
]`, c)

	data, err := self.server.RunQuery("select count(a) from test_boolean_columns_where where a = true", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Points, HasLen, 1)
	c.Assert(data[0].Points[0][1], Equals, 2.0)
}

func (self *IntegrationSuite) TestColumnsWithOnlySomeValuesWorkWithWhereQuery(c *C) {
	self.server.WriteData(`
[
  {
    "name": "test_missing_column_values",
    "columns": ["a", "b"],
    "points":[["a", "b"]]
  }
]`, c)

	self.server.WriteData(`
[
  {
    "name": "test_missing_column_values",
    "columns": ["a", "b"],
    "points":[["a", "b"]]
  }
]`, c)
	self.server.WriteData(`
[
  {
    "name": "test_missing_column_values",
    "columns": ["a", "b", "c"],
    "points":[["a", "b", "c"]]
  }
]`, c)

	data, err := self.server.RunQuery("select * from test_missing_column_values where c = 'c'", "m")
	c.Assert(err, IsNil)
	c.Assert(data, HasLen, 1)
	c.Assert(data[0].Points, HasLen, 1)
}
