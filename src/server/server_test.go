package server

import (
	"bytes"
	"common"
	"configuration"
	"datastore"
	"fmt"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net"
	"net/http"
	"os"
	"parser"
	"protocol"
	"runtime"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type ServerSuite struct{}

var _ = Suite(&ServerSuite{})

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
}

func getAvailablePorts(count int, c *C) []int {
	listeners := make([]net.Listener, count, count)
	ports := make([]int, count, count)
	for i, _ := range listeners {
		l, err := net.Listen("tcp4", ":0")
		c.Assert(err, IsNil)
		port := l.Addr().(*net.TCPAddr).Port
		fmt.Printf("PORT: %d\n", port)
		ports[i] = port
		listeners[i] = l
	}
	for _, l := range listeners {
		l.Close()
	}
	return ports
}

func getDir(prefix string, c *C) string {
	path, err := ioutil.TempDir(os.TempDir(), prefix)
	c.Assert(err, IsNil)
	return path
}

func startCluster(count int, c *C) []*Server {
	ports := getAvailablePorts(count*4, c)
	seedServerPort := ports[0]
	servers := make([]*Server, count, count)
	for i, _ := range servers {
		var seedServers []string
		if i == 0 {
			seedServers = []string{}
		} else {
			seedServers = []string{fmt.Sprintf("http://localhost:%d", seedServerPort)}
		}

		portOffset := i * 4
		config := &configuration.Configuration{
			RaftServerPort: ports[portOffset],
			AdminHttpPort:  ports[portOffset+1],
			ApiHttpPort:    ports[portOffset+2],
			ProtobufPort:   ports[portOffset+3],
			AdminAssetsDir: "./",
			DataDir:        getDir("influxdb_db", c),
			RaftDir:        getDir("influxdb_raft", c),
			SeedServers:    seedServers,
			Hostname:       "localhost",
		}
		server, err := NewServer(config)
		if err != nil {
			c.Error(err)
		}
		go func() {
			err := server.ListenAndServe()
			if err != nil {
				c.Error(err)
			}
		}()
		time.Sleep(time.Millisecond * 50)
		servers[i] = server
	}
	return servers
}

func (self *ServerSuite) postToServer(server *Server, url, data string, c *C) (*http.Response, error) {
	fullUrl := fmt.Sprintf("http://localhost:%d%s", server.Config.ApiHttpPort, url)
	resp, err := http.Post(fullUrl, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
	return resp, err
}

func executeQuery(user common.User, database, query string, db datastore.Datastore, c *C) []*protocol.Series {
	q, errQ := parser.ParseQuery(query)
	c.Assert(errQ, IsNil)
	resultSeries := []*protocol.Series{}
	yield := func(series *protocol.Series) error {
		// ignore time series which have no data, this includes
		// end of series indicator
		if len(series.Points) > 0 {
			resultSeries = append(resultSeries, series)
		}
		return nil
	}
	err := db.ExecuteQuery(user, database, q, yield)
	c.Assert(err, IsNil)
	return resultSeries
}

func (self *ServerSuite) TestDataReplication(c *C) {
	servers := startCluster(3, c)
	time.Sleep(time.Second * 4)
	for _, s := range servers {
		defer os.RemoveAll(s.Config.DataDir)
		defer os.RemoveAll(s.Config.RaftDir)
	}
	time.Sleep(time.Second)
	err := servers[0].RaftServer.CreateDatabase("test_rep", uint8(2))
	c.Assert(err, IsNil)
	time.Sleep(time.Millisecond * 10)

	// debug, take out
	resp, _ := self.postToServer(servers[0], "/db/test_rep/users?u=root&p=root", `{"username": "paul", "password": "pass"}`, c)

	data := `
  [{
    "points": [
        ["val1", 2]
    ],
    "name": "foo",
    "columns": ["val_1", "val_2"]
  }]`
	resp, _ = self.postToServer(servers[0], "/db/test_rep/series?u=paul&p=pass", data, c)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	time.Sleep(time.Millisecond * 10)

	countWithPoint := 0
	user := &MockUser{}
	for _, server := range servers {
		results := executeQuery(user, "test_rep", "select * from foo;", server.Db, c)
		pointCount := 0
		for _, series := range results {
			if *series.Name == "foo" {
				if len(series.Points) > 0 {
					pointCount += 1
				}
			} else {
				c.Error(fmt.Sprintf("Got a series in the query we didn't expect: %s", *series.Name))
			}
		}
		if pointCount > 1 {
			c.Error("Got too many points for the series from one db")
		} else if pointCount > 0 {
			countWithPoint += 1
		}
	}
	c.Assert(countWithPoint, Equals, 2)
}

func (self *ServerSuite) TestCrossClusterQueries(c *C) {

}
