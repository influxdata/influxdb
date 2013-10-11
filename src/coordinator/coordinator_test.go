package coordinator

import (
	"fmt"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type CoordinatorSuite struct{}

var _ = Suite(&CoordinatorSuite{})
var nextPortNum int
var nextDirNum int

const (
	MAX_RING_LOCATIONS  = 10
	SERVER_STARTUP_TIME = time.Millisecond * 300
	REPLICATION_LAG     = time.Millisecond * 200
)

func nextPort() int {
	nextPortNum += 1
	// this is a hack for OSX boxes running spotify. It binds to 127.0.0.1:8099. net.Listen doesn't return an
	// error when listening to :8099. ugh.
	if 8090+nextPortNum == 8099 {
		nextPortNum += 1
	}
	return 8090 + nextPortNum
}

func nextDir() string {
	nextDirNum += 1
	return fmt.Sprintf("test_%d", nextDirNum)
}

func startAndVerifyCluster(count int, c *C) []*RaftServer {
	nextPortNum = 0
	servers := make([]*RaftServer, count, count)
	errs := make([]error, count, count)
	for i := 0; i < count; i++ {
		path := nextDir()
		port := nextPort()
		_, server := newConfigAndServer(path, port)
		servers[i] = server

		var err error
		if i == 0 {
			go func() {
				err = server.ListenAndServe([]string{}, false)
			}()
		} else {
			go func() {
				err = server.ListenAndServe([]string{fmt.Sprintf("http://localhost:%d", servers[0].port)}, true)
			}()
		}
		errs[i] = err
	}
	time.Sleep(SERVER_STARTUP_TIME)
	for _, err := range errs {
		c.Assert(err, Equals, nil)
	}
	return servers
}

func clean(servers []*RaftServer) {
	for _, server := range servers {
		server.Close()
	}
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	clearPath("")
}

func newConfigAndServer(path string, port int) (*ClusterConfiguration, *RaftServer) {
	fullPath := path
	if !strings.HasPrefix(fullPath, "/tmp") {
		fullPath = "/tmp/chronos_coordinator_test/" + path
	}
	os.MkdirAll(fullPath, 0744)
	config := NewClusterConfiguration(MAX_RING_LOCATIONS)
	server := NewRaftServer(fullPath, "localhost", port, config)
	return config, server
}

func clearPath(path string) {
	os.RemoveAll("/tmp/chronos_coordinator_test/" + path)
}

func assertConfigContains(port int, contains string, isContained bool, c *C) {
	host := fmt.Sprintf("localhost:%d", port)
	resp, err1 := http.Get("http://" + host + "/cluster_config")
	c.Assert(err1, Equals, nil)
	defer resp.Body.Close()
	body, err2 := ioutil.ReadAll(resp.Body)
	c.Assert(err2, Equals, nil)
	c.Assert(strings.Contains(string(body), contains), Equals, isContained)
}

func (self *CoordinatorSuite) TestCanCreateCoordinatorWithNoSeed(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir := nextDir()
	port := nextPort()
	_, server := newConfigAndServer(logDir, port)
	defer server.Close()
	defer clearPath(logDir)
	var err error
	go func() {
		err = server.ListenAndServe([]string{}, false)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
}

func (self *CoordinatorSuite) TestCanCreateCoordinatorWithSeedThatIsNotRunning(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir := nextDir()
	port := nextPort()
	_, server := newConfigAndServer(logDir, port)
	defer clearPath(logDir)
	var err error
	go func() {
		err = server.ListenAndServe([]string{"localhost:8079"}, false)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
	server.Close()
}

func (self *CoordinatorSuite) TestCanRecoverCoordinatorWithData(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir := nextDir()
	port := nextPort()
	_, server := newConfigAndServer(logDir, port)
	defer clearPath(logDir)
	var err error
	go func() {
		err = server.ListenAndServe([]string{}, false)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
	server.AddReadApiKey("db", "key1")

	assertConfigContains(port, "key1", true, c)

	server.Close()
	time.Sleep(SERVER_STARTUP_TIME)

	_, server = newConfigAndServer(logDir, port)
	defer server.Close()
	err = nil
	go func() {
		err = server.ListenAndServe([]string{}, false)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
	assertConfigContains(port, "key1", true, c)
}

func (self *CoordinatorSuite) TestCanCreateCoordinatorsAndReplicate(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir1 := nextDir()
	port1 := nextPort()
	logDir2 := nextDir()
	port2 := nextPort()

	_, server := newConfigAndServer(logDir1, port1)
	defer server.Close()
	defer clearPath(logDir1)

	var err error
	go func() {
		err = server.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port2)}, false)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	_, server2 := newConfigAndServer(logDir2, port2)
	defer server2.Close()
	defer clearPath(logDir2)

	var err2 error
	go func() {
		err2 = server2.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port1)}, true)
	}()

	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err2, Equals, nil)
	c.Assert(err, Equals, nil)

	server.AddReadApiKey("db", "key1")
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(port1, "key1", true, c)
	assertConfigContains(port2, "key1", true, c)
}

func (self *CoordinatorSuite) TestDoWriteOperationsFromNonLeaderServer(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir1 := nextDir()
	port1 := nextPort()
	logDir2 := nextDir()
	port2 := nextPort()

	_, server := newConfigAndServer(logDir1, port1)
	defer server.Close()
	defer clearPath(logDir1)

	var err error
	go func() {
		err = server.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port2)}, false)
	}()

	_, server2 := newConfigAndServer(logDir2, port2)
	defer server2.Close()
	defer clearPath(logDir2)

	var err2 error
	go func() {
		err2 = server2.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port1)}, true)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
	c.Assert(err2, Equals, nil)
	err = server2.AddReadApiKey("db", "key1")
	c.Assert(err, Equals, nil)
	err = server2.AddWriteApiKey("db", "key2")
	c.Assert(err, Equals, nil)
	err = server2.AddServerToLocation("somehost", int64(-1))
	c.Assert(err, Equals, nil)
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(port1, "key1", true, c)
	assertConfigContains(port2, "key1", true, c)
	assertConfigContains(port1, "key2", true, c)
	assertConfigContains(port2, "key2", true, c)
	assertConfigContains(port1, "somehost", true, c)
	assertConfigContains(port2, "somehost", true, c)

	err = server2.RemoveApiKey("db", "key2")
	c.Assert(err, Equals, nil)
	err = server2.RemoveServerFromLocation("somehost", int64(-1))
	c.Assert(err, Equals, nil)
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(port2, "key2", false, c)
	assertConfigContains(port1, "somehost", false, c)
}

func (self *CoordinatorSuite) TestNewServerJoiningClusterWillPickUpData(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir := nextDir()
	port := nextPort()
	logDir2 := nextDir()
	port2 := nextPort()
	defer clearPath(logDir)
	defer clearPath(logDir2)

	_, server := newConfigAndServer(logDir, port)
	var err error
	go func() {
		err = server.ListenAndServe([]string{}, false)
	}()
	defer server.Close()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
	server.AddReadApiKey("db", "key1")

	assertConfigContains(port, "key1", true, c)

	_, server2 := newConfigAndServer(logDir2, port2)
	go func() {
		err = server2.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port)}, true)
	}()
	defer server2.Close()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
	assertConfigContains(port2, "key1", true, c)
}

func (self *CoordinatorSuite) TestCanElectNewLeaderAndRecover(c *C) {
	servers := startAndVerifyCluster(3, c)
	defer clean(servers)

	err := servers[0].AddReadApiKey("db", "key1")
	c.Assert(err, Equals, nil)
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(servers[0].port, "key1", true, c)
	assertConfigContains(servers[1].port, "key1", true, c)
	assertConfigContains(servers[2].port, "key1", true, c)

	leader, _ := servers[1].leaderConnectString()
	c.Assert(leader, Equals, fmt.Sprintf("http://localhost:%d", servers[0].port))

	// kill the leader
	servers[0].Close()
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	err = nil
	time.Sleep(SERVER_STARTUP_TIME)
	leader, _ = servers[1].leaderConnectString()
	c.Assert(leader, Not(Equals), fmt.Sprintf("http://localhost:%d", servers[0].port))
	err = servers[1].AddReadApiKey("db", "key2")
	c.Assert(err, Equals, nil)
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(servers[1].port, "key2", true, c)
	assertConfigContains(servers[2].port, "key2", true, c)

	_, server := newConfigAndServer(servers[0].path, servers[0].port)
	defer server.Close()
	err = nil
	go func() {
		err = server.ListenAndServe([]string{fmt.Sprintf("localhost:%d", servers[1].port), fmt.Sprintf("localhost:%d", servers[2].port)}, false)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)

	c.Assert(server.clusterConfig.ReadApiKeys["dbkey1"], Equals, true)
	c.Assert(server.clusterConfig.ReadApiKeys["dbkey2"], Equals, true)
	assertConfigContains(server.port, "key1", true, c)
	assertConfigContains(server.port, "key2", true, c)

	err = server.AddReadApiKey("blah", "sdf")
	c.Assert(err, Equals, nil)
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(servers[0].port, "sdf", true, c)
	assertConfigContains(servers[1].port, "sdf", true, c)
	assertConfigContains(servers[2].port, "sdf", true, c)
}

func (self *CoordinatorSuite) TestCanJoinAClusterWhenNotInitiallyPointedAtLeader(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir1 := nextDir()
	port1 := nextPort()
	logDir2 := nextDir()
	port2 := nextPort()
	logDir3 := nextDir()
	port3 := nextPort()
	defer clearPath(logDir1)
	defer clearPath(logDir2)
	defer clearPath(logDir3)

	_, server := newConfigAndServer(logDir1, port1)

	var err error
	go func() {
		err = server.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port2)}, false)
	}()

	_, server2 := newConfigAndServer(logDir2, port2)
	defer server2.Close()

	var err2 error
	go func() {
		err2 = server2.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port1), fmt.Sprintf("localhost:%d", port3)}, true)
	}()

	_, server3 := newConfigAndServer(logDir3, port3)
	defer server3.Close()

	var err3 error
	go func() {
		err3 = server3.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port2)}, true)
	}()
	time.Sleep(SERVER_STARTUP_TIME)

	c.Assert(err, Equals, nil)
	c.Assert(err2, Equals, nil)
	c.Assert(err3, Equals, nil)
	leader, _ := server2.leaderConnectString()
	c.Assert(leader, Equals, fmt.Sprintf("http://localhost:%d", port1))

	err = server.AddReadApiKey("db", "key1")
	c.Assert(err, Equals, nil)
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(port1, "key1", true, c)
	assertConfigContains(port2, "key1", true, c)
	assertConfigContains(port3, "key1", true, c)
}

func (self *CoordinatorSuite) TestCanCreateDatabase(c *C) {
	servers := startAndVerifyCluster(3, c)
	defer clean(servers)
	id, _ := servers[0].GetNextDatabaseId()
	c.Assert(id, Equals, "1")
	time.Sleep(REPLICATION_LAG)
	c.Assert(id, Equals, servers[1].clusterConfig.CurrentDatabaseId())
	c.Assert(id, Equals, servers[2].clusterConfig.CurrentDatabaseId())
	id2, _ := servers[1].GetNextDatabaseId()
	id3, _ := servers[2].GetNextDatabaseId()
	id4, _ := servers[0].GetNextDatabaseId()
	c.Assert(id2, Equals, "2")
	c.Assert(id3, Equals, "3")
	c.Assert(id4, Equals, "4")
	time.Sleep(REPLICATION_LAG)
	c.Assert(id4, Equals, servers[1].clusterConfig.CurrentDatabaseId())
	c.Assert(id4, Equals, servers[2].clusterConfig.CurrentDatabaseId())
}

func (self *CoordinatorSuite) TestDistributesRingLocationsToNewServer(c *C) {
}
