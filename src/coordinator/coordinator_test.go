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

func nextPort() int {
	nextPortNum += 1
	return 8090 + nextPortNum
}

func nextDir() string {
	nextDirNum += 1
	return fmt.Sprintf("test_%d", nextDirNum)
}

func newConfigAndServer(path string, port int) (*ClusterConfiguration, *RaftServer) {
	fullPath := "/tmp/chronos_coordinator_test/" + path
	os.MkdirAll(fullPath, 0744)
	config := NewClusterConfiguration(2)
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
	body, err2 := ioutil.ReadAll(resp.Body)
	c.Assert(err2, Equals, nil)
	c.Assert(strings.Contains(string(body), contains), Equals, isContained)
}

func (self *CoordinatorSuite) TestCanCreateCoordinatorWithNoSeed(c *C) {
	logDir := nextDir()
	port := nextPort()
	_, server := newConfigAndServer(logDir, port)
	defer server.Close()
	defer clearPath(logDir)
	var err error
	go func() {
		err = server.ListenAndServe([]string{}, false)
	}()
	time.Sleep(time.Second)
	c.Assert(err, Equals, nil)
}

func (self *CoordinatorSuite) TestCanCreateCoordinatorWithSeedThatIsNotRunning(c *C) {
	logDir := nextDir()
	port := nextPort()
	_, server := newConfigAndServer(logDir, port)
	defer clearPath(logDir)
	var err error
	go func() {
		err = server.ListenAndServe([]string{"localhost:8079"}, false)
	}()
	time.Sleep(time.Second)
	c.Assert(err, Equals, nil)
	server.Close()
}

func (self *CoordinatorSuite) TestCanRecoverCoordinatorWithData(c *C) {
	logDir := nextDir()
	port := nextPort()
	_, server := newConfigAndServer(logDir, port)
	defer clearPath(logDir)
	var err error
	go func() {
		err = server.ListenAndServe([]string{}, false)
	}()
	time.Sleep(time.Second)
	c.Assert(err, Equals, nil)
	server.AddReadApiKey("db", "key1")

	assertConfigContains(port, "key1", true, c)

	server.Close()
	time.Sleep(time.Second)

	_, server = newConfigAndServer(logDir, port)
	defer server.Close()
	err = nil
	go func() {
		err = server.ListenAndServe([]string{}, false)
	}()
	time.Sleep(time.Second)
	c.Assert(err, Equals, nil)
	assertConfigContains(port, "key1", true, c)
}

func (self *CoordinatorSuite) TestCanCreateCoordinatorsAndReplicate(c *C) {
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
	time.Sleep(time.Second)
	_, server2 := newConfigAndServer(logDir2, port2)
	defer server2.Close()
	defer clearPath("test6")

	var err2 error
	go func() {
		err2 = server2.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port1)}, true)
	}()

	time.Sleep(time.Second)
	c.Assert(err2, Equals, nil)
	c.Assert(err, Equals, nil)

	server.AddReadApiKey("db", "key1")
	time.Sleep(time.Millisecond * 200)
	assertConfigContains(port1, "key1", true, c)
	assertConfigContains(port2, "key1", true, c)
}

func (self *CoordinatorSuite) TestCanAddKeyFromNonLeaderServer(c *C) {
	logDir1 := nextDir()
	port1 := nextPort()
	logDir2 := nextDir()
	port2 := nextPort()

	_, server := newConfigAndServer(logDir1, port1)
	defer server.Close()
	defer clearPath("test3")

	var err error
	go func() {
		err = server.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port2)}, false)
	}()

	_, server2 := newConfigAndServer(logDir2, port2)
	defer server2.Close()
	defer clearPath("test4")

	var err2 error
	go func() {
		err2 = server2.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port1)}, true)
	}()
	time.Sleep(time.Second)
	c.Assert(err, Equals, nil)
	c.Assert(err2, Equals, nil)
	err3 := server2.AddReadApiKey("db", "key1")
	c.Assert(err3, Equals, nil)
	time.Sleep(time.Millisecond * 200)
	assertConfigContains(port1, "key1", true, c)
	assertConfigContains(port2, "key1", true, c)
}
