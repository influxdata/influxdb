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

func (self *ServerProcess) Query(database, query string, c *C) []interface{} {
	encodedQuery := url.QueryEscape(query)
	fullUrl := fmt.Sprintf("http://localhost:%d/db/%s/series?u=paul&p=pass&q=%s", self.apiPort, database, encodedQuery)
	resp, err := http.Get(fullUrl)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	var js []interface{}
	err = json.Unmarshal(body, &js)
	c.Assert(err, IsNil)
	return js
}

func (self *ServerProcess) Post(url, data string, c *C) *http.Response {
	fullUrl := fmt.Sprintf("http://localhost:%d%s", self.apiPort, url)
	resp, err := http.Post(fullUrl, "application/json", bytes.NewBufferString(data))
	c.Assert(err, IsNil)
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
	time.Sleep(time.Second)
	self.serverProcesses[0].Post("/db?u=root&p=root", "{\"name\":\"full_rep\", \"replicationFactor\":3}", c)
	self.serverProcesses[0].Post("/db?u=root&p=root", "{\"name\":\"test_rep\", \"replicationFactor\":2}", c)
	self.serverProcesses[0].Post("/db?u=root&p=root", "{\"name\":\"single_rep\", \"replicationFactor\":2}", c)
	self.serverProcesses[0].Post("/db/full_rep/users?u=root&p=root", "{\"name\":\"paul\", \"password\":\"pass\"}", c)
	self.serverProcesses[0].Post("/db/test_rep/users?u=root&p=root", "{\"name\":\"paul\", \"password\":\"pass\"}", c)
	self.serverProcesses[0].Post("/db/single_rep/users?u=root&p=root", "{\"name\":\"paul\", \"password\":\"pass\"}", c)
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
	self.serverProcesses[0].Post("/db/single_rep/series?u=paul&p=pass", data, c)
	time.Sleep(time.Millisecond * 10)
	self.serverProcesses[0].Query("single_rep", "select * from test_restart", c)

	response := self.serverProcesses[0].Query("single_rep", "select * from test_restart", c)
	c.Assert(response, HasLen, 1)
	series := response[0].(map[string]interface{})
	points := series["points"].([]interface{})
	c.Assert(points, HasLen, 1)

	for _, s := range self.serverProcesses {
		s.Stop()
	}
	time.Sleep(time.Second)

	err := self.serverProcesses[0].Start()
	c.Assert(err, IsNil)
	err = self.serverProcesses[1].Start()
	c.Assert(err, IsNil)
	err = self.serverProcesses[2].Start()
	time.Sleep(time.Second)

	response = self.serverProcesses[0].Query("single_rep", "select * from test_restart", c)
	c.Assert(response, HasLen, 1)
	series = response[0].(map[string]interface{})
	points = series["points"].([]interface{})
	c.Assert(points, HasLen, 1)
}
