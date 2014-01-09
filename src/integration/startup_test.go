package integration

import (
	. "launchpad.net/gocheck"
	"os"
	"time"
)

type StartupSuite struct {
	serverProcesses []*ServerProcess
}

var _ = Suite(&StartupSuite{})

func (self *StartupSuite) SetUpSuite(c *C) {
	err := os.RemoveAll("/tmp/influxdb/test")
	c.Assert(err, IsNil)
	self.serverProcesses = []*ServerProcess{
		NewServerProcess("test_config1.toml", 60500, 0, c),
		NewServerProcess("test_config2.toml", 60506, 0, c),
		NewServerProcess("test_config3.toml", 60510, 0, c),
	}
}

func (self *StartupSuite) TearDownSuite(c *C) {
	for _, s := range self.serverProcesses {
		s.Stop()
	}
}

func (self *StartupSuite) TestStartup(c *C) {
	time.Sleep(10 * time.Second)
	self.serverProcesses[0].Post("/db?u=root&p=root", "{\"name\":\"full_rep\", \"replicationFactor\":3}", c)
	self.serverProcesses[1].Post("/db?u=root&p=root", "{\"name\":\"test_rep\", \"replicationFactor\":2}", c)
	self.serverProcesses[2].Post("/db?u=root&p=root", "{\"name\":\"single_rep\", \"replicationFactor\":1}", c)
}
