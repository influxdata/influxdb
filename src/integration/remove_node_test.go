package integration

import (
	. "integration/helpers"
	"os"

	. "launchpad.net/gocheck"
)

type RemoveNodeSuite struct {
	serverProcesses []*Server
}

var _ = Suite(&RemoveNodeSuite{})

func (self *RemoveNodeSuite) SetUpSuite(c *C) {
}

func (self *RemoveNodeSuite) TearDownSuite(c *C) {
	for _, s := range self.serverProcesses {
		s.Stop()
	}
}

func (self *RemoveNodeSuite) TestRemovingNode(c *C) {
	err := os.RemoveAll("/tmp/influxdb/test")
	c.Assert(err, IsNil)
	s1 := NewServer("src/integration/test_rf_1.toml", c)
	defer s1.Stop()
	s1.WaitForServerToStart()

	s2 := NewServer("src/integration/test_rf_2.toml", c)
	s2.WaitForServerToStart()

	client := s1.GetClient("", c)
	servers, err := client.Servers()
	c.Assert(err, IsNil)
	c.Assert(servers, HasLen, 2)

	s2.Stop()

	c.Assert(client.RemoveServer(2), IsNil)

	servers, err = client.Servers()
	c.Assert(err, IsNil)
	c.Assert(servers, HasLen, 1)

	c.Assert(client.CreateDatabase("test"), IsNil)
	series := CreatePoints("test_replication_factor_1", 1, 1)
	client = s1.GetClient("test", c)
	c.Assert(client.WriteSeries(series), IsNil)

	s1.WaitForServerToSync()
}
