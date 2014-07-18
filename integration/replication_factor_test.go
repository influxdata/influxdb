package integration

import (
	"os"

	. "github.com/influxdb/influxdb/integration/helpers"
	. "launchpad.net/gocheck"
)

type ReplicationFactorSuite struct {
	serverProcesses []*Server
}

var _ = Suite(&ReplicationFactorSuite{})

func (self *ReplicationFactorSuite) SetUpSuite(c *C) {
	err := os.RemoveAll("/tmp/influxdb/test")
	c.Assert(err, IsNil)
	self.serverProcesses = []*Server{
		NewServer("integration/test_rf_1.toml", c),
		NewServer("integration/test_rf_2.toml", c),
	}
	client := self.serverProcesses[0].GetClient("", c)
	c.Assert(client.CreateDatabase("test"), IsNil)
	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}
}

func (self *ReplicationFactorSuite) TearDownSuite(c *C) {
	for _, s := range self.serverProcesses {
		s.Stop()
	}
}

func (self *ReplicationFactorSuite) TestReplayingWAL(c *C) {
	rootClient := self.serverProcesses[0].GetClient("test_replaying_wal", c)
	c.Assert(rootClient.CreateDatabase("test_replaying_wal"), IsNil)
	series := CreatePoints("test_replication_factor_1", 1, 1)
	c.Assert(rootClient.WriteSeries(series), IsNil)

	self.serverProcesses[0].Stop()
	self.serverProcesses[1].Stop()

	self.serverProcesses[1].Start()
	self.serverProcesses[1].WaitForServerToStart()
	self.serverProcesses[0].Start()
	self.serverProcesses[0].WaitForServerToStart()

	for _, s := range self.serverProcesses {
		s.WaitForServerToSync()
	}

	for _, s := range self.serverProcesses {
		client := s.GetClient("", c)
		c.Assert(client.Ping(), IsNil)
	}
}
