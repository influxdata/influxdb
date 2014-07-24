package integration

import (
	"os"

	. "github.com/influxdb/influxdb/integration/helpers"
	. "launchpad.net/gocheck"
)

type LateJoinSuite struct {
	serverProcesses []*Server
}

var _ = Suite(&LateJoinSuite{})

func (self *LateJoinSuite) SetUpSuite(c *C) {
}

func (self *LateJoinSuite) TearDownSuite(c *C) {
	for _, s := range self.serverProcesses {
		s.Stop()
	}
}

func (self *LateJoinSuite) TestReplayingWAL(c *C) {
	err := os.RemoveAll("/tmp/influxdb/test")
	c.Assert(err, IsNil)
	s1 := NewServer("integration/test_rf_1.toml", c)
	defer s1.Stop()

	client := s1.GetClient("", c)
	c.Assert(client.CreateDatabase("test"), IsNil)
	series := CreatePoints("test_replication_factor_1", 1, 1)
	client = s1.GetClient("test", c)
	c.Assert(client.WriteSeries(series), IsNil)

	s2 := NewServer("integration/test_rf_2.toml", c)
	defer s2.Stop()

	s2.WaitForServerToStart()
	s1.WaitForServerToSync()
	s2.WaitForServerToSync()

	client = s2.GetClient("", c)
	c.Assert(client.Ping(), IsNil)
}
