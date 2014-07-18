package integration

import (
	"os"

	influxdb "github.com/influxdb/influxdb/client"
	. "github.com/influxdb/influxdb/integration/helpers"
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
	s1 := NewServer("integration/test_rf_1.toml", c)
	defer s1.Stop()
	s1.WaitForServerToStart()

	s2 := NewServer("integration/test_rf_2.toml", c)
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

func (self *RemoveNodeSuite) TestRemovingNodeAndShards(c *C) {
	err := os.RemoveAll("/tmp/influxdb/test")
	c.Assert(err, IsNil)
	s1 := NewServer("integration/test_rf_1.toml", c)
	defer s1.Stop()
	s1.WaitForServerToStart()

	s2 := NewServer("integration/test_rf_2.toml", c)
	s2.WaitForServerToStart()

	client := s1.GetClient("", c)
	servers, err := client.Servers()
	c.Assert(err, IsNil)
	c.Assert(servers, HasLen, 2)

	c.Assert(client.CreateDatabase("test"), IsNil)
	space := &influxdb.ShardSpace{Name: "test_space", RetentionPolicy: "1h", Database: "test", Regex: "/test_removing_node_and_shards/", ReplicationFactor: 2}
	c.Assert(client.CreateShardSpace(space), IsNil)

	series := CreatePoints("test_removing_node_and_shards", 5, 10)
	client = s1.GetClient("test", c)
	c.Assert(client.WriteSeries(series), IsNil)

	s1.WaitForServerToSync()

	shards, err := client.GetShards()
	c.Assert(err, IsNil)
	c.Assert(shards.All, HasLen, 1)
	c.Assert(shards.All[0].ServerIds, HasLen, 2)
	c.Assert(shards.All[0].ServerIds[0], Equals, uint32(1))
	c.Assert(shards.All[0].ServerIds[1], Equals, uint32(2))

	s2.Stop()

	c.Assert(client.RemoveServer(2), IsNil)

	servers, err = client.Servers()
	c.Assert(err, IsNil)
	c.Assert(servers, HasLen, 1)

	shards, err = client.GetShards()
	c.Assert(err, IsNil)
	c.Assert(shards.All, HasLen, 1)
	c.Assert(shards.All[0].ServerIds, HasLen, 1)
	c.Assert(shards.All[0].ServerIds[0], Equals, uint32(1))
}
