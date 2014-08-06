package integration

import (
	"os"
	"time"

	influxdb "github.com/influxdb/influxdb/client"
	. "github.com/influxdb/influxdb/integration/helpers"
	. "launchpad.net/gocheck"
)

type ShardExpirationSuite struct {
	serverProcesses []*Server
}

var _ = Suite(&ShardExpirationSuite{})

func (self *ShardExpirationSuite) SetUpSuite(c *C) {
}

func (self *ShardExpirationSuite) TearDownSuite(c *C) {
	for _, s := range self.serverProcesses {
		s.Stop()
	}
}

func (self *ShardExpirationSuite) TestReplicatedShardExpiration(c *C) {
	// makes sure when a shard expires due to retention policy the data
	// is deleted as well as the metadata

	err := os.RemoveAll("/tmp/influxdb/test")
	c.Assert(err, IsNil)
	s1 := NewServer("integration/test_rf_1.toml", c)
	defer s1.Stop()
	s1.WaitForServerToStart()

	s2 := NewServer("integration/test_rf_2.toml", c)
	defer s2.Stop()
	s2.WaitForServerToStart()

	client := s1.GetClient("", c)
	c.Assert(client.CreateDatabase("db1"), IsNil)
	space := &influxdb.ShardSpace{Name: "short", RetentionPolicy: "5s", Database: "db1", Regex: "/^test_shard_expiration/"}
	err = client.CreateShardSpace(space)
	c.Assert(err, IsNil)

	s1.WriteData(`
[
  {
    "name": "test_shard_expiration",
    "columns": ["time", "val"],
    "points":[[1307997668000, 1]]
  }
]`, c)

	// Make sure the shard exists
	shards, err := client.GetShards()
	shardsInSpace := filterShardsInSpace("short", shards.All)
	c.Assert(shardsInSpace, HasLen, 1)

	time.Sleep(6 * time.Second)

	// Make sure the shard is gone
	shards, err = client.GetShards()
	shardsInSpace = filterShardsInSpace("short", shards.All)
	c.Assert(shardsInSpace, HasLen, 0)

}
