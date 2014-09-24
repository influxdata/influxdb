package cluster

import (
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/metastore"
	. "launchpad.net/gocheck"
)

type ClusterConfigurationSuite struct{}

var _ = Suite(&ClusterConfigurationSuite{})

// wrote this test while tracking down https://github.com/influxdb/influxdb/issues/886
func (self *ClusterConfigurationSuite) TestSerializesShardSpaces(c *C) {
	config := &configuration.Configuration{}
	store := metastore.NewStore()
	clusterConfig := NewClusterConfiguration(config, nil, nil, nil, store)
	clusterConfig.CreateDatabase("db1")
	space1 := NewShardSpace("db1", "space1")
	space1.Regex = "/space1/"
	space1.ReplicationFactor = 2
	space1.Split = 2
	err := clusterConfig.AddShardSpace(space1)
	c.Assert(err, IsNil)
	space2 := NewShardSpace("db1", "space2")
	space2.Regex = "/space2/"
	err = clusterConfig.AddShardSpace(space2)
	c.Assert(err, IsNil)

	verify := func(conf *ClusterConfiguration) {
		spaces := conf.databaseShardSpaces["db1"]
		c.Assert(spaces, HasLen, 2)
		space2 := spaces[0]
		space1 := spaces[1]
		c.Assert(space1.Name, Equals, "space1")
		c.Assert(space1.Regex, Equals, "/space1/")
		c.Assert(space1.ReplicationFactor, Equals, uint32(2))
		c.Assert(space1.Split, Equals, uint32(2))
		c.Assert(space2.Name, Equals, "space2")
		c.Assert(space2.Regex, Equals, "/space2/")
	}

	verify(clusterConfig)

	bytes, err := clusterConfig.Save()
	c.Assert(err, IsNil)
	newConfig := NewClusterConfiguration(config, nil, nil, nil, store)
	err = newConfig.Recovery(bytes)
	c.Assert(err, IsNil)
	verify(newConfig)
	verify(clusterConfig)
}
