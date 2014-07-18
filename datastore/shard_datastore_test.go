package datastore

import (
	"os"

	"github.com/influxdb/influxdb/configuration"
	. "launchpad.net/gocheck"
)

const TEST_DATASTORE_SHARD_DIR = "/tmp/influxdb/_shard_datastore_test"

type ShardDatastoreSuite struct{}

var _ = Suite(&ShardDatastoreSuite{})

func (self *ShardDatastoreSuite) SetUpSuite(c *C) {
	err := os.RemoveAll(TEST_DATASTORE_SHARD_DIR)
	c.Assert(err, IsNil)
}

func (self *ShardDatastoreSuite) TestWillEnforceMaxOpenShards(c *C) {
	config := &configuration.Configuration{}
	config.DataDir = TEST_DATASTORE_SHARD_DIR
	config.StorageMaxOpenShards = 2
	config.StorageDefaultEngine = "leveldb"

	store, err := NewShardDatastore(config, nil)
	c.Assert(err, IsNil)

	shard, err := store.GetOrCreateShard(uint32(2))
	c.Assert(err, IsNil)
	c.Assert(shard.IsClosed(), Equals, false)
	_, err = store.GetOrCreateShard(uint32(1))
	c.Assert(err, IsNil)
	c.Assert(shard.IsClosed(), Equals, false)
	_, err = store.GetOrCreateShard(uint32(3))
	c.Assert(err, IsNil)
	c.Assert(shard.IsClosed(), Equals, false)
	store.ReturnShard(uint32(2))
	c.Assert(shard.IsClosed(), Equals, true)
}
