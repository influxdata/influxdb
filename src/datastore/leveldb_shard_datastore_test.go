package datastore

import (
	"configuration"
	. "launchpad.net/gocheck"
	"os"
)

const TEST_DATASTORE_SHARD_DIR = "/tmp/influxdb/leveldb_shard_datastore_test"

type LevelDbShardDatastoreSuite struct{}

var _ = Suite(&LevelDbShardDatastoreSuite{})

func (self *LevelDbShardDatastoreSuite) SetUpSuite(c *C) {
	err := os.RemoveAll(TEST_DATASTORE_SHARD_DIR)
	c.Assert(err, IsNil)
}

func (self *LevelDbShardDatastoreSuite) TestWillEnforceMaxOpenShards(c *C) {
	config := &configuration.Configuration{}
	config.DataDir = TEST_DATASTORE_SHARD_DIR
	config.LevelDbMaxOpenShards = 2

	store, err := NewLevelDbShardDatastore(config)
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
