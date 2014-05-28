package datastore

import (
	"common"
	"configuration"
	. "launchpad.net/gocheck"
	"os"
)

const TEST_DATASTORE_SHARD_DIR = "/tmp/influxdb/leveldb_shard_datastore_test"
const TEST_DATASTORE_SHARD_DIR2 = "/tmp/influxdb/leveldb_shard_datastore_test2"

type LevelDbShardDatastoreSuite struct{}

var _ = Suite(&LevelDbShardDatastoreSuite{})

func (self *LevelDbShardDatastoreSuite) SetUpTest(c *C) {
	for _, name := range []string{TEST_DATASTORE_SHARD_DIR, TEST_DATASTORE_SHARD_DIR2} {
		err := os.RemoveAll(name)
		c.Assert(err, IsNil)
	}
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

func (self *LevelDbShardDatastoreSuite) TestRegressionMismatchedPointsAndColumns(c *C) {
	config := &configuration.Configuration{}
	config.DataDir = TEST_DATASTORE_SHARD_DIR2
	config.LevelDbMaxOpenShards = 1

	store, err := NewLevelDbShardDatastore(config)
	c.Assert(err, IsNil)

	shard, err := store.GetOrCreateShard(uint32(1))
	c.Assert(err, IsNil)
	c.Assert(shard.IsClosed(), Equals, false)

	series, err := common.StringToSeriesArray(`
	[
		{
			"points": [
				{"values": [{"int64_value": 100}, {"int64_value": 200}], "timestamp": 1381346631, "sequence_number": 1},
				{"values": [{"int64_value": 100}], "timestamp": 1381346631, "sequence_number": 2}
			],
			"name": "t",
			"fields": ["column_one", "column_two"]
		}
	]
	`)
	c.Assert(err, IsNil)

	// Don't panic when the series missing some field value (2nd points doesn't have column_two)
	err = shard.Write("debug", series)
	c.Assert(err, IsNil)
}
