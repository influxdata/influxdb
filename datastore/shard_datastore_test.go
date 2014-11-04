package datastore

import (
	"errors"
	"os"
	"path"

	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/datastore/storage"
	"github.com/influxdb/influxdb/metastore"

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

type mockDB struct{}

func (m mockDB) Name() string { return "mockdb" }

func (m mockDB) Path() string { return "" }

func (m mockDB) Put(key, value []byte) error { return nil }

func (m mockDB) Get(key []byte) ([]byte, error) { return []byte{}, nil }

func (m mockDB) BatchPut(writes []storage.Write) error { return nil }

func (m mockDB) Del(first, last []byte) error { return nil }

func (m mockDB) Iterator() storage.Iterator { return nil }

func (m mockDB) Compact() {}

func (m mockDB) Close() {}

func NewMockDB(path string, config interface{}) (storage.Engine, error) {
	return nil, errors.New("some error")
}

type mockDBConfig struct{}

func NewMockDBConfig() interface{} {
	return &mockDBConfig{}
}

func (self *ShardDatastoreSuite) TestGetOrCreateShardDeletesFilesOnError(c *C) {
	// Register our mockDB so that ShardData will create & use it instead
	// of a real one (i.e., leveldb, rocksdb, etc.).
	err := storage.RegisterEngine("mockdb", storage.Initializer{NewMockDBConfig, NewMockDB})
	c.Assert(err, IsNil)

	config := &configuration.Configuration{
		DataDir:              TEST_DATASTORE_SHARD_DIR,
		StorageMaxOpenShards: 100,
		StorageDefaultEngine: "mockdb",
	}

	sd, err := NewShardDatastore(config, metastore.NewStore())
	c.Assert(err, IsNil)

	s, err := sd.GetOrCreateShard(999)
	c.Assert(err, NotNil)
	c.Assert(s, IsNil)
	_, err = os.Stat(path.Join(TEST_DATASTORE_SHARD_DIR, "shard_db_v2/00999"))
	c.Assert(os.IsNotExist(err), Equals, true)
}
