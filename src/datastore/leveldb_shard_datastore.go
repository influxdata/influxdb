package datastore

import (
	"cluster"
	log "code.google.com/p/log4go"
	"configuration"
	"fmt"
	"github.com/jmhodges/levigo"
	"os"
	"path/filepath"
	"sync"
)

type LevelDbShardDatastore struct {
	baseDbDir      string
	config         *configuration.Configuration
	shards         map[uint32]*LevelDbShard
	shardsLock     sync.RWMutex
	levelDbOptions *levigo.Options
}

const (
	ONE_KILOBYTE                    = 1024
	SHARD_BLOOM_FILTER_BITS_PER_KEY = 10
	SHARD_DATABASE_DIR              = "shard_db"
)

func NewLevelDbShardDatastore(config *configuration.Configuration) (*LevelDbShardDatastore, error) {
	baseDbDir := filepath.Join(config.DataDir, SHARD_DATABASE_DIR)
	err := os.MkdirAll(baseDbDir, 0744)
	if err != nil {
		return nil, err
	}
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(ONE_MEGABYTE))
	opts.SetCreateIfMissing(true)
	opts.SetBlockSize(64 * ONE_KILOBYTE)
	filter := levigo.NewBloomFilter(SHARD_BLOOM_FILTER_BITS_PER_KEY)
	opts.SetFilterPolicy(filter)

	return &LevelDbShardDatastore{
		baseDbDir:      baseDbDir,
		config:         config,
		shards:         make(map[uint32]*LevelDbShard),
		levelDbOptions: opts,
	}, nil
}

func (self *LevelDbShardDatastore) GetOrCreateShard(id uint32) (cluster.LocalShardDb, error) {
	self.shardsLock.RLock()
	db := self.shards[id]
	self.shardsLock.RUnlock()

	if db != nil {
		return db, nil
	}

	self.shardsLock.Lock()
	defer self.shardsLock.Unlock()

	// check to make sure it hasn't been put there between the RUnlock and the Lock
	db = self.shards[id]
	if db != nil {
		return db, nil
	}

	dbDir := self.shardDir(id)

	log.Info("DATASTORE: opening or creating shard %s", dbDir)
	ldb, err := levigo.Open(dbDir, self.levelDbOptions)
	if err != nil {
		return nil, err
	}

	db, err = NewLevelDbShard(ldb)
	if err != nil {
		return nil, err
	}
	self.shards[id] = db
	return db, nil
}

func (self *LevelDbShardDatastore) DeleteShard(shardId uint32) error {
	self.shardsLock.Lock()
	shardDb := self.shards[shardId]
	delete(self.shards, shardId)
	self.shardsLock.Unlock()

	if shardDb != nil {
		shardDb.close()
	}

	dir := self.shardDir(shardId)
	log.Info("DATASTORE: dropping shard %s", dir)
	return os.RemoveAll(dir)
}

func (self *LevelDbShardDatastore) shardDir(id uint32) string {
	return filepath.Join(self.baseDbDir, fmt.Sprintf("%.5d", id))
}
