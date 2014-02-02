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
	opts.SetBlockSize(TWO_FIFTY_SIX_KILOBYTES)
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
	self.shardsLock.Unlock()

	if db != nil {
		return db, nil
	}

	dbDir := filepath.Join(self.baseDbDir, fmt.Sprintf("%.5d", id))

	log.Info("DATASTORE: opening or creating shard %s", dbDir)
	ldb, err := levigo.Open(dbDir, self.levelDbOptions)
	if err != nil {
		return nil, err
	}

	self.shardsLock.Lock()
	defer self.shardsLock.Unlock()
	db = NewLevelDbShard(ldb)
	self.shards[id] = db
	return db, nil
}
