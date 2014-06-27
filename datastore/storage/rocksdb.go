// +build rocksdb

package storage

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"

	"github.com/influxdb/influxdb/configuration"
	rocksdb "github.com/influxdb/rocksdb"
)

const ROCKSDB_NAME = "rocksdb"

func init() {
	registerEngine(ROCKSDB_NAME, Initializer{
		NewRocksDBConfig,
		NewRocksDB,
	})
}

type RocksDBConfiguration struct {
	MaxOpenFiles int                `toml:"max-open-files"`
	LruCacheSize configuration.Size `toml:"lru-cache-size"`
}

type RocksDB struct {
	db    *rocksdb.DB
	opts  *rocksdb.Options
	wopts *rocksdb.WriteOptions
	ropts *rocksdb.ReadOptions
	path  string
}

func NewRocksDBConfig() interface{} {
	return &RocksDBConfiguration{}
}

var rocksDBCache *rocksdb.Cache
var rocksDBCacheLock sync.Mutex

func NewRocksDB(path string, config interface{}) (Engine, error) {
	c, ok := config.(*RocksDBConfiguration)
	if !ok {
		return nil, fmt.Errorf("Config is of type %T instead of %T", config, RocksDBConfiguration{})
	}

	// if it wasn't set, set it to 100
	if c.MaxOpenFiles == 0 {
		c.MaxOpenFiles = 100
	}

	// if it wasn't set, set it to 200 MB
	if c.LruCacheSize == 0 {
		c.LruCacheSize = 200 * 1024 * 1024
	}

	// initialize the global cache
	if rocksDBCache == nil {
		rocksDBCacheLock.Lock()
		if rocksDBCache == nil {
			rocksDBCache = rocksdb.NewLRUCache(int(c.LruCacheSize))
		}
		rocksDBCacheLock.Unlock()
	}

	opts := rocksdb.NewOptions()
	env := rocksdb.NewDefaultEnv()
	env.SetBackgroundThreads(runtime.NumCPU() * 2)
	env.SetHighPriorityBackgroundThreads(1)
	opts.SetMaxBackgroundCompactions(runtime.NumCPU()*2 - 1)
	opts.SetMaxBackgroundFlushes(1)
	opts.SetEnv(env)
	opts.SetCache(rocksDBCache)
	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(c.MaxOpenFiles)
	db, err := rocksdb.Open(path, opts)
	wopts := rocksdb.NewWriteOptions()
	ropts := rocksdb.NewReadOptions()
	return RocksDB{db, opts, wopts, ropts, path}, err
}

func (db RocksDB) Compact() {
	db.db.CompactRange(rocksdb.Range{})
}

func (db RocksDB) Close() {
	db.ropts.Close()
	db.wopts.Close()
	db.opts.Close()
	db.db.Close()
}

func (db RocksDB) Put(key, value []byte) error {
	return db.BatchPut([]Write{{key, value}})
}

func (db RocksDB) Get(key []byte) ([]byte, error) {
	return db.db.Get(db.ropts, key)
}

func (_ RocksDB) Name() string {
	return ROCKSDB_NAME
}

func (db RocksDB) Path() string {
	return db.path
}

func (db RocksDB) BatchPut(writes []Write) error {
	wb := rocksdb.NewWriteBatch()
	defer wb.Close()
	for _, w := range writes {
		if w.Value == nil {
			wb.Delete(w.Key)
			continue
		}
		wb.Put(w.Key, w.Value)
	}
	return db.db.Write(db.wopts, wb)
}

func (db RocksDB) Del(start, finish []byte) error {
	wb := rocksdb.NewWriteBatch()
	defer wb.Close()

	itr := db.Iterator()
	defer itr.Close()

	for itr.Seek(start); itr.Valid(); itr.Next() {
		k := itr.Key()
		if bytes.Compare(k, finish) > 0 {
			break
		}
		wb.Delete(k)
	}

	if err := itr.Error(); err != nil {
		return err
	}

	return db.db.Write(db.wopts, wb)
}

type RocksDBIterator struct {
	_itr *rocksdb.Iterator
	err  error
}

func (itr *RocksDBIterator) Seek(key []byte) {
	itr._itr.Seek(key)
}

func (itr *RocksDBIterator) Next() {
	itr._itr.Next()
}

func (itr *RocksDBIterator) Prev() {
	itr._itr.Prev()
}

func (itr *RocksDBIterator) Valid() bool {
	return itr._itr.Valid()
}

func (itr *RocksDBIterator) Error() error {
	return itr.err
}

func (itr *RocksDBIterator) Key() []byte {
	return itr._itr.Key()
}

func (itr *RocksDBIterator) Value() []byte {
	return itr._itr.Value()
}

func (itr *RocksDBIterator) Close() error {
	itr._itr.Close()
	return nil
}

func (db RocksDB) Iterator() Iterator {
	itr := db.db.NewIterator(db.ropts)

	return &RocksDBIterator{itr, nil}
}
