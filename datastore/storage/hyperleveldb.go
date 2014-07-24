// +build hyperleveldb

package storage

import (
	"bytes"
	"fmt"
	"sync"

	hyperleveldb "github.com/influxdb/hyperleveldb-go"
	"github.com/influxdb/influxdb/configuration"
)

const HYPERLEVELDB_NAME = "hyperleveldb"

func init() {
	registerEngine(HYPERLEVELDB_NAME, Initializer{
		NewHyperlevelDBConfig,
		NewHyperlevelDB,
	})
}

type HyperlevelDBConfiguration struct {
	MaxOpenFiles int                `toml:"max-open-files"`
	LruCacheSize configuration.Size `toml:"lru-cache-size"`
}

type HyperlevelDB struct {
	db    *hyperleveldb.DB
	opts  *hyperleveldb.Options
	wopts *hyperleveldb.WriteOptions
	ropts *hyperleveldb.ReadOptions
	path  string
}

func NewHyperlevelDBConfig() interface{} {
	return &HyperlevelDBConfiguration{}
}

var hyperlevelDBCache *hyperleveldb.Cache
var hyperlevelDBCacheLock sync.Mutex

func NewHyperlevelDB(path string, config interface{}) (Engine, error) {
	c, ok := config.(*HyperlevelDBConfiguration)
	if !ok {
		return nil, fmt.Errorf("Config is of type %T instead of %T", config, HyperlevelDBConfiguration{})
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
	if hyperlevelDBCache == nil {
		hyperlevelDBCacheLock.Lock()
		if hyperlevelDBCache == nil {
			hyperlevelDBCache = hyperleveldb.NewLRUCache(int(c.LruCacheSize))
		}
		hyperlevelDBCacheLock.Unlock()
	}

	opts := hyperleveldb.NewOptions()
	opts.SetCache(hyperlevelDBCache)
	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(c.MaxOpenFiles)
	db, err := hyperleveldb.Open(path, opts)
	wopts := hyperleveldb.NewWriteOptions()
	ropts := hyperleveldb.NewReadOptions()
	return HyperlevelDB{db, opts, wopts, ropts, path}, err
}

func (db HyperlevelDB) Compact() {
	db.db.CompactRange(hyperleveldb.Range{})
}

func (db HyperlevelDB) Close() {
	db.ropts.Close()
	db.wopts.Close()
	db.opts.Close()
	db.db.Close()
}

func (db HyperlevelDB) Put(key, value []byte) error {
	return db.BatchPut([]Write{{key, value}})
}

func (db HyperlevelDB) Get(key []byte) ([]byte, error) {
	return db.db.Get(db.ropts, key)
}

func (_ HyperlevelDB) Name() string {
	return HYPERLEVELDB_NAME
}

func (db HyperlevelDB) Path() string {
	return db.path
}

func (db HyperlevelDB) BatchPut(writes []Write) error {
	wb := hyperleveldb.NewWriteBatch()
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

func (db HyperlevelDB) Del(start, finish []byte) error {
	wb := hyperleveldb.NewWriteBatch()
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

type HyperlevelDBIterator struct {
	_itr *hyperleveldb.Iterator
	err  error
}

func (itr *HyperlevelDBIterator) Seek(key []byte) {
	itr._itr.Seek(key)
}

func (itr *HyperlevelDBIterator) Next() {
	itr._itr.Next()
}

func (itr *HyperlevelDBIterator) Prev() {
	itr._itr.Prev()
}

func (itr *HyperlevelDBIterator) Valid() bool {
	return itr._itr.Valid()
}

func (itr *HyperlevelDBIterator) Error() error {
	return itr.err
}

func (itr *HyperlevelDBIterator) Key() []byte {
	return itr._itr.Key()
}

func (itr *HyperlevelDBIterator) Value() []byte {
	return itr._itr.Value()
}

func (itr *HyperlevelDBIterator) Close() error {
	itr._itr.Close()
	return nil
}

func (db HyperlevelDB) Iterator() Iterator {
	itr := db.db.NewIterator(db.ropts)

	return &HyperlevelDBIterator{itr, nil}
}
