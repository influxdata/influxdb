package storage

import (
	"bytes"
	"configuration"
	"fmt"
	"os"

	mdb "github.com/influxdb/gomdb"
)

const MDB_NAME = "lmdb"

func init() {
	registerEngine(MDB_NAME, Initializer{
		NewMDBConfiguration,
		NewMDB,
	})
}

type MDBConfiguration struct {
	MapSize configuration.Size `toml:"map-size"`
}

type MDB struct {
	env  *mdb.Env
	db   mdb.DBI
	path string
}

func NewMDBConfiguration() interface{} {
	return &MDBConfiguration{}
}

func NewMDB(path string, config interface{}) (Engine, error) {
	c, ok := config.(*MDBConfiguration)
	if !ok {
		return nil, fmt.Errorf("Got config of type %T instead of %T", config, MDBConfiguration{})
	}

	if c.MapSize == 0 {
		c.MapSize = 10 * 1024 * 1024 * 1024
	}

	env, err := mdb.NewEnv()
	if err != nil {
		return MDB{}, err
	}

	// TODO: max dbs should be configurable
	if err := env.SetMaxDBs(1); err != nil {
		return MDB{}, err
	}
	if err := env.SetMapSize(uint64(c.MapSize)); err != nil {
		return MDB{}, err
	}

	if _, err := os.Stat(path); err != nil {
		err = os.MkdirAll(path, 0755)
		if err != nil {
			return MDB{}, err
		}
	}

	err = env.Open(path, mdb.WRITEMAP|mdb.MAPASYNC|mdb.CREATE, 0755)
	if err != nil {
		return MDB{}, err
	}

	tx, err := env.BeginTxn(nil, 0)
	if err != nil {
		return MDB{}, err
	}

	dbi, err := tx.DBIOpen(nil, mdb.CREATE|mdb.DUPSORT|mdb.DUPFIXED)
	if err != nil {
		return MDB{}, err
	}

	if err := tx.Commit(); err != nil {
		return MDB{}, err
	}

	db := MDB{
		env:  env,
		db:   dbi,
		path: path,
	}

	return db, nil
}

// InfluxDB uses 24 byte keys and small values. The keys are of
// the form SSSSttttssss
// where SSSS is an 8 byte seriesID, tttt is an 8 byte timestamp,
// and ssss is an 8 byte sequence number. We use MDB_DUPSORT and
// reconstruct this as a 6 byte key with duplicate values:
// key SSS
// value SttttssssVVVV

// Turn an InfluxDB KV pair to an internal KV pair
func ImportKV(key, val[]byte) ([]byte, []byte) {
	newk := key[0:6]
	newv := make([]byte, len(key) - 6 + len(val))
	copy(newv, key[6:])
	if val != nil {
		copy(newv[18:], val)
	}
	return newk, newv
}

// Turn an internal KV pair into an InfluxDB KV pair
func ExportKV(key, val[]byte) ([]byte, []byte) {
	newk := make([]byte, 24)
	copy(newk, key)
	copy(newk[6:], val[0:18])
	newv := val[18:]
	return newk, newv
}

func (db MDB) Put(key, value []byte) error {
	return db.BatchPut([]Write{{key, value}})
}

func (db MDB) BatchPut(writes []Write) error {
	itr := db.iterator(false)

	for _, w := range writes {
		key, val := ImportKV(w.Key, w.Value)
		if w.Value == nil {
			itr.key, itr.value, itr.err = itr.c.Get(key, val, mdb.GET_RANGE)
			if itr.err == nil && bytes.Compare(itr.key, w.Key) == 0 {
				itr.err = itr.c.Del(0)
			}
		} else {
			itr.err = itr.c.Put(key, val, 0)
		}

		if itr.err != nil && itr.err != mdb.NotFound {
			break
		}
	}
	itr.setState()

	return itr.Close()
}

func (db MDB) Get(key []byte) ([]byte, error) {
	itr := db.iterator(true)
	defer itr.Close()

	k, v := ImportKV(key, nil)
	k, v, err := itr.c.Get(k, v, mdb.GET_RANGE)
	if err == nil {
		k, v = ExportKV(k, v)
		if bytes.Compare(k, key) == 0 {
			return v, nil
		}
		return nil, nil
	}
	return nil, err
}

func (db MDB) Del(start, finish []byte) error {
	itr := db.iterator(false)
	defer itr.Close()

	count := 0
	for itr.Seek(start); itr.Valid(); itr.Next() {
		key := itr.Key()
		if bytes.Compare(key, finish) > 0 {
			break
		}

		err := itr.c.Del(0)
		if err != nil {
			return err
		}
		count++
	}
	return nil
}

type MDBIterator struct {
	key   []byte
	value []byte
	c     *mdb.Cursor
	tx    *mdb.Txn
	valid bool
	err   error
}

func (itr *MDBIterator) Key() []byte {
	return itr.key
}

func (itr *MDBIterator) Value() []byte {
	return itr.value
}

func (itr *MDBIterator) Valid() bool {
	return itr.valid
}

func (itr *MDBIterator) Error() error {
	return itr.err
}

func (itr *MDBIterator) getCurrent() {
	var k, v []byte
	k, v, itr.err = itr.c.Get(nil, nil, mdb.GET_CURRENT)
	if itr.err == nil {
		itr.key, itr.value = ExportKV(k, v)
	}
	itr.setState()
}

func (itr *MDBIterator) Seek(key []byte) {
	k, v := ImportKV(key, nil)
	k, v, itr.err = itr.c.Get(k, v, mdb.GET_RANGE)
	if itr.err == nil {
		itr.key, itr.value = ExportKV(k, v)
	} else {
		itr.key = nil
		itr.value = nil
	}
	itr.setState()
}
func (itr *MDBIterator) Next() {
	var k, v []byte
	k, v, itr.err = itr.c.Get(nil, nil, mdb.NEXT)
	if itr.err == nil {
		itr.key, itr.value = ExportKV(k, v)
	} else {
		itr.key = nil
		itr.value = nil
	}
	itr.setState()
}

func (itr *MDBIterator) Prev() {
	var k, v []byte
	k, v, itr.err = itr.c.Get(nil, nil, mdb.PREV)
	if itr.err == nil {
		itr.key, itr.value = ExportKV(k, v)
	} else {
		itr.key = nil
		itr.value = nil
	}
	itr.setState()
}

func (itr *MDBIterator) setState() {
	if itr.err != nil {
		if itr.err == mdb.NotFound {
			itr.err = nil
		}
		itr.valid = false
	}
}

func (itr *MDBIterator) Close() error {
	if err := itr.c.Close(); err != nil {
		itr.tx.Abort()
		return err
	}
	if itr.err != nil {
		itr.tx.Abort()
		return itr.err
	}
	return itr.tx.Commit()
}

func (_ MDB) Name() string {
	return MDB_NAME
}

func (db MDB) Path() string {
	return db.path
}

func (db MDB) Iterator() Iterator {
	return db.iterator(true)
}

func (db MDB) Compact() {
}

func (db MDB) iterator(rdonly bool) *MDBIterator {
	flags := uint(0)
	if rdonly {
		flags = mdb.RDONLY
	}
	tx, err := db.env.BeginTxn(nil, flags)
	if err != nil {
		return &MDBIterator{nil, nil, nil, nil, false, err}
	}

	c, err := tx.CursorOpen(db.db)
	if err != nil {
		tx.Abort()
		return &MDBIterator{nil, nil, nil, nil, false, err}
	}

	return &MDBIterator{nil, nil, c, tx, true, nil}
}

func (db MDB) Close() {
	db.env.DBIClose(db.db)
	if err := db.env.Close(); err != nil {
		panic(err)
	}
}
