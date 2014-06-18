package storage

import (
	"bytes"
	"configuration"
	"fmt"
	"os"

	mdb "github.com/szferi/gomdb"
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

	dbi, err := tx.DBIOpen(nil, mdb.CREATE)
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

func (db MDB) Put(key, value []byte) error {
	return db.BatchPut([]Write{Write{key, value}})
}

func (db MDB) BatchPut(writes []Write) error {
	tx, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}

	for _, w := range writes {
		var err error
		if w.Value == nil {
			err = tx.Del(db.db, w.Key, nil)
		} else {
			err = tx.Put(db.db, w.Key, w.Value, 0)
		}

		if err != nil && err != mdb.NotFound {
			tx.Abort()
			return err
		}
	}

	return tx.Commit()
}

func (db MDB) Get(key []byte) ([]byte, error) {
	tx, err := db.env.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		return nil, err
	}
	defer tx.Commit()

	v, err := tx.Get(db.db, key)
	if err == mdb.NotFound {
		return nil, nil
	}
	return v, err
}

func (db MDB) Del(start, finish []byte) error {
	tx, err := db.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}
	defer tx.Commit()

	itr := db.iterator(true)
	defer itr.Close()

	count := 0
	for itr.Seek(start); itr.Valid(); itr.Next() {
		key := itr.Key()
		if bytes.Compare(key, finish) > 0 {
			break
		}

		// TODO: We should be using one cursor instead of two
		// transactions, but deleting using a cursor, crashes
		err = tx.Del(db.db, itr.key, nil)
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
	itr.key, itr.value, itr.err = itr.c.Get(nil, mdb.GET_CURRENT)
	itr.setState()
}

func (itr *MDBIterator) Seek(key []byte) {
	itr.key, itr.value, itr.err = itr.c.Get(key, mdb.SET_RANGE)
	itr.setState()
}
func (itr *MDBIterator) Next() {
	itr.key, itr.value, itr.err = itr.c.Get(nil, mdb.NEXT)
	itr.setState()
}

func (itr *MDBIterator) Prev() {
	itr.key, itr.value, itr.err = itr.c.Get(nil, mdb.PREV)
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
		itr.tx.Commit()
		return err
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
