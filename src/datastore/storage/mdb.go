package storage

import (
	"bytes"
	"os"

	mdb "github.com/szferi/gomdb"
)

const MDB_NAME = "lmdb"

func init() {
	registerEngine(MDB_NAME, NewMDB)
}

type Mdb struct {
	env  *mdb.Env
	db   mdb.DBI
	path string
}

func NewMDB(path string) (Engine, error) {
	env, err := mdb.NewEnv()
	if err != nil {
		return Mdb{}, err
	}

	// TODO: max dbs should be configurable
	if err := env.SetMaxDBs(10); err != nil {
		return Mdb{}, err
	}
	if err := env.SetMapSize(10 << 30); err != nil {
		return Mdb{}, err
	}

	if _, err := os.Stat(path); err != nil {
		err = os.MkdirAll(path, 0755)
		if err != nil {
			return Mdb{}, err
		}
	}

	err = env.Open(path, mdb.WRITEMAP|mdb.MAPASYNC|mdb.CREATE, 0755)
	if err != nil {
		return Mdb{}, err
	}

	tx, err := env.BeginTxn(nil, 0)
	if err != nil {
		return Mdb{}, err
	}

	dbi, err := tx.DBIOpen(nil, mdb.CREATE)
	if err != nil {
		return Mdb{}, err
	}

	if err := tx.Commit(); err != nil {
		return Mdb{}, err
	}

	db := Mdb{
		env:  env,
		db:   dbi,
		path: path,
	}

	return db, nil
}

func (db Mdb) Put(key, value []byte) error {
	return db.BatchPut([]Write{Write{key, value}})
}

func (db Mdb) BatchPut(writes []Write) error {
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

func (db Mdb) Get(key []byte) ([]byte, error) {
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

func (db Mdb) Del(start, finish []byte) error {
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

type MdbIterator struct {
	key   []byte
	value []byte
	c     *mdb.Cursor
	tx    *mdb.Txn
	valid bool
	err   error
}

func (itr *MdbIterator) Key() []byte {
	return itr.key
}

func (itr *MdbIterator) Value() []byte {
	return itr.value
}

func (itr *MdbIterator) Valid() bool {
	return itr.valid
}

func (itr *MdbIterator) Error() error {
	return itr.err
}

func (itr *MdbIterator) getCurrent() {
	itr.key, itr.value, itr.err = itr.c.Get(nil, mdb.GET_CURRENT)
	itr.setState()
}

func (itr *MdbIterator) Seek(key []byte) {
	itr.key, itr.value, itr.err = itr.c.Get(key, mdb.SET_RANGE)
	itr.setState()
}
func (itr *MdbIterator) Next() {
	itr.key, itr.value, itr.err = itr.c.Get(nil, mdb.NEXT)
	itr.setState()
}

func (itr *MdbIterator) Prev() {
	itr.key, itr.value, itr.err = itr.c.Get(nil, mdb.PREV)
	itr.setState()
}

func (itr *MdbIterator) setState() {
	if itr.err != nil {
		if itr.err == mdb.NotFound {
			itr.err = nil
		}
		itr.valid = false
	}
}

func (itr *MdbIterator) Close() error {
	if err := itr.c.Close(); err != nil {
		itr.tx.Commit()
		return err
	}
	return itr.tx.Commit()
}

func (_ Mdb) Name() string {
	return MDB_NAME
}

func (db Mdb) Path() string {
	return db.path
}

func (db Mdb) Iterator() Iterator {
	return db.iterator(true)
}

func (db Mdb) Compact() {
}

func (db Mdb) iterator(rdonly bool) *MdbIterator {
	flags := uint(0)
	if rdonly {
		flags = mdb.RDONLY
	}
	tx, err := db.env.BeginTxn(nil, flags)
	if err != nil {
		return &MdbIterator{nil, nil, nil, nil, false, err}
	}

	c, err := tx.CursorOpen(db.db)
	if err != nil {
		tx.Abort()
		return &MdbIterator{nil, nil, nil, nil, false, err}
	}

	return &MdbIterator{nil, nil, c, tx, true, nil}
}

func (db Mdb) Close() {
	db.env.DBIClose(db.db)
	if err := db.env.Close(); err != nil {
		panic(err)
	}
}
