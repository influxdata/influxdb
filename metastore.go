package influxdb

import (
	"time"
	"unsafe"

	"github.com/boltdb/bolt"
)

// metastore represents the low-level data store for metadata.
type metastore struct {
	db *bolt.DB
}

// open initializes the metastore.
func (m *metastore) open(path string) error {
	// Open the bolt-backed database.
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	m.db = db

	// Initialize the metastore.
	if err := m.init(); err != nil {
		return err
	}

	return nil
}

// close closes the store.
func (m *metastore) close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// init initializes the metastore to ensure all top-level buckets are created.
func (m *metastore) init() error {
	return m.db.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte("Databases"))
		_, _ = tx.CreateBucketIfNotExists([]byte("Users"))
		return nil
	})
}

// view executes a function in the context of a read-only transaction.
func (m *metastore) view(fn func(*metatx) error) error {
	return m.db.View(func(tx *bolt.Tx) error { return fn(&metatx{tx}) })
}

// update executes a function in the context of a read-write transaction.
func (m *metastore) update(fn func(*metatx) error) error {
	return m.db.Update(func(tx *bolt.Tx) error { return fn(&metatx{tx}) })
}

// mustView executes a function in the context of a read-only transaction.
// Panics if system error occurs. Return error from the fn for validation errors.
func (m *metastore) mustView(fn func(*metatx) error) (err error) {
	if e := m.view(func(tx *metatx) error {
		err = fn(tx)
		return nil
	}); e != nil {
		panic("metastore view: " + err.Error())
	}
	return
}

// mustUpdate executes a function in the context of a read-write transaction.
// Panics if a disk or system error occurs. Return error from the fn for validation errors.
func (m *metastore) mustUpdate(fn func(*metatx) error) (err error) {
	if e := m.update(func(tx *metatx) error {
		err = fn(tx)
		return nil
	}); e != nil {
		panic("metastore update: " + err.Error())
	}
	return
}

// metatx represents a metastore transaction.
type metatx struct {
	*bolt.Tx
}

// database returns a database from the metastore by name.
func (tx *metatx) database(name string) (db *database) {
	if b := tx.Bucket([]byte("Databases")).Bucket([]byte(name)); b != nil {
		mustUnmarshalJSON(b.Get([]byte("meta")), &db)
	}
	return
}

// databases returns a list of all databases from the metastore.
func (tx *metatx) databases() (a []*database) {
	c := tx.Bucket([]byte("Databases")).Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		b := c.Bucket().Bucket(k)
		v := b.Get([]byte("meta"))
		db := newDatabase()
		mustUnmarshalJSON(v, &db)
		a = append(a, db)
	}
	return
}

// saveDatabase persists a database to the metastore.
func (tx *metatx) saveDatabase(db *database) error {
	b, err := tx.Bucket([]byte("Databases")).CreateBucketIfNotExists([]byte(db.name))
	if err != nil {
		return err
	}
	_, err = b.CreateBucketIfNotExists([]byte("TagBytesToID"))
	if err != nil {
		return err
	}
	_, err = b.CreateBucketIfNotExists([]byte("Series"))
	if err != nil {
		return err
	}
	return b.Put([]byte("meta"), mustMarshalJSON(db))
}

// deleteDatabase removes database from the metastore.
func (tx *metatx) deleteDatabase(name string) error {
	return tx.Bucket([]byte("Databases")).DeleteBucket([]byte(name))
}

// sets the series id for the database, name, and tags.
func (tx *metatx) createSeries(database, name string, tags map[string]string) (*Series, error) {
	// create the buckets to store tag indexes for the series and give it a unique ID in the DB
	db := tx.Bucket([]byte("Databases")).Bucket([]byte(database))
	t := db.Bucket([]byte("Series"))
	b, err := t.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}

	// give the series a unique ID
	id, _ := t.NextSequence()

	// store the tag map for the series
	b, err = db.Bucket([]byte("Series")).CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}

	s := &Series{ID: uint32(id), Tags: tags}
	idBytes := make([]byte, 4)
	*(*uint32)(unsafe.Pointer(&idBytes[0])) = uint32(id)

	if err := b.Put(idBytes, mustMarshalJSON(s)); err != nil {
		return nil, err
	}
	return s, nil
}

// series returns all the measurements and series in a database
func (tx *metatx) measurements(database string) []*Measurement {
	// get the bucket that holds series data for the database
	b := tx.Bucket([]byte("Databases")).Bucket([]byte(database)).Bucket([]byte("Series"))

	measurements := make([]*Measurement, 0)
	c := b.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		mc := b.Bucket(k).Cursor()
		m := &Measurement{Name: string(k), Series: make([]*Series, 0)}
		for id, v := mc.First(); id != nil; id, v = mc.Next() {
			var s *Series
			mustUnmarshalJSON(v, &s)
			m.Series = append(m.Series, s)
		}
		measurements = append(measurements, m)
	}
	return measurements
}

// user returns a user from the metastore by name.
func (tx *metatx) user(name string) (u *User) {
	if v := tx.Bucket([]byte("Users")).Get([]byte(name)); v != nil {
		mustUnmarshalJSON(v, &u)
	}
	return
}

// users returns a list of all users from the metastore.
func (tx *metatx) users() (a []*User) {
	c := tx.Bucket([]byte("Users")).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		u := &User{}
		mustUnmarshalJSON(v, &u)
		a = append(a, u)
	}
	return
}

// saveUser persists a user to the metastore.
func (tx *metatx) saveUser(u *User) error {
	return tx.Bucket([]byte("Users")).Put([]byte(u.Name), mustMarshalJSON(u))
}

// deleteUser removes the user from the metastore.
func (tx *metatx) deleteUser(name string) error {
	return tx.Bucket([]byte("Users")).Delete([]byte(name))
}
