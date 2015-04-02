package influxdb

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

// metastore represents the low-level data store for metadata.
type metastore struct {
	db *bolt.DB
}

// open initializes the metastore.
func (m *metastore) open(path string) error {
	// Open the bolt-backed database.
	db, err := bolt.Open(path, 0666, &bolt.Options{Timeout: 1 * time.Second})
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
		_, _ = tx.CreateBucketIfNotExists([]byte("Meta"))
		_, _ = tx.CreateBucketIfNotExists([]byte("DataNodes"))
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
		panic("metastore view: " + e.Error())
	}
	return
}

// mustUpdate executes a function in the context of a read-write transaction.
// This function requires an index so that it can track the last commit from the broker.
// Panics if a disk or system error occurs. Return error from the fn for validation errors.
func (m *metastore) mustUpdate(index uint64, fn func(*metatx) error) (err error) {
	if e := m.update(func(tx *metatx) error {
		// Ignore replayed commands.
		curr := tx.index()
		if index > 0 && index <= curr {
			return nil
		}

		// Execute function passed in.
		err = fn(tx)

		// Update index, if set.
		if index > 0 {
			if e := tx.setIndex(index); e != nil {
				return fmt.Errorf("set index: %s", e)
			}
		}
		return nil
	}); e != nil {
		panic("metastore update: " + e.Error())
	}
	return
}

// metatx represents a metastore transaction.
type metatx struct {
	*bolt.Tx
}

// id returns the server id.
func (tx *metatx) id() (id uint64) {
	if v := tx.Bucket([]byte("Meta")).Get([]byte("id")); v != nil {
		id = btou64(v)
	}
	return
}

// setID sets the server id.
func (tx *metatx) setID(v uint64) error {
	return tx.Bucket([]byte("Meta")).Put([]byte("id"), u64tob(v))
}

// index returns the index stored in the meta bucket.
func (tx *metatx) index() (index uint64) {
	if v := tx.Bucket([]byte("Meta")).Get([]byte("index")); v != nil {
		index = btou64(v)
	}
	return
}

// setIndex sets the index stored in the meta bucket.
func (tx *metatx) setIndex(v uint64) error {
	return tx.Bucket([]byte("Meta")).Put([]byte("index"), u64tob(v))
}

// mustNextSequence generates a new sequence for a key in the meta bucket.
func (tx *metatx) mustNextSequence(key []byte) (id uint64) {
	// Retrieve the previous value, if it exists.
	if v := tx.Bucket([]byte("Meta")).Get(key); v != nil {
		id = btou64(v)
	}
	id++

	// Save the new value.
	// This panics on error because keys are hardcoded this shouldn't fail.
	if err := tx.Bucket([]byte("Meta")).Put(key, u64tob(id)); err != nil {
		panic("next seq: " + err.Error())
	}
	return
}

// nextShardID generates a new sequence id for a shard.
func (tx *metatx) nextShardID() (id uint64) {
	return tx.mustNextSequence([]byte("shardID"))
}

// nextShardGroupID generates a new sequence id for a shard group.
func (tx *metatx) nextShardGroupID() (id uint64) {
	return tx.mustNextSequence([]byte("shardGroupID"))
}

// dataNodes returns a list of all data nodes from the metastore.
func (tx *metatx) dataNodes() (a []*DataNode) {
	c := tx.Bucket([]byte("DataNodes")).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		n := newDataNode()
		mustUnmarshalJSON(v, &n)
		a = append(a, n)
	}
	return
}

// nextDataNodeID returns a autoincrementing id.
func (tx *metatx) nextDataNodeID() uint64 {
	id, _ := tx.Bucket([]byte("DataNodes")).NextSequence()
	return id
}

// saveDataNode persists a data node to the metastore.
func (tx *metatx) saveDataNode(n *DataNode) error {
	return tx.Bucket([]byte("DataNodes")).Put(u64tob(n.ID), mustMarshalJSON(n))
}

// deleteDataNode removes data node from the metastore.
func (tx *metatx) deleteDataNode(id uint64) error {
	return tx.Bucket([]byte("DataNodes")).Delete(u64tob(id))
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
	_, _ = b.CreateBucketIfNotExists([]byte("TagBytesToID"))
	_, _ = b.CreateBucketIfNotExists([]byte("Measurements"))
	_, _ = b.CreateBucketIfNotExists([]byte("Series"))
	return b.Put([]byte("meta"), mustMarshalJSON(db))
}

// dropDatabase removes database from the metastore.
func (tx *metatx) dropDatabase(name string) error {
	return tx.Bucket([]byte("Databases")).DeleteBucket([]byte(name))
}

// dropMeasurement removes measurement from the metastore.
func (tx *metatx) dropMeasurement(database, measurement string) error {
	return tx.Bucket([]byte("Databases")).Bucket([]byte(database)).Bucket([]byte("Series")).DeleteBucket([]byte(measurement))
}

// saveMeasurement persists a measurement to the metastore.

func (tx *metatx) saveMeasurement(database string, m *Measurement) error {
	b := tx.Bucket([]byte("Databases")).Bucket([]byte(database)).Bucket([]byte("Measurements"))
	return b.Put([]byte(m.Name), mustMarshalJSON(m))
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
	s := &Series{ID: uint32(id), Tags: tags}
	idBytes := u32tob(uint32(id))

	if err := b.Put(idBytes, mustMarshalJSON(s)); err != nil {
		return nil, err
	}
	return s, nil
}

// dropSeries removes all seriesIDS for a given database/measurement
func (tx *metatx) dropSeries(database string, seriesByMeasurement map[string][]uint32) error {
	for measurement, ids := range seriesByMeasurement {
		b := tx.Bucket([]byte("Databases")).Bucket([]byte(database)).Bucket([]byte("Series")).Bucket([]byte(measurement))
		if b != nil {
			for _, id := range ids {
				if err := b.Delete(u32tob(id)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// loops through all the measurements and series in a database
func (tx *metatx) indexDatabase(db *database) {
	// get the bucket that holds series data for the database
	b := tx.Bucket([]byte("Databases")).Bucket([]byte(db.name))

	// Iterate over and index series.
	seriesBucket := b.Bucket([]byte("Series"))
	c := seriesBucket.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		mc := seriesBucket.Bucket(k).Cursor()
		name := string(k)
		for id, v := mc.First(); id != nil; id, v = mc.Next() {
			var s *Series
			mustUnmarshalJSON(v, &s)
			db.addSeriesToIndex(name, s)
		}
	}

	// Iterate over measurement metadata.
	c = b.Bucket([]byte("Measurements")).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		m := db.createMeasurementIfNotExists(string(k))
		mustUnmarshalJSON(v, &m)
	}
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

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 converts an 8-byte slice into an uint64.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

// u32tob converts a uint32 into a 4-byte slice.
func u32tob(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// btou32 converts an 4-byte slice into an uint32.
func btou32(b []byte) uint32 { return binary.BigEndian.Uint32(b) }
