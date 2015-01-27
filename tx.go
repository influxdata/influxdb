package influxdb

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/influxql"
)

// tx represents a transaction that spans multiple shard data stores.
// This transaction will open and close all data stores atomically.
type tx struct {
	mu     sync.Mutex
	server *Server
	opened bool
	now    time.Time

	itrs []*shardIterator // shard iterators
}

// newTx return a new initialized Tx.
func newTx(server *Server) *tx {
	return &tx{
		server: server,
		now:    time.Now(),
	}
}

// SetNow sets the current time for the transaction.
func (tx *tx) SetNow(now time.Time) { tx.now = now }

// Open opens a read-only transaction on all data stores atomically.
func (tx *tx) Open() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Mark transaction as open.
	tx.opened = true

	// Open each iterator individually. If any fail close the transaction and error out
	for _, itr := range tx.itrs {
		if err := itr.open(); err != nil {
			_ = tx.close()
			return err
		}
	}

	return nil
}

// Close closes all data store transactions atomically.
func (tx *tx) Close() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.close()
}

func (tx *tx) close() error {
	// Mark transaction as closed.
	tx.opened = false

	for _, itr := range tx.itrs {
		_ = itr.close()
	}

	return nil
}

// CreateIterators returns an iterator for a simple select statement.
func (tx *tx) CreateIterators(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
	// Parse the source segments.
	database, policyName, measurement, err := splitIdent(stmt.Source.(*influxql.Measurement).Name)
	if err != nil {
		return nil, err
	}

	// Grab time range from statement.
	tmin, tmax := influxql.TimeRange(stmt.Condition)
	if tmin.IsZero() {
		tmin = time.Unix(0, 1)
	}
	if tmax.IsZero() {
		tmax = tx.now
	}

	// Find database and retention policy.
	db := tx.server.databases[database]
	if db == nil {
		return nil, ErrDatabaseNotFound
	}
	rp := db.policies[policyName]
	if rp == nil {
		return nil, ErrRetentionPolicyNotFound
	}

	// Find shard groups within time range.
	var shardGroups []*ShardGroup
	for _, group := range rp.shardGroups {
		if timeBetweenInclusive(group.StartTime, tmin, tmax) || timeBetweenInclusive(group.EndTime, tmin, tmax) {
			shardGroups = append(shardGroups, group)
		}
	}
	if len(shardGroups) == 0 {
		return nil, nil
	}

	// Normalize dimensions to extract the interval.
	_, dimensions, err := stmt.Dimensions.Normalize()
	if err != nil {
		return nil, err
	}

	// Find measurement.
	m, err := tx.server.measurement(database, measurement)
	if err != nil {
		return nil, err
	}

	// Find field.
	fieldName := stmt.Fields[0].Expr.(*influxql.VarRef).Val
	f := m.FieldByName(fieldName)
	if f == nil {
		return nil, fmt.Errorf("field not found: %s", fieldName)
	}
	tagSets := m.tagSets(stmt, dimensions)

	// Convert time range to bytes.
	kmin := u64tob(uint64(tmin.UnixNano()))
	kmax := u64tob(uint64(tmax.UnixNano()))

	// Create an iterator for every shard.
	var itrs []influxql.Iterator
	for tag, set := range tagSets {
		for _, group := range shardGroups {
			// TODO: only create iterators for the shards we actually have to hit in a group
			for _, sh := range group.Shards {
				itr := &shardIterator{
					fieldID:    f.ID,
					tags:       tag,
					conditions: set, // TODO: only pass in conditions for series that are in this shard
					db:         sh.store,
					cur: &multiCursor{
						kmin: kmin,
						kmax: kmax,
					},
				}

				// Add to tx so the bolt transaction can be opened/closed.
				tx.itrs = append(tx.itrs, itr)

				itrs = append(itrs, itr)
			}
		}
	}

	return itrs, nil
}

// splitIdent splits an identifier into it's database, policy, and measurement parts.
func splitIdent(s string) (db, rp, m string, err error) {
	a, err := influxql.SplitIdent(s)
	if err != nil {
		return "", "", "", err
	} else if len(a) != 3 {
		return "", "", "", fmt.Errorf("invalid ident, expected 3 segments: %q", s)
	}
	return a[0], a[1], a[2], nil
}

// shardIterator represents an iterator for traversing over a single series.
type shardIterator struct {
	fieldID    uint8
	tags       string // encoded dimensional tag values
	conditions map[uint32]influxql.Expr
	db         *bolt.DB // data stores by shard id
	txn        *bolt.Tx // read transactions by shard id

	cur *multiCursor
}

func (i *shardIterator) Tags() string { return i.tags }

func (i *shardIterator) Next() (key int64, value interface{}) {
	// Ignore if there's no more data.
	k, v := i.cur.Next()
	if k == nil {
		return 0, nil
	}

	// Read timestamp & field value.
	key = int64(btou64(k))
	value = unmarshalValue(v, i.fieldID)
	return key, value
}

func (i *shardIterator) open() error {
	// Open the data store
	txn, err := i.db.Begin(false)
	if err != nil {
		return err
	}
	i.txn = txn

	// Open cursors for each series id
	for id, _ := range i.conditions {
		b := i.txn.Bucket(u32tob(id))
		if b == nil {
			continue
		}

		// add the cursor fo this series on the shard
		i.cur.cursors = append(i.cur.cursors, b.Cursor())
	}

	i.cur.initialize()

	return nil
}

func (i *shardIterator) close() error {
	_ = i.txn.Rollback()
	return nil
}

type multiCursor struct {
	cursors    []*bolt.Cursor
	keyValues  []keyValue
	kmin, kmax []byte // min/max keys
}

type keyValue struct {
	key   []byte
	value []byte
}

func (c *multiCursor) initialize() {
	c.keyValues = make([]keyValue, len(c.cursors))
	for i, cur := range c.cursors {
		c.keyValues[i].key, c.keyValues[i].value = cur.Seek(c.kmin)
	}
}

func (c *multiCursor) Next() (key, value []byte) {
	min := -1

	for i, kv := range c.keyValues {
		if kv.key != nil && bytes.Compare(kv.key, c.kmax) == -1 {
			min = i
		}
	}

	// if min is -1 we've exhausted all cursors for the given time range
	if min == -1 {
		return nil, nil
	}

	kv := c.keyValues[min]
	key = kv.key
	value = kv.value

	c.keyValues[min].key, c.keyValues[min].value = c.cursors[min].Next()
	if bytes.Compare(c.keyValues[min].key, c.kmax) == 1 {
		c.keyValues[min].key = nil
	}

	return
}
