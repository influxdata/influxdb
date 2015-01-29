package influxdb

import (
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
	if m == nil {
		return nil, ErrMeasurementNotFound
	}

	// Find field.
	fieldName := stmt.Fields[0].Expr.(*influxql.VarRef).Val
	f := m.FieldByName(fieldName)
	if f == nil {
		return nil, fmt.Errorf("field not found: %s", fieldName)
	}
	tagSets := m.tagSets(stmt, dimensions)

	// Create an iterator for every shard.
	var itrs []influxql.Iterator
	for tag, set := range tagSets {
		for _, group := range shardGroups {
			// TODO: only create iterators for the shards we actually have to hit in a group
			for _, sh := range group.Shards {

				// create a series cursor for each unique series id
				cursors := make([]*seriesCursor, 0, len(set))
				for id, cond := range set {
					cursors = append(cursors, &seriesCursor{id: id, condition: cond})
				}

				// create the shard iterator that will map over all series for the shard
				itr := &shardIterator{
					fieldName: f.Name,
					fieldID:   f.ID,
					tags:      tag,
					db:        sh.store,
					cursors:   cursors,
					tmin:      tmin.UnixNano(),
					tmax:      tmax.UnixNano(),
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
	fieldName  string
	fieldID    uint8
	tags       string // encoded dimensional tag values
	cursors    []*seriesCursor
	keyValues  []keyValue
	db         *bolt.DB // data stores by shard id
	txn        *bolt.Tx // read transactions by shard id
	tmin, tmax int64
}

func (i *shardIterator) open() error {
	// Open the data store
	txn, err := i.db.Begin(false)
	if err != nil {
		return err
	}
	i.txn = txn

	// Open cursors for each series id
	for _, c := range i.cursors {
		b := i.txn.Bucket(u32tob(c.id))
		if b == nil {
			continue
		}

		c.cur = b.Cursor()
	}

	i.keyValues = make([]keyValue, len(i.cursors))
	for j, cur := range i.cursors {
		i.keyValues[j].key, i.keyValues[j].value = cur.Next(i.fieldName, i.fieldID, i.tmin, i.tmax)
	}

	return nil
}

func (i *shardIterator) close() error {
	_ = i.txn.Rollback()
	return nil
}

func (i *shardIterator) Tags() string { return i.tags }

func (i *shardIterator) Next() (key int64, value interface{}) {
	min := -1

	for ind, kv := range i.keyValues {
		if kv.key != 0 && kv.key < i.tmax {
			min = ind
		}
	}

	// if min is -1 we've exhausted all cursors for the given time range
	if min == -1 {
		return 0, nil
	}

	kv := i.keyValues[min]
	key = kv.key
	value = kv.value

	i.keyValues[min].key, i.keyValues[min].value = i.cursors[min].Next(i.fieldName, i.fieldID, i.tmin, i.tmax)
	return key, value
}

type keyValue struct {
	key   int64
	value interface{}
}

type seriesCursor struct {
	id          uint32
	condition   influxql.Expr
	cur         *bolt.Cursor
	initialized bool
}

func (c *seriesCursor) Next(fieldName string, fieldID uint8, tmin, tmax int64) (key int64, value interface{}) {
	// TODO: clean this up when we make it so series ids are only queried against the shards they exist in.
	//       Right now we query for all series ids on a query against each shard, even if that shard may not have the
	//       data, so cur could be nil.
	if c.cur == nil {
		return 0, nil
	}

	for {
		var k, v []byte
		if !c.initialized {
			k, v = c.cur.Seek(u64tob(uint64(tmin)))
			c.initialized = true
		} else {
			k, v = c.cur.Next()
		}

		// Exit if there is no more data.
		if k == nil {
			return 0, nil
		}

		// Marshal key & value.
		key, value = int64(btou64(k)), unmarshalValue(v, fieldID)

		if key > tmax {
			return 0, nil
		}

		// Evaluate condition. Move to next key/value if non-true.
		if c.condition != nil {
			if ok, _ := influxql.Eval(c.condition, map[string]interface{}{fieldName: value}).(bool); !ok {
				continue
			}
		}

		return key, value
	}
}
