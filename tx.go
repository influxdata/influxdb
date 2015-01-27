package influxdb

import (
	"bytes"
	"errors"
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

	dbs  map[uint64]*bolt.DB // data stores by shard id
	txns map[uint64]*bolt.Tx // read transactions by shard id
	itrs []*seriesIterator   // series iterators
}

// newTx return a new initialized Tx.
func newTx(server *Server) *tx {
	return &tx{
		server: server,
		now:    time.Now(),
		dbs:    make(map[uint64]*bolt.DB),
		txns:   make(map[uint64]*bolt.Tx),
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

	// Open each data store individually. If any fail then close the whole txn.
	for shardID, db := range tx.dbs {
		txn, err := db.Begin(false)
		if err != nil {
			_ = tx.close()
			return err
		}
		tx.txns[shardID] = txn
	}

	// Open cursors on every series iterator.
	for _, itr := range tx.itrs {
		for _, shardID := range itr.shardIDs {
			txn := tx.txns[shardID]

			// Retrieve series bucket.
			b := txn.Bucket(u32tob(itr.seriesID))
			if b == nil {
				continue
			}

			// Add cursor.
			itr.cur.cursors = append(itr.cur.cursors, b.Cursor())
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

	// Close every transaction.
	for _, txn := range tx.txns {
		_ = txn.Rollback()
	}
	tx.txns = make(map[uint64]*bolt.Tx)

	return nil
}

// CreateIterators returns an iterator for a simple select statement.
func (tx *tx) CreateIterators(stmt *influxql.SelectStatement) ([]influxql.Iterator, error) {
	// Parse the source segments.
	database, policyName, measurement, err := splitIdent(stmt.Source.(*influxql.Measurement).Name)
	warn(">", database, policyName, measurement, err)
	if err != nil {
		return nil, err
	}

	// Normalize dimensions to extract the interval.
	interval, dimensions, err := stmt.Dimensions.Normalize()
	warn("dim:", interval, dimensions, err)
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

	// Expand conditions.
	tagSetExprs := m.expandExpr(stmt.Condition)
	for i, tagSetExpr := range tagSetExprs {
		warn(">", i, tagSetExpr.expr.String())
	}

	// Create an iterator for each condition.
	// Find all series ids that match the condition.
	var itrs []influxql.Iterator
	for _, tagSetExpr := range tagSetExprs {
		a, err := tx.createExprIterators(m, f.ID, interval, dimensions, tagSetExpr.values, tagSetExpr.expr)
		if err != nil {
			return nil, fmt.Errorf("create iterators(%s): %s", tagSetExpr.expr, err)
		} else if len(a) == 0 {
			continue
		}
		itrs = append(itrs, a...)
	}

	return itrs, nil
}

// createIterator returns an iterator for a single tagset and expression.
func (tx *tx) createExprIterators(m *Measurement, fieldID uint8, interval time.Duration, dimensions []string, tagExprs []tagExpr, expr influxql.Expr) ([]influxql.Iterator, error) {
	// Require a lower time bound on the condition.
	// Add an upper time bound if one does not exist.
	tmin, tmax := influxql.TimeRange(expr)
	if tmin.IsZero() {
		return nil, errors.New("statement must have a lower time bound")
	} else if tmax.IsZero() {
		tmax = tx.now

		// Create an 'AND time <= now()'.
		cond := &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "time"},
			RHS: &influxql.TimeLiteral{Val: tx.now},
			Op:  influxql.LTE,
		}

		// Set or append onto existing condition.
		if expr == nil {
			expr = cond
		} else {
			expr = &influxql.BinaryExpr{LHS: expr, RHS: cond, Op: influxql.AND}
		}
	}

	// Find the intersection of series ids for each tag key value.
	var ids seriesIDs
	for _, tagExpr := range tagExprs {
		a := tx.seriesIDsByTagExpr(m, tagExpr)
		if ids == nil {
			ids = a
		} else {
			ids = ids.intersect(a)
		}
	}
	warn("ids>", tagExprs, ids)

	// Return no iterator if there are no series.
	if len(ids) == 0 {
		return nil, nil
	}

	// Create an iterator for each series.
	var itrs []influxql.Iterator
	for _, seriesID := range ids {
		itr := &seriesIterator{
			seriesID:  seriesID,
			fieldID:   fieldID,
			condition: expr,
			cur: &multiCursor{
				kmin: u64tob(uint64(tmin.UnixNano())),
				kmax: u64tob(uint64(tmax.UnixNano())),
			},
		}

		// Lookup shards.
		shards := tx.server.shardsBySeriesID[seriesID]
		for _, sh := range shards {
			itr.shardIDs = append(itr.shardIDs, sh.ID)
			if tx.dbs[sh.ID] == nil {
				tx.dbs[sh.ID] = sh.store
			}
		}

		// Track series iterators on tx so their cursors can be opened.
		itrs = append(itrs, itr)
		tx.itrs = append(tx.itrs, itr)
	}

	return itrs, nil
}

// seriesIDsByTagExpr returns a list of series identifiers for a tag expression.
func (tx *tx) seriesIDsByTagExpr(m *Measurement, tagExpr tagExpr) seriesIDs {
	idx := m.seriesByTagKeyValue[tagExpr.key]
	if idx == nil {
		return nil
	}

	var a seriesIDs
	switch tagExpr.op {
	case influxql.EQ:
		for _, value := range tagExpr.values {
			a = a.union(idx[value])
		}
	case influxql.NEQ:
		a = m.seriesIDs
		for _, value := range tagExpr.values {
			a = a.reject(idx[value])
		}
	default:
		panic("unreachable")
	}

	return a
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

// seriesIterator represents an iterator for traversing over a single series.
type seriesIterator struct {
	seriesID  uint32
	fieldID   uint8
	tags      string // encoded dimensional tag values
	condition influxql.Expr

	shardIDs []uint64
	cur      *multiCursor
}

func (i *seriesIterator) Tags() string { return i.tags }

func (i *seriesIterator) Next() (key int64, value interface{}) {
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

type multiCursor struct {
	cursors     []*bolt.Cursor
	index       int
	initialized bool
	kmin, kmax  []byte // min/max keys
}

func (c *multiCursor) Next() (key, value []byte) {
	for {
		// Exit if no more cursors remain.
		if c.index >= len(c.cursors) {
			return nil, nil
		}

		// Move to the first or next key.
		if !c.initialized {
			key, value = c.cursors[c.index].Seek(c.kmin)
			c.initialized = true
		} else {
			key, value = c.cursors[c.index].Next()
		}

		// Clear key if it's above max allowed.
		if bytes.Compare(key, c.kmax) == 1 {
			key, value = nil, nil
		}

		// If there is no key then increment the index and retry.
		if key == nil {
			c.index++
			c.initialized = false
			continue
		}

		return key, value
	}
}
