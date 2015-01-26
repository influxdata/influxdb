package influxdb

import (
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
		txn := tx.txns[itr.shardID]

		// Retrieve series bucket.
		b := txn.Bucket(u32tob(itr.seriesID))
		if b == nil {
			continue
		}

		// Create cursor.
		itr.cur = b.Cursor()
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
			tmax:      tmax.UnixNano(),
		}

		// Lookup shard.
		sh := tx.server.shardsBySeriesID[seriesID]
		if sh != nil {
			itr.shardID = sh.ID
			tx.dbs[sh.ID] = sh.store
		}

		itrs = append(itrs, itr)

		// Track series iterators on tx so their cursors can be opened.
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
	shardID     uint64
	seriesID    uint32
	fieldID     uint8
	tags        string // encoded dimensional tag values
	condition   influxql.Expr
	tmin, tmax  int64
	initialized bool
	cur         *bolt.Cursor
}

func (i *seriesIterator) Tags() string { return i.tags }

func (i *seriesIterator) Next() (key int64, value interface{}) {
	if i.cur == nil {
		return 0, nil
	}

	// If not run yet then seek to the first key.
	// Otherwise retrieve next key.
	var k, v []byte
	if !i.initialized {
		k, v = i.cur.Seek(u64tob(uint64(i.tmin)))
		i.initialized = true
	} else {
		k, v = i.cur.Next()
	}

	// Ignore if there's no more data.
	if k == nil {
		return 0, nil
	}

	// Read timestamp. Ignore if greater than tmax.
	key = int64(btou64(k))
	if i.tmax != 0 && key > i.tmax {
		return 0, nil
	}

	// Read field value.
	value = unmarshalValue(v, i.fieldID)
	warn("!", key, value)
	return key, value
}
