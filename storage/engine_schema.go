package storage

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
)

// TagKeys returns an iterator where the values are tag keys for the bucket
// matching the predicate within the time range (start, end].
func (e *Engine) TagKeys(ctx context.Context, orgID, bucketID influxdb.ID, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return cursors.EmptyStringIterator, nil
	}

	return e.engine.TagKeys(ctx, orgID, bucketID, start, end, predicate)
}

// TagValues returns an iterator which enumerates the values for the specific
// tagKey in the given bucket matching the predicate within the
// time range (start, end].
func (e *Engine) TagValues(ctx context.Context, orgID, bucketID influxdb.ID, tagKey string, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return cursors.EmptyStringIterator, nil
	}

	return e.engine.TagValues(ctx, orgID, bucketID, tagKey, start, end, predicate)
}
