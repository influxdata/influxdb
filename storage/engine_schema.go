package storage

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
)

// TagKeys returns an iterator where the values are tag keys for the bucket
// matching the predicate within the time range (start, end].
func (e *Engine) TagKeys(ctx context.Context, orgID, bucketID influxdb.ID, start, end int64, predicate influxql.Expr) cursors.StringIterator {
	// This method would be invoked when the consumer wants to get the following schema information for an arbitrary
	// time range in a single bucket:
	//
	// * All tag keys;
	// * All tag keys filtered by an arbitrary predicate, e.g., tag keys for a measurement or tag keys appearing alongside another tag key pair.
	//
	return cursors.EmptyStringIterator
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

	// This method would be invoked when the consumer wants to get the following schema information for an arbitrary
	// time range in a single bucket:
	//
	// * All measurement names, i.e. tagKey == _measurement);
	// * All field names for a specific measurement using a predicate
	//     * i.e. tagKey is "_field", predicate _measurement == "<measurement>"
	//
}
