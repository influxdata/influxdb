package storage

import (
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxql"
)

// StringIterator describes the behavior for enumerating a sequence of
// string values.
type StringIterator interface {
	// Next advances the StringIterator to the next value. It returns false
	// when there are no more values.
	Next() bool

	// Value returns the current value.
	Value() string
}

// TagKeys returns an iterator where the values are tag keys for the bucket
// matching the predicate within the time range (start, end].
func (e *Engine) TagKeys(orgID, bucketID influxdb.ID, start, end int64, predicate influxql.Expr) StringIterator {
	// This method would be invoked when the consumer wants to get the following schema information for an arbitrary
	// time range in a single bucket:
	//
	// * All tag keys;
	// * All tag keys filtered by an arbitrary predicate, e.g., tag keys for a measurement or tag keys appearing alongside another tag key pair.
	//
	return nullStringIterator
}

// TagValues returns an iterator which enumerates the values for the specific
// tagKey in the given bucket matching the predicate within the
// time range (start, end].
func (e *Engine) TagValues(orgID, bucketID influxdb.ID, tagKey string, start, end int64, predicate influxql.Expr) StringIterator {
	// This method would be invoked when the consumer wants to get the following schema information for an arbitrary
	// time range in a single bucket:
	//
	// * All measurement names, i.e. tagKey == _measurement);
	// * All field names for a specific measurement using a predicate
	//     * i.e. tagKey is "_field", predicate _measurement == "<measurement>"
	//
	return nullStringIterator
}

var nullStringIterator StringIterator = &emptyStringIterator{}

type emptyStringIterator struct{}

func (*emptyStringIterator) Next() bool    { return false }
func (*emptyStringIterator) Value() string { return "" }
