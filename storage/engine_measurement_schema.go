package storage

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/influxdata/influxql"
)

// MeasurementNames returns an iterator which enumerates the measurements for the given
// bucket and limited to the time range [start, end].
//
// MeasurementNames will always return a StringIterator if there is no error.
//
// If the context is canceled before MeasurementNames has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementNames(ctx context.Context, orgID, bucketID influxdb.ID, start, end int64) (cursors.StringIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return cursors.EmptyStringIterator, nil
	}

	return e.engine.MeasurementNames(ctx, orgID, bucketID, start, end)
}

// MeasurementTagValues returns an iterator which enumerates the tag values for the given
// bucket, measurement and tag key, filtered using the optional the predicate and limited to the
// time range [start, end].
//
// MeasurementTagValues will always return a StringIterator if there is no error.
//
// If the context is canceled before TagValues has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementTagValues(ctx context.Context, orgID, bucketID influxdb.ID, measurement, tagKey string, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return cursors.EmptyStringIterator, nil
	}

	return e.engine.MeasurementTagValues(ctx, orgID, bucketID, measurement, tagKey, start, end, predicate)
}

// MeasurementTagKeys returns an iterator which enumerates the tag keys for the given
// bucket and measurement, filtered using the optional the predicate and limited to the
// time range [start, end].
//
// MeasurementTagKeys will always return a StringIterator if there is no error.
//
// If the context is canceled before MeasurementTagKeys has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementTagKeys(ctx context.Context, orgID, bucketID influxdb.ID, measurement string, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return cursors.EmptyStringIterator, nil
	}

	return e.engine.MeasurementTagKeys(ctx, orgID, bucketID, measurement, start, end, predicate)
}

// MeasurementFields returns an iterator which enumerates the field schema for the given
// bucket and measurement, filtered using the optional the predicate and limited to the
// time range [start, end].
//
// MeasurementFields will always return a MeasurementFieldsIterator if there is no error.
//
// If the context is canceled before MeasurementFields has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementFields(ctx context.Context, orgID, bucketID influxdb.ID, measurement string, start, end int64, predicate influxql.Expr) (cursors.MeasurementFieldsIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return cursors.EmptyMeasurementFieldsIterator, nil
	}

	return e.engine.MeasurementFields(ctx, orgID, bucketID, measurement, start, end, predicate)
}
