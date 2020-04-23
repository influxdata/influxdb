package storage

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/influxdata/influxql"
)

// MeasurementNamesNoTime returns an iterator which enumerates the measurements for the given
// bucket.
//
// MeasurementNamesNoTime will always return a StringIterator if there is no error.
//
// If the context is canceled before MeasurementNamesNoTime has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementNamesNoTime(ctx context.Context, orgID, bucketID influxdb.ID, predicate influxql.Expr) (cursors.StringIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return cursors.EmptyStringIterator, nil
	}

	// TODO - hook up to store meta api
	return nil, nil
}

// MeasurementTagKeysNoTime returns an iterator which enumerates the tag keys
// for the given bucket, measurement and tag key and filtered using the optional
// the predicate.
//
// MeasurementTagKeysNoTime will always return a StringIterator if there is no error.
//
// If the context is canceled before MeasurementTagKeysNoTime has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementTagKeysNoTime(ctx context.Context, orgID, bucketID influxdb.ID, measurement, tagKey string, predicate influxql.Expr) (cursors.StringIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return cursors.EmptyStringIterator, nil
	}

	// TODO - hook up to store meta api
	return nil, nil
}

// MeasurementTagValuesNoTime returns an iterator which enumerates the tag values for the given
// bucket, measurement and tag key and filtered using the optional the predicate.
//
// MeasurementTagValuesNoTime will always return a StringIterator if there is no error.
//
// If the context is canceled before MeasurementTagValuesNoTime has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementTagValuesNoTime(ctx context.Context, orgID, bucketID influxdb.ID, measurement, tagKey string, predicate influxql.Expr) (cursors.StringIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return cursors.EmptyStringIterator, nil
	}

	// TODO - hook up to store meta api
	return nil, nil
}

// MeasurementFieldsNoTime returns an iterator which enumerates the field schema for the given
// bucket and measurement, filtered using the optional the predicate.
//
// MeasurementFieldsNoTime will always return a MeasurementFieldsIterator if there is no error.
//
// If the context is canceled before MeasurementFieldsNoTime has finished processing, a non-nil
// error will be returned along with statistics for the already scanned data.
func (e *Engine) MeasurementFieldsNoTime(ctx context.Context, orgID, bucketID influxdb.ID, measurement string, predicate influxql.Expr) (cursors.MeasurementFieldsIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return cursors.EmptyMeasurementFieldsIterator, nil
	}

	// TODO - hook up to store meta api
	return nil, nil
}
