package tsm1

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/pkg/metrics"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

func (e *Engine) CreateCursor(ctx context.Context, r *tsdb.CursorRequest) (tsdb.Cursor, error) {
	// Look up fields for measurement.
	mf := e.fieldset.Fields(r.Measurement)
	if mf == nil {
		return nil, nil
	}

	// Find individual field.
	f := mf.Field(r.Field)
	if f == nil {
		// field doesn't exist for this measurement
		return nil, nil
	}

	if grp := metrics.GroupFromContext(ctx); grp != nil {
		grp.GetCounter(numberOfRefCursorsCounter).Add(1)
	}

	var opt query.IteratorOptions
	opt.Ascending = r.Ascending
	opt.StartTime = r.StartTime
	opt.EndTime = r.EndTime
	var t int64
	if r.Ascending {
		t = r.EndTime
	} else {
		t = r.StartTime
	}

	// Return appropriate cursor based on type.
	switch f.Type {
	case influxql.Float:
		return newFloatRangeBatchCursor(t, r.Ascending, e.buildFloatBatchCursor(ctx, r.Measurement, r.Series, r.Field, opt)), nil
	case influxql.Integer:
		return newIntegerRangeBatchCursor(t, r.Ascending, e.buildIntegerBatchCursor(ctx, r.Measurement, r.Series, r.Field, opt)), nil
	case influxql.Unsigned:
		return newUnsignedRangeBatchCursor(t, r.Ascending, e.buildUnsignedBatchCursor(ctx, r.Measurement, r.Series, r.Field, opt)), nil
	case influxql.String:
		return newStringRangeBatchCursor(t, r.Ascending, e.buildStringBatchCursor(ctx, r.Measurement, r.Series, r.Field, opt)), nil
	case influxql.Boolean:
		return newBooleanRangeBatchCursor(t, r.Ascending, e.buildBooleanBatchCursor(ctx, r.Measurement, r.Series, r.Field, opt)), nil
	default:
		panic(fmt.Sprintf("unreachable: %T", f.Type))
	}
}
