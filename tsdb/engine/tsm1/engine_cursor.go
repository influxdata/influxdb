package tsm1

import (
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
)

func (e *Engine) CreateCursor(r *tsdb.CursorRequest) (tsdb.Cursor, error) {
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
		return newFloatRangeCursor(r.Series, t, r.Ascending, e.buildFloatCursor(r.Measurement, r.Series, r.Field, opt)), nil

	case influxql.Integer:
		return newIntegerRangeCursor(r.Series, t, r.Ascending, e.buildIntegerCursor(r.Measurement, r.Series, r.Field, opt)), nil

	case influxql.Unsigned:
		return nil, nil

	case influxql.String:
		return nil, nil

	case influxql.Boolean:
		return nil, nil

	default:
		panic("unreachable")
	}
}
