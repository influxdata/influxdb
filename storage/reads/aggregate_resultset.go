package reads

import (
	"context"
	"math"

	"github.com/influxdata/flux/interval"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

type windowAggregateResultSet struct {
	ctx          context.Context
	req          *datatypes.ReadWindowAggregateRequest
	seriesCursor SeriesCursor
	seriesRow    SeriesRow
	arrayCursors multiShardCursors
	cursor       cursors.Cursor
	err          error
}

// IsLastDescendingAggregateOptimization checks two things: If the request passed in
// is using the `last` aggregate type, and if it doesn't have a window. If both
// conditions are met, it returns false, otherwise, it returns true.
func IsLastDescendingAggregateOptimization(req *datatypes.ReadWindowAggregateRequest) bool {
	if len(req.Aggregate) != 1 {
		// Descending optimization for last only applies when it is the only aggregate.
		return false
	}

	// The following is an optimization where in the case of a single window,
	// the selector `last` is implemented as a descending array cursor followed
	// by a limit array cursor that selects only the first point, i.e the point
	// with the largest timestamp, from the descending array cursor.
	if req.Aggregate[0].Type == datatypes.AggregateTypeLast {
		if req.Window == nil {
			if req.WindowEvery == 0 || req.WindowEvery == math.MaxInt64 {
				return true
			}
		} else if (req.Window.Every.Nsecs == 0 && req.Window.Every.Months == 0) || req.Window.Every.Nsecs == math.MaxInt64 {
			return true
		}
	}
	return false
}

func NewWindowAggregateResultSet(ctx context.Context, req *datatypes.ReadWindowAggregateRequest, cursor SeriesCursor) (ResultSet, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	span.LogKV("aggregate_window_every", req.WindowEvery)
	for _, aggregate := range req.Aggregate {
		span.LogKV("aggregate_type", aggregate.String())
	}

	if nAggs := len(req.Aggregate); nAggs != 1 {
		return nil, errors.Errorf(errors.InternalError, "attempt to create a windowAggregateResultSet with %v aggregate functions", nAggs)
	}

	ascending := !IsLastDescendingAggregateOptimization(req)
	results := &windowAggregateResultSet{
		ctx:          ctx,
		req:          req,
		seriesCursor: cursor,
		arrayCursors: newMultiShardArrayCursors(ctx, req.Range.Start, req.Range.End, ascending),
	}
	return results, nil
}

func (r *windowAggregateResultSet) Next() bool {
	if r == nil || r.err != nil {
		return false
	}

	seriesRow := r.seriesCursor.Next()
	if seriesRow == nil {
		return false
	}
	r.seriesRow = *seriesRow
	r.cursor, r.err = r.createCursor(r.seriesRow)
	return r.err == nil
}

func convertNsecs(nsecs int64) values.Duration {
	negative := false
	if nsecs < 0 {
		negative, nsecs = true, -nsecs
	}
	return values.MakeDuration(nsecs, 0, negative)
}

func (r *windowAggregateResultSet) createCursor(seriesRow SeriesRow) (cursors.Cursor, error) {
	agg := r.req.Aggregate[0]
	every := r.req.WindowEvery
	offset := r.req.Offset
	cursor := r.arrayCursors.createCursor(seriesRow)

	var everyDur values.Duration
	var offsetDur values.Duration
	var periodDur values.Duration

	if r.req.Window != nil {
		// assume window was passed in and translate protobuf window to execute.Window
		everyDur = values.MakeDuration(r.req.Window.Every.Nsecs, r.req.Window.Every.Months, r.req.Window.Every.Negative)
		periodDur = values.MakeDuration(r.req.Window.Every.Nsecs, r.req.Window.Every.Months, r.req.Window.Every.Negative)
		if r.req.Window.Offset != nil {
			offsetDur = values.MakeDuration(r.req.Window.Offset.Nsecs, r.req.Window.Offset.Months, r.req.Window.Offset.Negative)
		} else {
			offsetDur = values.MakeDuration(0, 0, false)
		}
	} else {
		// nanosecond values were passed in and need to be converted to windows
		everyDur = convertNsecs(every)
		periodDur = convertNsecs(every)
		offsetDur = convertNsecs(offset)
	}

	window, err := interval.NewWindow(everyDur, periodDur, offsetDur)
	if err != nil {
		return nil, err
	}

	if everyDur.Nanoseconds() == math.MaxInt64 {
		// This means to aggregate over whole series for the query's time range
		return newAggregateArrayCursor(r.ctx, agg, cursor)
	} else {
		return newWindowAggregateArrayCursor(r.ctx, agg, window, cursor)
	}
}

func (r *windowAggregateResultSet) Cursor() cursors.Cursor {
	return r.cursor
}

func (r *windowAggregateResultSet) Close() {
	if r == nil {
		return
	}
	r.seriesRow.Query = nil
	r.seriesCursor.Close()
}

func (r *windowAggregateResultSet) Err() error { return r.err }

func (r *windowAggregateResultSet) Stats() cursors.CursorStats {
	if r.seriesRow.Query == nil {
		return cursors.CursorStats{}
	}
	// See the equivalent method in *resultSet.Stats.
	return r.seriesRow.Query.Stats()
}

func (r *windowAggregateResultSet) Tags() models.Tags {
	return r.seriesRow.Tags
}
