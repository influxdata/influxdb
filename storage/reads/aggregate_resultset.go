package reads

import (
	"context"
	"math"

	"github.com/influxdata/flux/interval"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/kit/errors"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
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

// IsAscendingWindowAggregate checks two things: If the request passed in
// is using the `last` aggregate type, and if it doesn't have a window. If both
// conditions are met, it returns false, otherwise, it returns true.
func IsAscendingWindowAggregate(req *datatypes.ReadWindowAggregateRequest) bool {
	if len(req.Aggregate) != 1 {
		// Descending optimization for last only applies when it is the only aggregate.
		return true
	}

	// The following is an optimization where in the case of a single window,
	// the selector `last` is implemented as a descending array cursor followed
	// by a limit array cursor that selects only the first point, i.e the point
	// with the largest timestamp, from the descending array cursor.
	if req.Aggregate[0].Type == datatypes.Aggregate_AggregateTypeLast {
		if req.Window == nil {
			if req.WindowEvery == 0 || req.WindowEvery == math.MaxInt64 {
				return false
			}
		} else if (req.Window.Every.Nsecs == 0 && req.Window.Every.Months == 0) || req.Window.Every.Nsecs == math.MaxInt64 {
			return false
		}
	}
	return true
}

func NewWindowAggregateResultSet(ctx context.Context, req *datatypes.ReadWindowAggregateRequest, cursor SeriesCursor) (ResultSet, error) {
	if nAggs := len(req.Aggregate); nAggs != 1 {
		if nAggs == 2 {
			if req.Aggregate[0].Type != datatypes.Aggregate_AggregateTypeMean || req.Aggregate[1].Type != datatypes.Aggregate_AggregateTypeCount {
				return nil, errors.Errorf(errors.InternalError, "attempt to create a windowAggregateResultSet with %v, %v aggregates", req.Aggregate[0].Type, req.Aggregate[1].Type)
			}
		} else {
			return nil, errors.Errorf(errors.InternalError, "attempt to create a windowAggregateResultSet with %v aggregate functions", nAggs)
		}
	}

	ascending := IsAscendingWindowAggregate(req)
	results := &windowAggregateResultSet{
		ctx:          ctx,
		req:          req,
		seriesCursor: cursor,
		arrayCursors: newMultiShardArrayCursors(ctx, req.Range.GetStart(), req.Range.GetEnd(), ascending),
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

func GetWindow(req *datatypes.ReadWindowAggregateRequest) (interval.Window, error) {
	var everyDur values.Duration
	var offsetDur values.Duration
	var periodDur values.Duration

	every := req.WindowEvery
	offset := req.Offset
	if req.Window != nil {
		// assume window was passed in and translate protobuf window to execute.Window
		everyDur = values.MakeDuration(req.Window.Every.Nsecs, req.Window.Every.Months, req.Window.Every.Negative)
		periodDur = values.MakeDuration(req.Window.Every.Nsecs, req.Window.Every.Months, req.Window.Every.Negative)
		if req.Window.Offset != nil {
			offsetDur = values.MakeDuration(req.Window.Offset.Nsecs, req.Window.Offset.Months, req.Window.Offset.Negative)
		} else {
			offsetDur = values.MakeDuration(0, 0, false)
		}
	} else {
		// nanosecond values were passed in and need to be converted to windows
		everyDur = convertNsecs(every)
		periodDur = convertNsecs(every)
		offsetDur = convertNsecs(offset)
	}

	return interval.NewWindow(everyDur, periodDur, offsetDur)
}

func (r *windowAggregateResultSet) createCursor(seriesRow SeriesRow) (cursors.Cursor, error) {

	cursor := r.arrayCursors.createCursor(seriesRow)

	window, err := GetWindow(r.req)
	if err != nil {
		return nil, err
	}

	if window.Every().Nanoseconds() == math.MaxInt64 {
		// This means to aggregate over whole series for the query's time range
		return newAggregateArrayCursor(r.ctx, r.req.Aggregate, cursor)
	} else {
		return NewWindowAggregateArrayCursor(r.ctx, r.req.Aggregate, window, cursor)
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
