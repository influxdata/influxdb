package reads

import (
	"context"
	"math"

	"github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

type windowAggregateResultSet struct {
	ctx          context.Context
	req          *datatypes.ReadWindowAggregateRequest
	cursor       SeriesCursor
	seriesRow    *SeriesRow
	arrayCursors *arrayCursors
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

	ascending := true

	// The following is an optimization where in the case of a single window,
	// the selector `last` is implemented as a descending array cursor followed
	// by a limit array cursor that selects only the first point, i.e the point
	// with the largest timestamp, from the descending array cursor.
	//
	if req.Aggregate[0].Type == datatypes.AggregateTypeLast && (req.WindowEvery == 0 || req.WindowEvery == math.MaxInt64) {
		ascending = false
	}

	results := &windowAggregateResultSet{
		ctx:          ctx,
		req:          req,
		cursor:       cursor,
		arrayCursors: newArrayCursors(ctx, req.Range.Start, req.Range.End, ascending),
	}
	return results, nil
}

func (r *windowAggregateResultSet) Next() bool {
	if r == nil {
		return false
	}
	r.seriesRow = r.cursor.Next()
	return r.seriesRow != nil
}

func (r *windowAggregateResultSet) Cursor() cursors.Cursor {
	agg := r.req.Aggregate[0]
	every := r.req.WindowEvery
	offset := r.req.Offset
	cursor := r.arrayCursors.createCursor(*r.seriesRow)

	if every == math.MaxInt64 {
		// This means to aggregate over whole series for the query's time range
		return newAggregateArrayCursor(r.ctx, agg, cursor)
	} else {
		return newWindowAggregateArrayCursor(r.ctx, agg, every, offset, cursor)
	}
}

func (r *windowAggregateResultSet) Close() {}

func (r *windowAggregateResultSet) Err() error { return nil }

func (r *windowAggregateResultSet) Stats() cursors.CursorStats {
	if r.seriesRow == nil || r.seriesRow.Query == nil {
		return cursors.CursorStats{}
	}
	return r.seriesRow.Query.Stats()
}

func (r *windowAggregateResultSet) Tags() models.Tags {
	if r.seriesRow == nil {
		return models.Tags{}
	}
	return r.seriesRow.Tags
}
