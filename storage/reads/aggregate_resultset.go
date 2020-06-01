package reads

import (
	"context"

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

	for _, aggregate := range req.Aggregate {
		span.LogKV("aggregate_type", aggregate.String())
		span.LogKV("aggregate_window_every", req.WindowEvery)
	}

	results := &windowAggregateResultSet{
		ctx:          ctx,
		req:          req,
		cursor:       cursor,
		arrayCursors: newArrayCursors(ctx, req.Range.Start, req.Range.End, true),
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
	cursor := r.arrayCursors.createCursor(*r.seriesRow)
	return newWindowAggregateArrayCursor(r.ctx, r.req, cursor)
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
