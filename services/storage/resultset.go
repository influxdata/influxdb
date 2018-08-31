package storage

import (
	"context"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

type multiShardCursors interface {
	createCursor(row SeriesRow) tsdb.Cursor
	newAggregateCursor(ctx context.Context, agg *Aggregate, cursor tsdb.Cursor) tsdb.Cursor
}

type resultSet struct {
	ctx context.Context
	agg *Aggregate
	cur SeriesCursor
	row SeriesRow
	mb  multiShardCursors
}

func NewResultSet(ctx context.Context, req *ReadRequest, cur SeriesCursor) ResultSet {
	return &resultSet{
		ctx: ctx,
		agg: req.Aggregate,
		cur: cur,
		mb:  newMultiShardArrayCursors(ctx, req.TimestampRange.Start, req.TimestampRange.End, !req.Descending, req.PointsLimit),
	}
}

// Close closes the result set. Close is idempotent.
func (r *resultSet) Close() {
	if r == nil {
		return // Nothing to do.
	}
	r.row.Query = nil
	r.cur.Close()
}

// Next returns true if there are more results available.
func (r *resultSet) Next() bool {
	if r == nil {
		return false
	}

	row := r.cur.Next()
	if row == nil {
		return false
	}

	r.row = *row

	return true
}

func (r *resultSet) Cursor() tsdb.Cursor {
	cur := r.mb.createCursor(r.row)
	if r.agg != nil {
		cur = r.mb.newAggregateCursor(r.ctx, r.agg, cur)
	}
	return cur
}

func (r *resultSet) Tags() models.Tags {
	return r.row.Tags
}
