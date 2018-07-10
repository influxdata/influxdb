package storage

import (
	"context"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

type readRequest struct {
	ctx        context.Context
	start, end int64
	asc        bool
	limit      int64
	aggregate  *Aggregate
}

type multiShardCursors interface {
	createCursor(row seriesRow) tsdb.Cursor
	newAggregateCursor(ctx context.Context, agg *Aggregate, cursor tsdb.Cursor) tsdb.Cursor
}

type resultSet struct {
	req readRequest
	cur seriesCursor
	row seriesRow
	mb  multiShardCursors
}

// Close closes the result set. Close is idempotent.
func (r *resultSet) Close() {
	if r == nil {
		return // Nothing to do.
	}
	r.row.query = nil
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
	if r.req.aggregate != nil {
		cur = r.mb.newAggregateCursor(r.req.ctx, r.req.aggregate, cur)
	}
	return cur
}

func (r *resultSet) Tags() models.Tags {
	return r.row.tags
}
