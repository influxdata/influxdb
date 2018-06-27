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

type ResultSet struct {
	req readRequest
	cur seriesCursor
	row seriesRow
	mb  *multiShardBatchCursors
}

// Close closes the result set. Close is idempotent.
func (r *ResultSet) Close() {
	if r == nil {
		return // Nothing to do.
	}
	r.row.query = nil
	r.cur.Close()
}

// Next returns true if there are more results available.
func (r *ResultSet) Next() bool {
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

func (r *ResultSet) Cursor() tsdb.Cursor {
	cur := r.mb.createCursor(r.row)
	if r.req.aggregate != nil {
		cur = newAggregateBatchCursor(r.req.ctx, r.req.aggregate, cur)
	}
	return cur
}

func (r *ResultSet) Tags() models.Tags {
	return r.row.tags
}
