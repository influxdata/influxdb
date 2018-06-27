package storage

import (
	"context"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

type ResultSet struct {
	cur seriesCursor
	row seriesRow
	ci  CursorIterator
}

func newResultSet(ctx context.Context, req *ReadRequest, cur seriesCursor) *ResultSet {
	return &ResultSet{
		cur: cur,
		ci: CursorIterator{
			ctx: ctx,
			req: tsdb.CursorRequest{
				Ascending: true,
				StartTime: req.Start,
				EndTime:   req.End,
			},
		},
	}
}

func (r *ResultSet) Close() {
	r.row.query = nil
	r.cur.Close()
}

// Next moves to the result set forward to the next series key.
func (r *ResultSet) Next() bool {
	row := r.cur.Next()
	if row == nil {
		return false
	}

	r.row = *row

	return true
}

func (r *ResultSet) Name() []byte                 { return r.row.name }
func (r *ResultSet) Tags() models.Tags            { return r.row.tags }
func (r *ResultSet) Field() []byte                { return []byte(r.row.field.n) }
func (r *ResultSet) FieldType() influxql.DataType { return r.row.field.d }

func (r *ResultSet) CursorIterator() *CursorIterator {
	r.ci.req.Name = r.row.name
	r.ci.req.Tags = r.row.tags
	r.ci.req.Field = r.row.field.n
	r.ci.itrs = r.row.query

	return &r.ci
}

type CursorIterator struct {
	ctx  context.Context
	req  tsdb.CursorRequest
	itrs tsdb.CursorIterators
	cur  tsdb.Cursor
}

func (ci *CursorIterator) Next() bool {
	if len(ci.itrs) == 0 {
		return false
	}

	var shard tsdb.CursorIterator
	ci.cur = nil
	for ci.cur == nil && len(ci.itrs) > 0 {
		shard, ci.itrs = ci.itrs[0], ci.itrs[1:]
		ci.cur, _ = shard.Next(ci.ctx, &ci.req)
	}

	return ci.cur != nil
}

func (ci *CursorIterator) Cursor() tsdb.Cursor {
	return ci.cur
}
