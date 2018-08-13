package storage

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/tsdb"
)

type singleValue struct {
	v interface{}
}

func (v *singleValue) Value(key string) (interface{}, bool) {
	return v.v, true
}

func newAggregateArrayCursor(ctx context.Context, agg *Aggregate, cursor tsdb.Cursor) tsdb.Cursor {
	if cursor == nil {
		return nil
	}

	switch agg.Type {
	case AggregateTypeSum:
		return newSumArrayCursor(cursor)
	case AggregateTypeCount:
		return newCountArrayCursor(cursor)
	default:
		// TODO(sgc): should be validated higher up
		panic("invalid aggregate")
	}
}

func newSumArrayCursor(cur tsdb.Cursor) tsdb.Cursor {
	switch cur := cur.(type) {
	case tsdb.FloatArrayCursor:
		return newFloatArraySumCursor(cur)
	case tsdb.IntegerArrayCursor:
		return newIntegerArraySumCursor(cur)
	case tsdb.UnsignedArrayCursor:
		return newUnsignedArraySumCursor(cur)
	default:
		// TODO(sgc): propagate an error instead?
		return nil
	}
}

func newCountArrayCursor(cur tsdb.Cursor) tsdb.Cursor {
	switch cur := cur.(type) {
	case tsdb.FloatArrayCursor:
		return &integerFloatCountArrayCursor{FloatArrayCursor: cur}
	case tsdb.IntegerArrayCursor:
		return &integerIntegerCountArrayCursor{IntegerArrayCursor: cur}
	case tsdb.UnsignedArrayCursor:
		return &integerUnsignedCountArrayCursor{UnsignedArrayCursor: cur}
	case tsdb.StringArrayCursor:
		return &integerStringCountArrayCursor{StringArrayCursor: cur}
	case tsdb.BooleanArrayCursor:
		return &integerBooleanCountArrayCursor{BooleanArrayCursor: cur}
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
}

type cursorContext struct {
	ctx   context.Context
	req   *tsdb.CursorRequest
	itrs  tsdb.CursorIterators
	limit int64
	count int64
	err   error
}

type multiShardArrayCursors struct {
	ctx   context.Context
	limit int64
	req   tsdb.CursorRequest

	cursors struct {
		i integerMultiShardArrayCursor
		f floatMultiShardArrayCursor
		u unsignedMultiShardArrayCursor
		b booleanMultiShardArrayCursor
		s stringMultiShardArrayCursor
	}
}

func newMultiShardArrayCursors(ctx context.Context, rr *readRequest) *multiShardArrayCursors {
	lim := rr.limit
	if lim < 0 {
		lim = 1
	}

	m := &multiShardArrayCursors{
		ctx:   ctx,
		limit: lim,
		req: tsdb.CursorRequest{
			Ascending: rr.asc,
			StartTime: rr.start,
			EndTime:   rr.end,
		},
	}

	cc := cursorContext{
		ctx:   ctx,
		limit: lim,
		req:   &m.req,
	}

	m.cursors.i.cursorContext = cc
	m.cursors.f.cursorContext = cc
	m.cursors.u.cursorContext = cc
	m.cursors.b.cursorContext = cc
	m.cursors.s.cursorContext = cc

	return m
}

func (m *multiShardArrayCursors) createCursor(row seriesRow) tsdb.Cursor {
	m.req.Name = row.name
	m.req.Tags = row.stags
	m.req.Field = row.field.n

	var cond expression
	if row.valueCond != nil {
		cond = &astExpr{row.valueCond}
	}

	var shard tsdb.CursorIterator
	var cur tsdb.Cursor
	for cur == nil && len(row.query) > 0 {
		shard, row.query = row.query[0], row.query[1:]
		cur, _ = shard.Next(m.ctx, &m.req)
	}

	if cur == nil {
		return nil
	}

	switch c := cur.(type) {
	case tsdb.IntegerArrayCursor:
		m.cursors.i.reset(c, row.query, cond)
		return &m.cursors.i
	case tsdb.FloatArrayCursor:
		m.cursors.f.reset(c, row.query, cond)
		return &m.cursors.f
	case tsdb.UnsignedArrayCursor:
		m.cursors.u.reset(c, row.query, cond)
		return &m.cursors.u
	case tsdb.StringArrayCursor:
		m.cursors.s.reset(c, row.query, cond)
		return &m.cursors.s
	case tsdb.BooleanArrayCursor:
		m.cursors.b.reset(c, row.query, cond)
		return &m.cursors.b
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
}

func (m *multiShardArrayCursors) newAggregateCursor(ctx context.Context, agg *Aggregate, cursor tsdb.Cursor) tsdb.Cursor {
	return newAggregateArrayCursor(ctx, agg, cursor)
}
