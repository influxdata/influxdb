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

func newMultiShardArrayCursors(ctx context.Context, start, end int64, asc bool, limit int64) *multiShardArrayCursors {
	if limit < 0 {
		limit = 1
	}

	m := &multiShardArrayCursors{
		ctx:   ctx,
		limit: limit,
		req: tsdb.CursorRequest{
			Ascending: asc,
			StartTime: start,
			EndTime:   end,
		},
	}

	cc := cursorContext{
		ctx:   ctx,
		limit: limit,
		req:   &m.req,
	}

	m.cursors.i.cursorContext = cc
	m.cursors.f.cursorContext = cc
	m.cursors.u.cursorContext = cc
	m.cursors.b.cursorContext = cc
	m.cursors.s.cursorContext = cc

	return m
}

func (m *multiShardArrayCursors) createCursor(row SeriesRow) tsdb.Cursor {
	m.req.Name = row.Name
	m.req.Tags = row.SeriesTags
	m.req.Field = row.Field

	var cond expression
	if row.ValueCond != nil {
		cond = &astExpr{row.ValueCond}
	}

	var shard tsdb.CursorIterator
	var cur tsdb.Cursor
	for cur == nil && len(row.Query) > 0 {
		shard, row.Query = row.Query[0], row.Query[1:]
		cur, _ = shard.Next(m.ctx, &m.req)
	}

	if cur == nil {
		return nil
	}

	switch c := cur.(type) {
	case tsdb.IntegerArrayCursor:
		m.cursors.i.reset(c, row.Query, cond)
		return &m.cursors.i
	case tsdb.FloatArrayCursor:
		m.cursors.f.reset(c, row.Query, cond)
		return &m.cursors.f
	case tsdb.UnsignedArrayCursor:
		m.cursors.u.reset(c, row.Query, cond)
		return &m.cursors.u
	case tsdb.StringArrayCursor:
		m.cursors.s.reset(c, row.Query, cond)
		return &m.cursors.s
	case tsdb.BooleanArrayCursor:
		m.cursors.b.reset(c, row.Query, cond)
		return &m.cursors.b
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
}

func (m *multiShardArrayCursors) newAggregateCursor(ctx context.Context, agg *Aggregate, cursor tsdb.Cursor) tsdb.Cursor {
	return newAggregateArrayCursor(ctx, agg, cursor)
}
