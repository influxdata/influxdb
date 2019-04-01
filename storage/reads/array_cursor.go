package reads

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type singleValue struct {
	v interface{}
}

func (v *singleValue) Value(key string) (interface{}, bool) {
	return v.v, true
}

func newAggregateArrayCursor(ctx context.Context, agg *datatypes.Aggregate, cursor cursors.Cursor) cursors.Cursor {
	if cursor == nil {
		return nil
	}

	switch agg.Type {
	case datatypes.AggregateTypeSum:
		return newSumArrayCursor(cursor)
	case datatypes.AggregateTypeCount:
		return newCountArrayCursor(cursor)
	default:
		// TODO(sgc): should be validated higher up
		panic("invalid aggregate")
	}
}

func newSumArrayCursor(cur cursors.Cursor) cursors.Cursor {
	switch cur := cur.(type) {
	case cursors.FloatArrayCursor:
		return newFloatArraySumCursor(cur)
	case cursors.IntegerArrayCursor:
		return newIntegerArraySumCursor(cur)
	case cursors.UnsignedArrayCursor:
		return newUnsignedArraySumCursor(cur)
	default:
		// TODO(sgc): propagate an error instead?
		return nil
	}
}

func newCountArrayCursor(cur cursors.Cursor) cursors.Cursor {
	switch cur := cur.(type) {
	case cursors.FloatArrayCursor:
		return &integerFloatCountArrayCursor{FloatArrayCursor: cur}
	case cursors.IntegerArrayCursor:
		return &integerIntegerCountArrayCursor{IntegerArrayCursor: cur}
	case cursors.UnsignedArrayCursor:
		return &integerUnsignedCountArrayCursor{UnsignedArrayCursor: cur}
	case cursors.StringArrayCursor:
		return &integerStringCountArrayCursor{StringArrayCursor: cur}
	case cursors.BooleanArrayCursor:
		return &integerBooleanCountArrayCursor{BooleanArrayCursor: cur}
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
}

type cursorContext struct {
	ctx   context.Context
	req   *cursors.CursorRequest
	itrs  cursors.CursorIterators
	limit int64
	count int64
	err   error
}

type multiShardArrayCursors struct {
	ctx   context.Context
	limit int64
	req   cursors.CursorRequest

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
		req: cursors.CursorRequest{
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

func (m *multiShardArrayCursors) createCursor(row SeriesRow) cursors.Cursor {
	m.req.Name = row.Name
	m.req.Tags = row.SeriesTags
	m.req.Field = row.Field

	var cond expression
	if row.ValueCond != nil {
		cond = &astExpr{row.ValueCond}
	}

	var shard cursors.CursorIterator
	var cur cursors.Cursor
	for cur == nil && len(row.Query) > 0 {
		shard, row.Query = row.Query[0], row.Query[1:]
		cur, _ = shard.Next(m.ctx, &m.req)
	}

	if cur == nil {
		return nil
	}

	switch c := cur.(type) {
	case cursors.IntegerArrayCursor:
		m.cursors.i.reset(c, row.Query, cond)
		return &m.cursors.i
	case cursors.FloatArrayCursor:
		m.cursors.f.reset(c, row.Query, cond)
		return &m.cursors.f
	case cursors.UnsignedArrayCursor:
		m.cursors.u.reset(c, row.Query, cond)
		return &m.cursors.u
	case cursors.StringArrayCursor:
		m.cursors.s.reset(c, row.Query, cond)
		return &m.cursors.s
	case cursors.BooleanArrayCursor:
		m.cursors.b.reset(c, row.Query, cond)
		return &m.cursors.b
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
}

func (m *multiShardArrayCursors) newAggregateCursor(ctx context.Context, agg *datatypes.Aggregate, cursor cursors.Cursor) cursors.Cursor {
	return newAggregateArrayCursor(ctx, agg, cursor)
}
