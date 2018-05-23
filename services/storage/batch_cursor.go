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

func newAggregateBatchCursor(ctx context.Context, agg *Aggregate, cursor tsdb.Cursor) tsdb.Cursor {
	if cursor == nil {
		return nil
	}

	switch agg.Type {
	case AggregateTypeSum:
		return newSumBatchCursor(cursor)
	case AggregateTypeCount:
		return newCountBatchCursor(cursor)
	default:
		// TODO(sgc): should be validated higher up
		panic("invalid aggregate")
	}
}

func newSumBatchCursor(cur tsdb.Cursor) tsdb.Cursor {
	switch cur := cur.(type) {
	case tsdb.FloatBatchCursor:
		return &floatSumBatchCursor{FloatBatchCursor: cur}
	case tsdb.IntegerBatchCursor:
		return &integerSumBatchCursor{IntegerBatchCursor: cur}
	case tsdb.UnsignedBatchCursor:
		return &unsignedSumBatchCursor{UnsignedBatchCursor: cur}
	default:
		// TODO(sgc): propagate an error instead?
		return nil
	}
}

func newCountBatchCursor(cur tsdb.Cursor) tsdb.Cursor {
	switch cur := cur.(type) {
	case tsdb.FloatBatchCursor:
		return &integerFloatCountBatchCursor{FloatBatchCursor: cur}
	case tsdb.IntegerBatchCursor:
		return &integerIntegerCountBatchCursor{IntegerBatchCursor: cur}
	case tsdb.UnsignedBatchCursor:
		return &integerUnsignedCountBatchCursor{UnsignedBatchCursor: cur}
	case tsdb.StringBatchCursor:
		return &integerStringCountBatchCursor{StringBatchCursor: cur}
	case tsdb.BooleanBatchCursor:
		return &integerBooleanCountBatchCursor{BooleanBatchCursor: cur}
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

type multiShardBatchCursors struct {
	ctx   context.Context
	limit int64
	req   tsdb.CursorRequest

	cursors struct {
		i integerMultiShardBatchCursor
		f floatMultiShardBatchCursor
		u unsignedMultiShardBatchCursor
		b booleanMultiShardBatchCursor
		s stringMultiShardBatchCursor
	}
}

func newMultiShardBatchCursors(ctx context.Context, rr *readRequest) *multiShardBatchCursors {
	lim := rr.limit
	if lim < 0 {
		lim = 1
	}

	m := &multiShardBatchCursors{
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

func (m *multiShardBatchCursors) createCursor(row seriesRow) tsdb.Cursor {
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
	case tsdb.IntegerBatchCursor:
		m.cursors.i.reset(c, row.query, cond)
		return &m.cursors.i
	case tsdb.FloatBatchCursor:
		m.cursors.f.reset(c, row.query, cond)
		return &m.cursors.f
	case tsdb.UnsignedBatchCursor:
		m.cursors.u.reset(c, row.query, cond)
		return &m.cursors.u
	case tsdb.StringBatchCursor:
		m.cursors.s.reset(c, row.query, cond)
		return &m.cursors.s
	case tsdb.BooleanBatchCursor:
		m.cursors.b.reset(c, row.query, cond)
		return &m.cursors.b
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
}
