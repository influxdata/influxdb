package reads

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

type singleValue struct {
	v interface{}
}

func (v *singleValue) Value(key string) (interface{}, bool) {
	return v.v, true
}

func newAggregateArrayCursor(ctx context.Context, agg *datatypes.Aggregate, cursor cursors.Cursor) cursors.Cursor {
	return newWindowAggregateArrayCursor(ctx, agg, 0, cursor)
}

func newWindowAggregateArrayCursor(ctx context.Context, agg *datatypes.Aggregate, every int64, cursor cursors.Cursor) cursors.Cursor {
	if cursor == nil {
		return nil
	}

	switch agg.Type {
	case datatypes.AggregateTypeCount:
		return newWindowCountArrayCursor(cursor, every)
	case datatypes.AggregateTypeSum:
		return newWindowSumArrayCursor(cursor, every)
	default:
		// TODO(sgc): should be validated higher up
		panic("invalid aggregate")
	}
}

func newWindowCountArrayCursor(cur cursors.Cursor, every int64) cursors.Cursor {
	switch cur := cur.(type) {
	case cursors.FloatArrayCursor:
		return newFloatWindowCountArrayCursor(cur, every)
	case cursors.IntegerArrayCursor:
		return newIntegerWindowCountArrayCursor(cur, every)
	case cursors.UnsignedArrayCursor:
		return newUnsignedWindowCountArrayCursor(cur, every)
	case cursors.StringArrayCursor:
		return newStringWindowCountArrayCursor(cur, every)
	case cursors.BooleanArrayCursor:
		return newBooleanWindowCountArrayCursor(cur, every)
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
}

func newWindowSumArrayCursor(cur cursors.Cursor, every int64) cursors.Cursor {
	switch cur := cur.(type) {
	case cursors.FloatArrayCursor:
		return newFloatWindowSumArrayCursor(cur, every)
	case cursors.IntegerArrayCursor:
		return newIntegerWindowSumArrayCursor(cur, every)
	case cursors.UnsignedArrayCursor:
		return newUnsignedWindowSumArrayCursor(cur, every)
	default:
		panic(fmt.Sprintf("unsupported for aggregate sum: %T", cur))
	}
}

type cursorContext struct {
	ctx            context.Context
	req            *cursors.CursorRequest
	cursorIterator cursors.CursorIterator
	err            error
}

type arrayCursors struct {
	ctx context.Context
	req cursors.CursorRequest

	cursors struct {
		i integerArrayCursor
		f floatArrayCursor
		u unsignedArrayCursor
		b booleanArrayCursor
		s stringArrayCursor
	}
}

func newArrayCursors(ctx context.Context, start, end int64, asc bool) *arrayCursors {
	m := &arrayCursors{
		ctx: ctx,
		req: cursors.CursorRequest{
			Ascending: asc,
			StartTime: start,
			EndTime:   end,
		},
	}

	cc := cursorContext{
		ctx: ctx,
		req: &m.req,
	}

	m.cursors.i.cursorContext = cc
	m.cursors.f.cursorContext = cc
	m.cursors.u.cursorContext = cc
	m.cursors.b.cursorContext = cc
	m.cursors.s.cursorContext = cc

	return m
}

func (m *arrayCursors) createCursor(seriesRow SeriesRow) cursors.Cursor {
	m.req.Name = seriesRow.Name
	m.req.Tags = seriesRow.SeriesTags
	m.req.Field = seriesRow.Field

	var cond expression
	if seriesRow.ValueCond != nil {
		cond = &astExpr{seriesRow.ValueCond}
	}

	if seriesRow.Query == nil {
		return nil
	}
	cur, _ := seriesRow.Query.Next(m.ctx, &m.req)
	seriesRow.Query = nil
	if cur == nil {
		return nil
	}

	switch c := cur.(type) {
	case cursors.IntegerArrayCursor:
		m.cursors.i.reset(c, seriesRow.Query, cond)
		return &m.cursors.i
	case cursors.FloatArrayCursor:
		m.cursors.f.reset(c, seriesRow.Query, cond)
		return &m.cursors.f
	case cursors.UnsignedArrayCursor:
		m.cursors.u.reset(c, seriesRow.Query, cond)
		return &m.cursors.u
	case cursors.StringArrayCursor:
		m.cursors.s.reset(c, seriesRow.Query, cond)
		return &m.cursors.s
	case cursors.BooleanArrayCursor:
		m.cursors.b.reset(c, seriesRow.Query, cond)
		return &m.cursors.b
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
}
