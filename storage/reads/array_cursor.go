package reads

import (
	"context"
	"fmt"
	"math"

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
	if cursor == nil {
		return nil
	}

	switch agg.Type {
	case datatypes.AggregateTypeCount:
		return newCountArrayCursor(cursor)
	case datatypes.AggregateTypeSum:
		return newSumArrayCursor(cursor)
	default:
		panic("invalid aggregate")
	}
}

func newWindowAggregateArrayCursor(ctx context.Context, req *datatypes.ReadWindowAggregateRequest, cursor cursors.Cursor) cursors.Cursor {
	if cursor == nil {
		return nil
	}

	switch req.Aggregate[0].Type {
	case datatypes.AggregateTypeCount:
		return newWindowCountArrayCursor(cursor, req)
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
		return newIntegerFloatCountArrayCursor(cur)
	case cursors.IntegerArrayCursor:
		return newIntegerIntegerCountArrayCursor(cur)
	case cursors.UnsignedArrayCursor:
		return newIntegerUnsignedCountArrayCursor(cur)
	case cursors.StringArrayCursor:
		return newIntegerStringCountArrayCursor(cur)
	case cursors.BooleanArrayCursor:
		return newIntegerBooleanCountArrayCursor(cur)
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
}

func newWindowCountArrayCursor(cur cursors.Cursor, req *datatypes.ReadWindowAggregateRequest) cursors.Cursor {
	if req.WindowEvery == math.MaxInt64 {
		// This means to aggregate over the entire range,
		// don't do windowed aggregation.
		return newCountArrayCursor(cur)
	}
	switch cur := cur.(type) {
	case cursors.FloatArrayCursor:
		return newIntegerFloatWindowCountArrayCursor(cur, req.WindowEvery)
	case cursors.IntegerArrayCursor:
		return newIntegerIntegerWindowCountArrayCursor(cur, req.WindowEvery)
	case cursors.UnsignedArrayCursor:
		return newIntegerUnsignedWindowCountArrayCursor(cur, req.WindowEvery)
	case cursors.StringArrayCursor:
		return newIntegerStringWindowCountArrayCursor(cur, req.WindowEvery)
	case cursors.BooleanArrayCursor:
		return newIntegerBooleanWindowCountArrayCursor(cur, req.WindowEvery)
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
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
