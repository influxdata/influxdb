package reads

import (
	"context"
	"fmt"

	"github.com/influxdata/flux/interval"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

type singleValue struct {
	v interface{}
}

func (v *singleValue) Value(key string) (interface{}, bool) {
	return v.v, true
}

func newAggregateArrayCursor(ctx context.Context, agg *datatypes.Aggregate, cursor cursors.Cursor) (cursors.Cursor, error) {
	switch agg.Type {
	case datatypes.Aggregate_AggregateTypeFirst, datatypes.Aggregate_AggregateTypeLast:
		return newLimitArrayCursor(cursor), nil
	}
	return newWindowAggregateArrayCursor(ctx, agg, interval.Window{}, cursor)
}

func newWindowAggregateArrayCursor(ctx context.Context, agg *datatypes.Aggregate, window interval.Window, cursor cursors.Cursor) (cursors.Cursor, error) {
	if cursor == nil {
		return nil, nil
	}

	switch agg.Type {
	case datatypes.Aggregate_AggregateTypeCount:
		return newWindowCountArrayCursor(cursor, window), nil
	case datatypes.Aggregate_AggregateTypeSum:
		return newWindowSumArrayCursor(cursor, window)
	case datatypes.Aggregate_AggregateTypeFirst:
		return newWindowFirstArrayCursor(cursor, window), nil
	case datatypes.Aggregate_AggregateTypeLast:
		return newWindowLastArrayCursor(cursor, window), nil
	case datatypes.Aggregate_AggregateTypeMin:
		return newWindowMinArrayCursor(cursor, window), nil
	case datatypes.Aggregate_AggregateTypeMax:
		return newWindowMaxArrayCursor(cursor, window), nil
	case datatypes.Aggregate_AggregateTypeMean:
		return newWindowMeanArrayCursor(cursor, window)
	default:
		// TODO(sgc): should be validated higher up
		panic("invalid aggregate")
	}
}

type cursorContext struct {
	ctx  context.Context
	req  *cursors.CursorRequest
	itrs cursors.CursorIterators
	err  error
}

type multiShardArrayCursors struct {
	ctx context.Context
	req cursors.CursorRequest

	cursors struct {
		i integerMultiShardArrayCursor
		f floatMultiShardArrayCursor
		u unsignedMultiShardArrayCursor
		b booleanMultiShardArrayCursor
		s stringMultiShardArrayCursor
	}
}

// newMultiShardArrayCursors is a factory for creating cursors for each series key.
// The range of the cursor is [start, end). The start time is the lower absolute time
// and the end time is the higher absolute time regardless of ascending or descending order.
func newMultiShardArrayCursors(ctx context.Context, start, end int64, asc bool) *multiShardArrayCursors {
	// When we construct the CursorRequest, we translate the time range
	// from [start, stop) to [start, stop]. The cursor readers from storage are
	// inclusive on both ends and we perform that conversion here.
	m := &multiShardArrayCursors{
		ctx: ctx,
		req: cursors.CursorRequest{
			Ascending: asc,
			StartTime: start,
			EndTime:   end - 1,
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
