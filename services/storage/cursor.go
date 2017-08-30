package storage

import (
	"fmt"

	"github.com/influxdata/influxdb/tsdb"
)

func newFilterCursor(cur tsdb.Cursor, cond expression) tsdb.Cursor {
	switch cur := cur.(type) {
	case tsdb.FloatCursor:
		return newFloatFilterCursor(cur, cond)

	case tsdb.IntegerCursor:
		return newIntegerFilterCursor(cur, cond)

	case tsdb.UnsignedCursor:
		return newUnsignedFilterCursor(cur, cond)

	case tsdb.StringCursor:
		return newStringFilterCursor(cur, cond)

	case tsdb.BooleanCursor:
		return newBooleanFilterCursor(cur, cond)

	default:
		panic("invalid cursor type")
	}
}

type singleValue struct {
	v interface{}
}

func (v *singleValue) Value(key string) (interface{}, bool) {
	return v.v, true
}

func newAggregateCursor(agg *Aggregate, cursor tsdb.Cursor) tsdb.Cursor {
	if cursor == nil {
		return nil
	}

	switch agg.Type {
	case AggregateTypeSum:
		return newSumCursor(cursor)
	case AggregateTypeCount:
		return newCountCursor(cursor)

	default:
		// TODO(sgc): should be validated higher up
		panic("invalid aggregate")
	}
}

func newSumCursor(cur tsdb.Cursor) tsdb.Cursor {
	switch cur := cur.(type) {
	case tsdb.FloatCursor:
		return &floatSumCursor{FloatCursor: cur}

	case tsdb.IntegerCursor:
		return &integerSumCursor{IntegerCursor: cur}

	case tsdb.UnsignedCursor:
		return &unsignedSumCursor{UnsignedCursor: cur}

	default:
		panic("unreachable")
	}
}

func newCountCursor(cur tsdb.Cursor) tsdb.Cursor {
	switch cur := cur.(type) {
	case tsdb.FloatCursor:
		return &integerFloatCountCursor{FloatCursor: cur}

	case tsdb.IntegerCursor:
		return &integerIntegerCountCursor{IntegerCursor: cur}

	case tsdb.UnsignedCursor:
		return &integerUnsignedCountCursor{UnsignedCursor: cur}

	case tsdb.StringCursor:
		return &integerStringCountCursor{StringCursor: cur}

	case tsdb.BooleanCursor:
		return &integerBooleanCountCursor{BooleanCursor: cur}

	default:
		panic("unreachable")
	}
}

func newMultiShardCursor(row plannerRow, asc bool, start, end int64) tsdb.Cursor {
	req := &tsdb.CursorRequest{
		Measurement: row.measurement,
		Series:      row.key,
		Field:       row.field,
		Ascending:   asc,
		StartTime:   start,
		EndTime:     end,
	}

	var cond expression
	if row.valueCond != nil {
		cond = &astExpr{row.valueCond}
	}

	var shard *tsdb.Shard
	var cur tsdb.Cursor
	for cur == nil && len(row.shards) > 0 {
		shard, row.shards = row.shards[0], row.shards[1:]
		cur, _ = shard.CreateCursor(req)
	}

	switch c := cur.(type) {
	case tsdb.IntegerCursor:
		return newIntegerMultiShardCursor(c, req, row.shards, cond)

	case tsdb.FloatCursor:
		return newFloatMultiShardCursor(c, req, row.shards, cond)

	case tsdb.UnsignedCursor:
		return newUnsignedMultiShardCursor(c, req, row.shards, cond)

	case tsdb.StringCursor:
		return newStringMultiShardCursor(c, req, row.shards, cond)

	case tsdb.BooleanCursor:
		return newBooleanMultiShardCursor(c, req, row.shards, cond)

	case nil:
		return nil

	default:
		panic("unreachable: " + fmt.Sprintf("%T", cur))
	}
}
