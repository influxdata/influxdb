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

func newMultiShardBatchCursor(ctx context.Context, row seriesRow, rr *readRequest) tsdb.Cursor {
	req := &tsdb.CursorRequest{
		Measurement: row.measurement,
		Series:      row.key,
		Field:       row.field,
		Ascending:   rr.asc,
		StartTime:   rr.start,
		EndTime:     rr.end,
	}

	var cond expression
	if row.valueCond != nil {
		cond = &astExpr{row.valueCond}
	}

	var shard *tsdb.Shard
	var cur tsdb.Cursor
	for cur == nil && len(row.shards) > 0 {
		shard, row.shards = row.shards[0], row.shards[1:]
		cur, _ = shard.CreateCursor(ctx, req)
	}

	if cur == nil {
		return nil
	}

	switch c := cur.(type) {
	case tsdb.IntegerBatchCursor:
		return newIntegerMultiShardBatchCursor(ctx, c, rr, req, row.shards, cond)
	case tsdb.FloatBatchCursor:
		return newFloatMultiShardBatchCursor(ctx, c, rr, req, row.shards, cond)
	case tsdb.UnsignedBatchCursor:
		return newUnsignedMultiShardBatchCursor(ctx, c, rr, req, row.shards, cond)
	case tsdb.StringBatchCursor:
		return newStringMultiShardBatchCursor(ctx, c, rr, req, row.shards, cond)
	case tsdb.BooleanBatchCursor:
		return newBooleanMultiShardBatchCursor(ctx, c, rr, req, row.shards, cond)
	default:
		panic(fmt.Sprintf("unreachable: %T", cur))
	}
}
