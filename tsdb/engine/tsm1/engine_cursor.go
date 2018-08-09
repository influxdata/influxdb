package tsm1

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/tsdb"
)

func (e *Engine) CreateCursorIterator(ctx context.Context) (tsdb.CursorIterator, error) {
	switch ct := tsdb.CursorTypeFromContext(ctx); ct {
	case tsdb.BatchCursorType:
		return &cursorIterator{e: e}, nil
	case tsdb.ArrayCursorType, tsdb.DefaultCursorType:
		return &arrayCursorIterator{e: e}, nil
	default:
		panic(fmt.Sprintf("unexpected cursor type %d", ct))
	}
}
