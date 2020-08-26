package tsm1

import (
	"context"

	"github.com/influxdata/influxdb/v2/tsdb"
)

func (e *Engine) CreateCursorIterator(ctx context.Context) (tsdb.CursorIterator, error) {
	return &arrayCursorIterator{e: e}, nil
}
