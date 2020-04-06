package tsm1

import (
	"context"

	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

func (e *Engine) CreateCursorIterator(ctx context.Context) (cursors.CursorIterator, error) {
	return &arrayCursorIterator{e: e}, nil
}
