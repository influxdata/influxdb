package tsdb

import (
	"context"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
)

// EOF represents a "not found" key returned by a Cursor.
const EOF = query.ZeroTime

// Cursor represents an iterator over a series.
type Cursor interface {
	Close()
	Err() error
}

type IntegerBatchCursor interface {
	Cursor
	Next() (keys []int64, values []int64)
}

type FloatBatchCursor interface {
	Cursor
	Next() (keys []int64, values []float64)
}

type UnsignedBatchCursor interface {
	Cursor
	Next() (keys []int64, values []uint64)
}

type StringBatchCursor interface {
	Cursor
	Next() (keys []int64, values []string)
}

type BooleanBatchCursor interface {
	Cursor
	Next() (keys []int64, values []bool)
}

type CursorRequest struct {
	Name      []byte
	Tags      models.Tags
	Field     string
	Ascending bool
	StartTime int64
	EndTime   int64
}

type CursorIterator interface {
	Next(ctx context.Context, r *CursorRequest) (Cursor, error)
}

type CursorIterators []CursorIterator

func CreateCursorIterators(ctx context.Context, shards []*Shard) (CursorIterators, error) {
	q := make(CursorIterators, 0, len(shards))
	for _, s := range shards {
		// possible errors are ErrEngineClosed or ErrShardDisabled, so we can safely skip those shards
		if cq, err := s.CreateCursorIterator(ctx); cq != nil && err == nil {
			q = append(q, cq)
		}
	}
	if len(q) == 0 {
		return nil, nil
	}
	return q, nil
}
