package cursors

import (
	"context"

	"github.com/influxdata/platform/models"
)

const DefaultMaxPointsPerBlock = 1000

type Cursor interface {
	Close()
	Err() error
}

type IntegerArrayCursor interface {
	Cursor
	Next() *IntegerArray
}

type FloatArrayCursor interface {
	Cursor
	Next() *FloatArray
}

type UnsignedArrayCursor interface {
	Cursor
	Next() *UnsignedArray
}

type StringArrayCursor interface {
	Cursor
	Next() *StringArray
}

type BooleanArrayCursor interface {
	Cursor
	Next() *BooleanArray
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
