package cursors

import (
	"context"

	"github.com/influxdata/influxdb/models"
)

const DefaultMaxPointsPerBlock = 1000

type Cursor interface {
	Close()
	Err() error
	Stats() CursorStats
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
	Stats() CursorStats
}

type CursorIterators []CursorIterator

// Stats returns the aggregate stats of all cursor iterators.
func (a CursorIterators) Stats() CursorStats {
	var stats CursorStats
	for _, itr := range a {
		stats.Add(itr.Stats())
	}
	return stats
}

// CursorStats represents stats collected by a cursor.
type CursorStats struct {
	ScannedValues int // number of values scanned
	ScannedBytes  int // number of uncompressed bytes scanned
}

// Add adds other to s and updates s.
func (s *CursorStats) Add(other CursorStats) {
	s.ScannedValues += other.ScannedValues
	s.ScannedBytes += other.ScannedBytes
}
