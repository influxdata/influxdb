package cursors

import (
	"context"

	"github.com/influxdata/influxdb/v2/models"
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

// CursorRequest is a request to the storage engine for a cursor to be
// created with the given name, tags, and field for a given direction
// and time range.
type CursorRequest struct {
	// Name is the measurement name a cursor is requested for.
	Name []byte

	// Tags is the set of series tags a cursor is requested for.
	Tags models.Tags

	// Field is the selected field for the cursor that is requested.
	Field string

	// Ascending is whether the cursor should move in an ascending
	// or descending time order.
	Ascending bool

	// StartTime is the start time of the cursor. It is the lower
	// absolute time regardless of the Ascending flag. This value
	// is an inclusive bound.
	StartTime int64

	// EndTime is the end time of the cursor. It is the higher
	// absolute time regardless of the Ascending flag. This value
	// is an inclusive bound.
	EndTime int64
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
