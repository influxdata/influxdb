package tsdb

import "github.com/influxdata/influxdb/query"

// EOF represents a "not found" key returned by a Cursor.
const EOF = query.ZeroTime

// Cursor represents an iterator over a series.
type Cursor interface {
	Close()
	SeriesKey() string
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
	Measurement string
	Series      string
	Field       string
	Ascending   bool
	StartTime   int64
	EndTime     int64
}
