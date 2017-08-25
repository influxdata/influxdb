package tsdb

import "github.com/influxdata/influxdb/query"

// EOF represents a "not found" key returned by a Cursor.
const EOF = query.ZeroTime

// Cursor represents an iterator over a series.
type Cursor interface {
	Close()
	SeriesKey() string
}

type IntegerCursor interface {
	Cursor
	Next() (key int64, value int64)
}

type FloatCursor interface {
	Cursor
	Next() (key int64, value float64)
}

type CursorRequest struct {
	Measurement string
	Series      string
	Field       string
	Ascending   bool
	StartTime   int64
	EndTime     int64
}
