package tsdb

// EOF represents a "not found" key returned by a Cursor.
const EOF = int64(-1)

// Cursor represents an iterator over a series.
type Cursor interface {
	SeekTo(seek int64) (key int64, value interface{})
	Next() (key int64, value interface{})
	Ascending() bool
}
