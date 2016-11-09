package lineprotocol

import (
	"io"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)

// Precision a type for the precision of the timestamp
type Precision int

const (
	Nanosecond = iota
	Second
)

// Timestamp represents a timestamp in line protocol
// in either second or nanosecond precision
type Timestamp struct {
	precision Precision
	ptr       unsafe.Pointer
}

// NewTimestamp returns a pointer to a Timestamp
func NewTimestamp(p Precision) *Timestamp {
	return &Timestamp{
		precision: p,
	}
}

// TimePtr returns an unsafe.Pointer to an underlying
// time.Time object
func (t *Timestamp) TimePtr() *unsafe.Pointer {
	return &t.ptr
}

// SetTime takes an pointer to a time.Time and atomically
// sets the pointer on the Timestamp struct
func (t *Timestamp) SetTime(ts *time.Time) {
	tsPtr := unsafe.Pointer(ts)
	atomic.StorePointer(&t.ptr, tsPtr)
}

// WriteTo writes the timestamp to an io.Writer
func (t *Timestamp) WriteTo(w io.Writer) (int64, error) {
	tsPtr := atomic.LoadPointer(&t.ptr)

	tsTime := *(*time.Time)(tsPtr)
	ts := tsTime.UnixNano()

	if t.precision == Second {
		ts = tsTime.Unix()
	}

	// Max int64 fits in 19 base-10 digits;
	buf := make([]byte, 0, 19)
	buf = strconv.AppendInt(buf, ts, 10)

	n, err := w.Write(buf)
	return int64(n), err
}
