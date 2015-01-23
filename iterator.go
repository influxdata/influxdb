package influxdb

import (
	"time"
)

// Iterator represents an interface to iterate over the raw data.
type Iterator struct {
	intmin, intmax int64
	interval       time.Duration
}

// Open opens and initializes the iterator.
func (i *Iterator) Open() error { panic("TODO") }

// Close closes the iterator.
func (i *Iterator) Close() error { panic("TODO") }

// Next returns the next value from the iterator.
func (i *Iterator) Next() (key int64, value interface{}) { panic("TODO") }

// NextIterval moves to the next iterval. Returns true unless EOF.
func (i *Iterator) NextIterval() bool { panic("TODO") }

// Time returns start time of the current interval.
func (i *Iterator) Time() int64 { return i.intmin }

// Interval returns the group by duration.
func (i *Iterator) Interval() time.Duration { return time.Duration(i.interval) }
