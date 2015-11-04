package tsdb

import (
	"time"
)

// maxTime is used as the maximum time value when computing an unbounded range.
var maxTime = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)

type Iterator interface {
}

type FloatIterator interface {
	Iterator
	Next() *FloatValue
}

type FloatValue struct {
	Time  time.Time
	Value float64
	Tags  map[string]string
}

// FloatMinIterator emits a float value for each interval.
type FloatMinIterator struct {
	// buffered input iterators
	inputs bufFloatIterators

	// Calculates the window based on offset from the epoch and window duration.
	Offset   time.Duration
	Interval time.Duration
}

// NewFloatMinIterator returns a new instance of
func NewFloatMinIterator(inputs []FloatIterator) *FloatMinIterator {
	return &FloatMinIterator{
		inputs: newBufFloatIterators(inputs),
	}
}

// Next returns the minimum value for the next available interval.
func (itr *FloatMinIterator) Next() *FloatValue {
	// Calculate next window.
	_, endTime := itr.inputs.nextWindow(itr.Interval, itr.Offset)

	// Consume all points from all iterators within the window.
	var min *FloatValue
	for _, input := range itr.inputs {
		for {
			v := input.Next()
			if v == nil {
				break
			} else if !v.Time.Before(endTime) {
				input.unread(v)
				break
			}

			if min == nil || v.Value < min.Value {
				min = v
			}
		}
	}

	return min
}

// bufFloatIterator represents a buffered FloatIterator.
type bufFloatIterator struct {
	itr FloatIterator
	buf *FloatValue
}

// Next returns the current buffer, if exists, or calls the underlying iterator.
func (itr *bufFloatIterator) Next() *FloatValue {
	if itr.buf != nil {
		buf := itr.buf
		itr.buf = nil
		return buf
	}
	return itr.itr.Next()
}

// unread sets v to the buffer. It is read on the next call to Next().
func (itr *bufFloatIterator) unread(v *FloatValue) {
	itr.buf = v
}

// bufFloatIterators represents a list of buffered FloatIterator.
type bufFloatIterators []*bufFloatIterator

// newBufFloatIterators returns a list of buffered FloatIterators.
func newBufFloatIterators(itrs []FloatIterator) bufFloatIterators {
	a := make(bufFloatIterators, len(itrs))
	for i := range itrs {
		a[i] = &bufFloatIterator{itr: itrs[i]}
	}
	return a
}

// nextWindow calculates the next window based on the next available points from each iterator.
func (a bufFloatIterators) nextWindow(interval, offset time.Duration) (startTime, endTime time.Time) {
	if interval <= 0 {
		return time.Time{}, maxTime
	}

	// Find the lowest next timestamp.
	var min time.Time
	for _, itr := range a {
		v := itr.Next()
		if v != nil && (min.IsZero() || v.Time.Before(min)) {
			min = v.Time
		}
		itr.unread(v)
	}

	// Calculate the window based on the next minimum.
	startTime = min.Truncate(interval).Add(offset)
	endTime = startTime.Add(interval)
	return
}
