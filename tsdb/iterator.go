package tsdb

import (
	"fmt"
	"math"
	"reflect"
	"time"
)

// maxTime is used as the maximum time value when computing an unbounded range.
var maxTime = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)

type Value interface{}

func valueOf(v Value) interface{} {
	switch v := v.(type) {
	case *FloatValue:
		return v.Value
	default:
		panic(fmt.Sprintf("invalid value type: %T", v))
	}
}

func valueTime(v Value) time.Time {
	switch v := v.(type) {
	case *FloatValue:
		return v.Time
	default:
		panic(fmt.Sprintf("invalid value type: %T", v))
	}
}

func valueNameTags(v Value) (name string, tags map[string]string) {
	fmt.Println("?vNT", v)
	switch v := v.(type) {
	case *FloatValue:
		return v.Name, v.Tags
	default:
		panic(fmt.Sprintf("invalid value type: %T", v))
	}
}

type Values []Value

func (a Values) Equals(other Values) bool {
	if len(a) != len(other) {
		return false
	}
	for i := range a {
		// Special handling for float values since NaN != NaN.
		// https://github.com/golang/go/issues/12025
		if x, ok := a[i].(*FloatValue); ok {
			if y, ok := other[i].(*FloatValue); ok {
				if !x.Time.Equal(y.Time) {
					return false
				} else if x.Value != y.Value && !(math.IsNaN(x.Value) && math.IsNaN(y.Value)) {
					return false
				} else if !reflect.DeepEqual(x.Tags, y.Tags) {
					return false
				}
			} else {
				return false
			}
		} else {
			if !reflect.DeepEqual(a[i], other[i]) {
				return false
			}
		}
	}

	return true
}

type FloatValue struct {
	Name  string
	Time  time.Time
	Value float64
	Tags  map[string]string
}

// Iterator represents a generic interface for all Iterators.
// Most iterator operations are done on the typed sub-interfaces.
type Iterator interface {
	Close() error
}

// Iterators represents a list of iterators.
type Iterators []Iterator

// Close closes all iterators.
func (a Iterators) Close() error {
	for _, itr := range a {
		itr.Close()
	}
	return nil
}

// FloatIterator represents a stream of float values.
type FloatIterator interface {
	Iterator
	Next() *FloatValue
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

// Close closes the iterator and all child iterators.
func (itr *FloatMinIterator) Close() error {
	for _, input := range itr.inputs {
		input.Close()
	}
	return nil
}

// Next returns the minimum value for the next available interval.
func (itr *FloatMinIterator) Next() *FloatValue {
	// Calculate next window.
	_, endTime := itr.inputs.nextWindow(itr.Interval, itr.Offset)

	// Consume all points from all iterators within the window.
	var min *FloatValue
	for _, input := range itr.inputs {
		for {
			v := input.NextBefore(endTime)
			if v == nil {
				break
			} else if min == nil || v.Value < min.Value {
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

// Close closes the underlying iterator.
func (itr *bufFloatIterator) Close() error { return itr.itr.Close() }

// Next returns the current buffer, if exists, or calls the underlying iterator.
func (itr *bufFloatIterator) Next() *FloatValue {
	if itr.buf != nil {
		buf := itr.buf
		itr.buf = nil
		return buf
	}
	return itr.itr.Next()
}

// Next returns the next value if it is before t.
// If the next value is on or after t then it is moved to the buffer.
func (itr *bufFloatIterator) NextBefore(t time.Time) *FloatValue {
	v := itr.Next()
	if v == nil {
		return nil
	} else if !v.Time.Before(t) {
		itr.unread(v)
		return nil
	}
	return v
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

type nilFloatIterator struct{}

func (*nilFloatIterator) Close() error      { return nil }
func (*nilFloatIterator) Next() *FloatValue { return nil }

// Join combines inputs based on timestamp and returns new iterators.
// The output iterators guarantee that one value will be output for every timestamp.
func Join(inputs []Iterator) (outputs []Iterator) {
	itrs := make([]joinIterator, len(inputs))
	for i, input := range inputs {
		switch input := input.(type) {
		case FloatIterator:
			itrs[i] = newFloatJoinIterator(input)
		default:
			panic(fmt.Sprintf("unsupported join iterator type: %T", input))
		}
	}

	// Begin joining goroutine.
	go join(itrs)

	return joinIterators(itrs).iterators()
}

// join runs in a separate goroutine to join input values on timestamp.
func join(itrs []joinIterator) {
	for {
		// Find min timestamp.
		var min time.Time
		for _, itr := range itrs {
			t := itr.loadBuf()
			if !t.IsZero() && (min.IsZero() || t.Before(t)) {
				min = t
			}
		}

		// Exit when no more values are available.
		if min.IsZero() {
			break
		}

		// Emit value on every output.
		for _, itr := range itrs {
			itr.emitAt(min)
		}
	}

	// Close all iterators.
	for _, itr := range itrs {
		itr.Close()
	}
}

// joinIterator represents output iterator used by join().
type joinIterator interface {
	Iterator
	loadBuf() time.Time
	emitAt(t time.Time)
}

type joinIterators []joinIterator

// iterators returns itrs as a list of generic iterators.
func (itrs joinIterators) iterators() []Iterator {
	a := make([]Iterator, len(itrs))
	for i, itr := range itrs {
		a[i] = itr
	}
	return a
}

// floatJoinIterator represents a join iterator that processes float values.
type floatJoinIterator struct {
	input FloatIterator
	buf   *FloatValue      // next value from input
	c     chan *FloatValue // streaming output channel
}

// newFloatJoinIterator returns a new join iterator that wraps input.
func newFloatJoinIterator(input FloatIterator) *floatJoinIterator {
	return &floatJoinIterator{
		input: input,
		c:     make(chan *FloatValue, 1),
	}
}

// Close close the iterator.
func (itr *floatJoinIterator) Close() error {
	close(itr.c)
	return nil
}

// Next returns the next point from the streaming channel.
func (itr *floatJoinIterator) Next() *FloatValue { return <-itr.c }

// loadBuf reads the next value from the input into the buffer.
func (itr *floatJoinIterator) loadBuf() time.Time {
	if itr.buf != nil {
		return itr.buf.Time
	}

	itr.buf = itr.input.Next()
	if itr.buf == nil {
		return time.Time{}
	}
	return itr.buf.Time
}

// emitAt emits the buffered point if its timestamp equals t.
// Otherwise it emits a null value with the timestamp t.
func (itr *floatJoinIterator) emitAt(t time.Time) {
	var v *FloatValue
	if itr.buf == nil || !itr.buf.Time.Equal(t) {
		v = &FloatValue{Time: t, Value: math.NaN()}
	} else {
		v, itr.buf = itr.buf, nil
	}
	itr.c <- v
}
