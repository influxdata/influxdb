package resultset

import (
	"slices"
)

// BlockType is the set of Go data types that can be represented by TSM.
type BlockType interface {
	int64 | float64 | string | uint64 | bool
}

// TimeArray is a generic type that represents a block of TSM data as a
// collection of timestamps and values. The timestamps are expected
// to be in ascending order.
//
// # NOTE
//
// TimeArray data is materialised from TSM blocks, which are always
// sorted by timestamp in ascending order.
type TimeArray[T BlockType] struct {
	Timestamps []int64
	Values     []T
}

// NewTimeArrayLen creates a new TimeArray with the specified length.
func NewTimeArrayLen[T BlockType](sz int) *TimeArray[T] {
	return &TimeArray[T]{
		Timestamps: make([]int64, sz),
		Values:     make([]T, sz),
	}
}

// MinTime returns the timestamp of the first element.
func (a *TimeArray[T]) MinTime() int64 {
	return a.Timestamps[0]
}

// MaxTime returns the timestamp of the last element.
func (a *TimeArray[T]) MaxTime() int64 {
	return a.Timestamps[len(a.Timestamps)-1]
}

// Len returns the number of elements of the receiver.
func (a *TimeArray[T]) Len() int {
	if a == nil {
		return 0
	}
	return len(a.Timestamps)
}

// Clear sets the length of the receiver to zero.
func (a *TimeArray[T]) Clear() {
	a.Timestamps = a.Timestamps[:0]
	a.Values = a.Values[:0]
}

// Exclude removes the subset of values in [min, max]. The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a *TimeArray[T]) Exclude(min, max int64) {
	if min > max {
		// bad range
		return
	}

	if a.MaxTime() < min || a.MinTime() > max {
		// the array is completely outside the range to be excluded
		return
	}

	rmin, _ := slices.BinarySearch(a.Timestamps, min)
	rmax, found := slices.BinarySearch(a.Timestamps, max)
	if rmax < a.Len() {
		if found {
			rmax++
		}
		rest := a.Len() - rmax
		if rest > 0 {
			ts := a.Timestamps[:rmin+rest]
			copy(ts[rmin:], a.Timestamps[rmax:])
			a.Timestamps = ts

			vs := a.Values[:rmin+rest]
			copy(vs[rmin:], a.Values[rmax:])
			a.Values = vs
			return
		}
	}

	a.Timestamps = a.Timestamps[:rmin]
	a.Values = a.Values[:rmin]
}
