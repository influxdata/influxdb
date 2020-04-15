package tsm1

import (
	"github.com/influxdata/influxdb/v2/models"
)

const (
	// InvalidMinNanoTime is an invalid nano timestamp that has an ordinal
	// value lower than models.MinNanoTime, the minimum valid timestamp
	// that can be represented.
	InvalidMinNanoTime = models.MinNanoTime - 1
)

// TimeRangeMaxTimeIterator will iterate over the keys of a TSM file, starting at
// the provided key. It is used to determine if each key has data which exists
// within a specified time interval.
type TimeRangeMaxTimeIterator struct {
	timeRangeBlockReader

	// cached values
	maxTime  int64
	hasData  bool
	isLoaded bool
}

// Next advances the iterator and reports if it is still valid.
func (b *TimeRangeMaxTimeIterator) Next() bool {
	if b.Err() != nil {
		return false
	}

	b.clearIsLoaded()

	return b.iter.Next()
}

// Seek points the iterator at the smallest key greater than or equal to the
// given key, returning true if it was an exact match. It returns false for
// ok if the key does not exist.
func (b *TimeRangeMaxTimeIterator) Seek(key []byte) (exact, ok bool) {
	if b.Err() != nil {
		return false, false
	}

	b.clearIsLoaded()

	return b.iter.Seek(key)
}

// HasData reports true if the current key has data for the time range.
func (b *TimeRangeMaxTimeIterator) HasData() bool {
	if b.Err() != nil {
		return false
	}

	b.load()

	return b.hasData
}

// MaxTime returns the maximum timestamp for the current key within the
// requested time range. If an error occurred or there is no data,
// InvalidMinTimeStamp will be returned, which is less than models.MinTimeStamp.
// This property can be leveraged when enumerating keys to find the maximum timestamp,
// as this value will always be lower than any valid timestamp returned.
//
// NOTE: If MaxTime is equal to the upper bounds of the queried time range, it
// means data was found equal to or beyond the requested time range and
// does not mean that data exists at that specific timestamp.
func (b *TimeRangeMaxTimeIterator) MaxTime() int64 {
	if b.Err() != nil {
		return InvalidMinNanoTime
	}

	b.load()

	return b.maxTime
}

func (b *TimeRangeMaxTimeIterator) clearIsLoaded() { b.isLoaded = false }

// setMaxTime sets maxTime = min(b.tr.Max, max) and
// returns true if maxTime == b.tr.Max, indicating
// the iterator has reached the upper bound.
func (b *TimeRangeMaxTimeIterator) setMaxTime(max int64) bool {
	if max > b.tr.Max {
		b.maxTime = b.tr.Max
		return true
	}
	b.maxTime = max
	return false
}

func (b *TimeRangeMaxTimeIterator) load() {
	if b.isLoaded {
		return
	}

	b.isLoaded = true
	b.hasData = false
	b.maxTime = InvalidMinNanoTime

	e, ts := b.getEntriesAndTombstones()
	if len(e) == 0 {
		return
	}

	if len(ts) == 0 {
		// no tombstones, fast path will avoid decoding blocks
		// if queried time interval intersects with one of the entries
		if intersectsEntry(e, b.tr) {
			b.hasData = true
			b.setMaxTime(e[len(e)-1].MaxTime)
			return
		}
	}

	for i := range e {
		if !b.readBlock(&e[i]) {
			goto ERROR
		}

		// remove tombstoned timestamps
		for i := range ts {
			b.a.Exclude(ts[i].Min, ts[i].Max)
		}

		if b.a.Contains(b.tr.Min, b.tr.Max) {
			b.hasData = true
			if b.setMaxTime(b.a.MaxTime()) {
				return
			}
		}
	}

	return
ERROR:
	// ERROR ensures cached state is set to invalid values
	b.hasData = false
	b.maxTime = InvalidMinNanoTime
}
