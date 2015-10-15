package tsm1

import (
	"reflect"
	"sync"
)

// writeLock is a lock that enables locking of ranges between a
// min and max value. We use this so that flushes from the WAL
// can occur concurrently along with compactions.
type WriteLock struct {
	rangesLock sync.Mutex
	ranges     []*rangeLock
}

// LockRange will ensure an exclusive lock between the min and
// max values inclusive. Any subsequent calls that have an
// an overlapping range will have to wait until the previous
// lock is released. A corresponding call to UnlockRange should
// be deferred.
func (w *WriteLock) LockRange(min, max int64) {
	r := &rangeLock{min: min, max: max}
	for {
		ranges := w.currentlyLockedRanges()

		// ensure there are no currently locked ranges that overlap
		for _, rr := range ranges {
			if rr.overlaps(r) {
				// wait until it gets unlocked
				rr.mu.Lock()
				// release the lock so the object can get GC'd
				rr.mu.Unlock()
			}
		}

		// ensure that no one else got a lock on the range while we
		// were waiting
		w.rangesLock.Lock()
		if len(w.ranges) == 0 || reflect.DeepEqual(ranges, w.ranges) {
			// and lock the range
			r.mu.Lock()

			// now that we know the range is free, add it to the locks
			w.ranges = append(w.ranges, r)
			w.rangesLock.Unlock()
			return
		}

		// try again
		w.rangesLock.Unlock()
	}
}

// UnlockRange will release a previously locked range.
func (w *WriteLock) UnlockRange(min, max int64) {
	w.rangesLock.Lock()
	defer w.rangesLock.Unlock()

	// take the range out of the slice and unlock it
	var a []*rangeLock
	for _, r := range w.ranges {
		if r.min == min && r.max == max {
			r.mu.Unlock()
			continue
		}
		a = append(a, r)
	}
	w.ranges = a
}

func (w *WriteLock) currentlyLockedRanges() []*rangeLock {
	w.rangesLock.Lock()
	defer w.rangesLock.Unlock()
	a := make([]*rangeLock, len(w.ranges))
	copy(a, w.ranges)
	return a
}

type rangeLock struct {
	mu  sync.Mutex
	min int64
	max int64
}

func (r *rangeLock) overlaps(l *rangeLock) bool {
	if l.min >= r.min && l.min <= r.max {
		return true
	} else if l.max >= r.min && l.max <= r.max {
		return true
	} else if l.min <= r.min && l.max >= r.max {
		return true
	} else if l.min >= r.min && l.max <= r.max {
		return true
	}
	return false
}
