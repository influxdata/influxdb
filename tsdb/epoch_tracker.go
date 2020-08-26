package tsdb

import (
	"sync"
)

// TODO(jeff): using a mutex is easiest, but there may be a way to do
// this with atomics only, and in a way such that writes are minimally
// blocked.

// epochTracker keeps track of epochs for write and delete operations
// allowing a delete to block until all previous writes have completed.
type epochTracker struct {
	mu      sync.Mutex
	epoch   uint64 // current epoch
	largest uint64 // largest delete possible
	writes  int64  // pending writes
	// pending deletes waiting on writes
	deletes map[uint64]*epochDeleteState
}

// newEpochTracker constructs an epochTracker.
func newEpochTracker() *epochTracker {
	return &epochTracker{
		deletes: make(map[uint64]*epochDeleteState),
	}
}

// epochDeleteState keeps track of the state for a pending delete.
type epochDeleteState struct {
	cond    *sync.Cond
	guard   *guard
	pending int64
}

// done signals that an earlier write has finished.
func (e *epochDeleteState) done() {
	e.cond.L.Lock()
	e.pending--
	if e.pending == 0 {
		e.cond.Broadcast()
	}
	e.cond.L.Unlock()
}

// Wait blocks until all earlier writes have finished.
func (e *epochDeleteState) Wait() {
	e.cond.L.Lock()
	for e.pending > 0 {
		e.cond.Wait()
	}
	e.cond.L.Unlock()
}

// next bumps the epoch and returns it.
func (e *epochTracker) next() uint64 {
	e.epoch++
	return e.epoch
}

// StartWrite should be called before a write is going to start, and after
// it has checked for guards.
func (e *epochTracker) StartWrite() ([]*guard, uint64) {
	e.mu.Lock()
	gen := e.next()
	e.writes++

	if len(e.deletes) == 0 {
		e.mu.Unlock()
		return nil, gen
	}

	guards := make([]*guard, 0, len(e.deletes))
	for _, state := range e.deletes {
		guards = append(guards, state.guard)
	}

	e.mu.Unlock()
	return guards, gen
}

// EndWrite should be called when the write ends for any reason.
func (e *epochTracker) EndWrite(gen uint64) {
	e.mu.Lock()
	if gen <= e.largest {
		// TODO(jeff): at the cost of making waitDelete more
		// complicated, we can keep a sorted slice which would
		// allow this to exit early rather than go over the
		// whole map.
		for dgen, state := range e.deletes {
			if gen > dgen {
				continue
			}
			state.done()
		}
	}
	e.writes--
	e.mu.Unlock()
}

// epochWaiter is a type that can be waited on for prior writes to finish.
type epochWaiter struct {
	gen     uint64
	guard   *guard
	state   *epochDeleteState
	tracker *epochTracker
}

// Wait blocks until all writes prior to the creation of the waiter finish.
func (e epochWaiter) Wait() {
	if e.state == nil || e.tracker == nil {
		return
	}
	e.state.Wait()
}

// Done marks the delete as completed, removing its guard.
func (e epochWaiter) Done() {
	e.tracker.mu.Lock()
	delete(e.tracker.deletes, e.gen)
	e.tracker.mu.Unlock()
	e.guard.Done()
}

// WaitDelete should be called after any delete guards have been installed.
// The returned epochWaiter will not be affected by any future writes.
func (e *epochTracker) WaitDelete(guard *guard) epochWaiter {
	e.mu.Lock()
	state := &epochDeleteState{
		pending: e.writes,
		cond:    sync.NewCond(new(sync.Mutex)),
		guard:   guard,
	}

	// record our pending delete
	gen := e.next()
	e.largest = gen
	e.deletes[gen] = state
	e.mu.Unlock()

	return epochWaiter{
		gen:     gen,
		guard:   guard,
		state:   state,
		tracker: e,
	}
}
