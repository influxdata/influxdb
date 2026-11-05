package tsdb

import (
	"sync/atomic"

	"github.com/influxdata/influxql"
)

// This file holds exported methods that exist only to support tests. Because it
// is a _test.go file it is compiled only by `go test`, so these helpers do not
// become part of the production build or the public package API, yet they remain
// callable from the external tsdb_test package.

// HoldDeleteForTest installs a delete guard on the shard's epoch tracker the
// way DeleteSeries and DeleteMeasurement do, and returns the func that ends the
// delete. It returns nil if the shard has no epoch tracker.
func (s *Store) HoldDeleteForTest(shardID uint64) func() {
	s.mu.RLock()
	epoch := s.epochs[shardID]
	s.mu.RUnlock()
	if epoch == nil {
		return nil
	}
	waiter := epoch.WaitDelete(newGuard(influxql.MinTime, influxql.MaxTime, nil, nil))
	waiter.Wait()
	return waiter.Done
}

// HoldWriteForTest enters the shard's epoch tracker the way WriteToShard does.
// A delete that reaches the shard's tracker after this blocks inside its
// operation until endWrite is called. deleteHeld reports whether such a delete
// exists; once true, it stays true until endWrite. Both are nil if the shard has
// no epoch tracker.
func (s *Store) HoldWriteForTest(shardID uint64) (deleteHeld func() bool, endWrite func()) {
	s.mu.RLock()
	epoch := s.epochs[shardID]
	s.mu.RUnlock()
	if epoch == nil {
		return nil, nil
	}
	_, gen := epoch.StartWrite()
	deleteHeld = func() bool {
		epoch.mu.Lock()
		defer epoch.mu.Unlock()
		for deleteGen := range epoch.deletes {
			if deleteGen > gen {
				return true
			}
		}
		return false
	}
	return deleteHeld, func() { epoch.EndWrite(gen) }
}

// failCloseOnceEngine fails its first Close without closing the wrapped engine.
type failCloseOnceEngine struct {
	Engine
	err    error
	failed atomic.Bool
}

func (e *failCloseOnceEngine) Close() error {
	if e.failed.CompareAndSwap(false, true) {
		return e.err
	}
	return e.Engine.Close()
}

// FailNextEngineCloseForTest makes the next Close of the shard's engine return
// err. Later Closes close the engine. It returns false if the shard is not open.
func (s *Store) FailNextEngineCloseForTest(shardID uint64, err error) bool {
	sh := s.Shard(shardID)
	if sh == nil {
		return false
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh._engine == nil {
		return false
	}
	sh._engine = &failCloseOnceEngine{Engine: sh._engine, err: err}
	return true
}
