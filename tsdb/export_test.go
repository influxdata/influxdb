package tsdb

import (
	"errors"
	"sync/atomic"

	"github.com/influxdata/influxql"
)

// Test-only hooks into Store and Shard internals for the tsdb_test package.

// epochForTest returns the shard's epoch tracker, or nil if it has none.
func (s *Store) epochForTest(shardID uint64) *epochTracker {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epochs[shardID]
}

// HoldDeleteForTest installs a delete guard on the shard's epoch tracker the
// way DeleteSeries and DeleteMeasurement do, and returns the func that ends the
// delete. It returns nil if the shard has no epoch tracker.
func (s *Store) HoldDeleteForTest(shardID uint64) func() {
	epoch := s.epochForTest(shardID)
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
	epoch := s.epochForTest(shardID)
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

// SetDeleteShardSeriesHookForTest makes DeleteShard call fn before it removes
// each series from the series file, with the number it has removed so far. It
// is not synchronized, so it must be set before any DeleteShard is started.
func (s *Store) SetDeleteShardSeriesHookForTest(fn func(removed uint64)) {
	s.deleteShardSeriesHook = fn
}

// ClosingForTest returns a channel that is closed when a Close of the store
// begins, which is also when deletions in progress are cancelled.
func (s *Store) ClosingForTest() <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closing
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
	return s.wrapEngineForTest(shardID, func(e Engine) Engine {
		return &failCloseOnceEngine{Engine: e, err: err}
	})
}

// failAfterCloseOnceEngine closes the wrapped engine on every Close, and fails
// its first Close after doing so, like an engine that fails partway through
// closing, with some of its cleanup already done.
type failAfterCloseOnceEngine struct {
	Engine
	err    error
	failed atomic.Bool
}

func (e *failAfterCloseOnceEngine) Close() error {
	err := e.Engine.Close()
	if e.failed.CompareAndSwap(false, true) {
		return errors.Join(err, e.err)
	}
	return err
}

// FailNextEngineCloseAfterCleanupForTest makes the next Close of the shard's
// engine close the engine and then return err, so the shard keeps the closed
// engine and a retried Close closes it again. It returns false if the shard is
// not open.
func (s *Store) FailNextEngineCloseAfterCleanupForTest(shardID uint64, err error) bool {
	return s.wrapEngineForTest(shardID, func(e Engine) Engine {
		return &failAfterCloseOnceEngine{Engine: e, err: err}
	})
}

// wrapEngineForTest replaces the shard's engine with wrap of it. It returns
// false if the shard is not open.
func (s *Store) wrapEngineForTest(shardID uint64, wrap func(Engine) Engine) bool {
	sh := s.Shard(shardID)
	if sh == nil {
		return false
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh._engine == nil {
		return false
	}
	sh._engine = wrap(sh._engine)
	return true
}

// EngineFieldSetClosedForTest reports whether the shard still holds an engine
// whose field set has stopped accepting saves, as after a failed close.
func (s *Shard) EngineFieldSetClosedForTest() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s._engine != nil && s._engine.MeasurementFieldSet().ClosedForSaves()
}

// LockChangesFile takes the lock SaveWriter holds while writing the changes
// file, so queued saves stay unwritten until unlock is called.
func (fs *MeasurementFieldSet) LockChangesFile() (unlock func()) {
	fs.changeMgr.mu.Lock()
	return fs.changeMgr.mu.Unlock
}

// SaveAsync queues changes without waiting for them to be written. The
// result of the write arrives on the returned channel.
func (fs *MeasurementFieldSet) SaveAsync(changes FieldChanges) (<-chan error, error) {
	return fs.changeMgr.enqueue(func() (FieldChanges, error) { return changes, nil })
}

// ClosedForSaves reports whether Close has stopped accepting saves.
func (fs *MeasurementFieldSet) ClosedForSaves() bool {
	fs.changeMgr.chMu.RLock()
	defer fs.changeMgr.chMu.RUnlock()
	return fs.changeMgr.writeRequests == nil
}
