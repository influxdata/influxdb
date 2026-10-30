package tsdb

import "github.com/influxdata/influxql"

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
