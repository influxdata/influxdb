package tsi1

import (
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

// This file holds exported methods that exist only to support tests. Because it
// is a _test.go file it is compiled only by `go test`, so these helpers do not
// become part of the production build or the public package API, yet they remain
// callable from the external tsi1_test package.

// SetMaxLogFileSize provides a setter for the partition setting of maxLogFileSize
// that is otherwise only available at creation time. Returns the previous value.
// Only for tests!
func (p *Partition) SetMaxLogFileSize(new int64) (old int64) {
	p.mu.Lock()
	old, p.maxLogFileSize = p.maxLogFileSize, new
	p.mu.Unlock()
	return old
}

// SetMaxLogFileAge provides a setter for the partition setting of maxLogFileAge
// that is otherwise only available at creation time. Returns the previous value.
// Only for tests!
func (p *Partition) SetMaxLogFileAge(new time.Duration) (old time.Duration) {
	p.mu.Lock()
	old, p.maxLogFileAge = p.maxLogFileAge, new
	p.mu.Unlock()
	return old
}

// SetManifestPathForTest is only to force a bad path in testing.
func (p *Partition) SetManifestPathForTest(path string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.manifestPathFn = func() string { return path }
}

// CreateSeriesListIfNotExists is an exported wrapper around
// createSeriesListIfNotExists for use in tests only. Only for tests!
func (p *Partition) CreateSeriesListIfNotExists(names [][]byte, tagsSlice []models.Tags, tracker tsdb.StatsTracker) ([]uint64, error) {
	return p.createSeriesListIfNotExists(names, tagsSlice, tracker)
}

// TagValueCacheIdleTimeout returns the series ID set cache's idle timeout; 0
// for a fixed-size cache, which ignores the setting.
// Only for tests!
func (i *Index) TagValueCacheIdleTimeout() time.Duration {
	return i.tagValueCache.idleTimeout
}

// HoldCompactionForTest makes the partition count a compaction that has not
// installed its output, which is what Wait waits for, until the returned func
// is called.
// Only for tests!
func (p *Partition) HoldCompactionForTest() (release func()) {
	p.currentCompactionN.Add(1)
	return func() { p.currentCompactionN.Add(-1) }
}

// HoldCompactionForTest does the same as Partition.HoldCompactionForTest, on
// the index's last partition, so Index.Wait passes the others first.
// Only for tests!
func (i *Index) HoldCompactionForTest() (release func()) {
	return i.partitions[len(i.partitions)-1].HoldCompactionForTest()
}

// TagValueCacheIdleSweeperRunning reports whether the series ID set cache's
// idle sweeper is running.
// Only for tests!
func (i *Index) TagValueCacheIdleSweeperRunning() bool {
	i.tagValueCache.Lock()
	defer i.tagValueCache.Unlock()
	return i.tagValueCache.sweepClosing != nil
}
