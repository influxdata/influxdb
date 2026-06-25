package tsi1

import (
	"time"

	"github.com/influxdata/influxdb/v2/models"
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
func (p *Partition) CreateSeriesListIfNotExists(names [][]byte, tagsSlice []models.Tags) ([]uint64, error) {
	return p.createSeriesListIfNotExists(names, tagsSlice)
}
