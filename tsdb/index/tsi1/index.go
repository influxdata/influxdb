package tsi1

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"github.com/uber-go/zap"
)

// IndexName is the name of the index.
const IndexName = "tsi1"

// TotalPartitions determines how many shards the index will be partitioned into.
//
// NOTE: Currently, this *must* not be variable. If this package is recompiled
// with a different TotalPartitions value, and ran against an existing TSI index
// the database will be unable to locate existing series properly.
//
// TODO(edd): If this sharding spike is successful then implement a consistent
// hashring so that we can fiddle with this.
//
// NOTE(edd): Currently this must be a power of 2.
const TotalPartitions = 16

// An IndexOption is a functional option for changing the configuration of
// an Index.
type IndexOption func(i *Index)

// WithPath sets the root path of the Index
var WithPath = func(path string) IndexOption {
	return func(i *Index) {
		i.path = path
	}
}

// DisableCompactions disables compactions on the Index.
var DisableCompactions = func() IndexOption {
	return func(i *Index) {
		i.disableCompactions = true
	}
}

// WithLogger sets the logger for the Index.
var WithLogger = func(l zap.Logger) IndexOption {
	return func(i *Index) {
		i.logger = l.With(zap.String("index", "tsi"))
	}
}

// Index represents a collection of layered index files and WAL.
type Index struct {
	mu         sync.RWMutex
	partitions []*Partition
	opened     bool

	// The following can be set when initialising an Index.
	path               string     // Root directory of the index partitions.
	disableCompactions bool       // Initially disables compactions on the index.
	logger             zap.Logger // Index's logger.

	// Index's version.
	version int
}

// NewIndex returns a new instance of Index.
func NewIndex(options ...IndexOption) *Index {
	idx := &Index{
		partitions: make([]*Partition, TotalPartitions),
		logger:     zap.New(zap.NullEncoder()),
		version:    Version,
	}

	for _, option := range options {
		option(idx)
	}

	// Inititalise index partitions.
	for i := 0; i < len(idx.partitions); i++ {
		p := NewPartition(filepath.Join(idx.path, fmt.Sprint(i)))
		p.compactionsDisabled = idx.disableCompactions
		p.logger = idx.logger

		idx.partitions[i] = p
	}
	return idx
}

// Type returns the type of Index this is.
func (i *Index) Type() string { return IndexName }

// Open opens the index.
func (i *Index) Open() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.opened {
		return errors.New("index already open")
	}

	// Open all the Partitions.
	for _, p := range i.partitions {
		if err := p.Open(); err != nil {
			return err
		}
	}

	// Mark opened.
	i.opened = true

	return nil
}

// Wait blocks until all outstanding compactions have completed.
func (i *Index) Wait() {
	for _, p := range i.partitions {
		p.Wait()
	}
}

// Close closes the index.
func (i *Index) Close() error {
	// Lock index and close partitions.
	i.mu.Lock()
	defer i.mu.Unlock()

	// TODO(edd): Close Partitions.
	for _, p := range i.partitions {
		if err := p.Close(); err != nil {
			return err
		}
	}

	return nil
}

// partition returns the appropriate Partition for a provided series key.
func (i *Index) partition(key []byte) *Partition {
	return i.partitions[int(xxhash.Sum64(key)&TotalPartitions)]
}

// availableThreads returns the minimum of GOMAXPROCS and the number of
// partitions in the Index.
func (i *Index) availableThreads() int {
	n := runtime.GOMAXPROCS(0)
	if len(i.partitions) < n {
		return len(i.partitions)
	}
	return n
}

// RetainFileSet returns the current fileset for all partitions in the Index.
func (i *Index) RetainFileSet() *FileSet {
	// TODO(edd): Merge all FileSets for all partitions. For the moment we will
	// just append them from each partition.
	return nil
}

// SetFieldSet sets a shared field set from the engine.
func (i *Index) SetFieldSet(fs *tsdb.MeasurementFieldSet) {
	i.mu.Lock()
	// TODO(edd): set the field set on all the Partitions?
	// since fs is a pointer, it probably can't be shared...
	i.mu.Unlock()
}

// ForEachMeasurementName iterates over all measurement names in the index,
// applying fn. Note, the provided function may be called concurrently, and it
// must be safe to do so.
//
// It returns the first error encountered, if any.
func (i *Index) ForEachMeasurementName(fn func(name []byte) error) error {
	n := i.availableThreads()

	// Store results.
	errC := make(chan error, n)

	// Run fn on each partition using a fixed number of goroutines.
	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx > len(i.partitions) {
					return // No more work.
				}
				errC <- i.partitions[idx].ForEachMeasurementName(fn)
			}
		}()
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return err
		}
	}
	return nil
}

// MeasurementExists returns true if a measurement exists.
func (i *Index) MeasurementExists(name []byte) (bool, error) {
	n := i.availableThreads()

	// Store errors
	var found uint32 // Use this to signal we found the measurement.
	errC := make(chan error, n)

	// Check each partition for the measurement concurrently.
	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to check
				if idx > len(i.partitions) {
					return // No more work.
				}

				// Check if the measurement has been found. If it has don't
				// need to check this partition and can just move on.
				if atomic.LoadUint32(&found) == 1 {
					errC <- nil
					continue
				}

				b, err := i.partitions[idx].MeasurementExists(name)
				errC <- err
				if b {
					atomic.StoreUint32(&found, 1)
				}
			}
		}()
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return false, err
		}
	}

	// Check if we found the measurement.
	return atomic.LoadUint32(&found) == 1, nil
}

// MeasurementNamesByExpr returns measurement names for the provided expression.
func (i *Index) MeasurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
	// TODO(edd): Call on each partition. Merge results.
	return nil, nil
}

// MeasurementNamesByRegex returns measurement names for the provided regex.
func (i *Index) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	// TODO(edd): Call on each Partition. Merge results.
	return nil, nil
}

// DropMeasurement deletes a measurement from the index. It returns the first
// error encountered, if any.
func (i *Index) DropMeasurement(name []byte) error {
	n := i.availableThreads()

	// Store results.
	errC := make(chan error, n)

	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx > len(i.partitions) {
					return // No more work.
				}
				errC <- i.partitions[idx].DropMeasurement(name)
			}
		}()
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return err
		}
	}
	return nil
}

// CreateSeriesListIfNotExists creates a list of series if they doesn't exist in bulk.
func (i *Index) CreateSeriesListIfNotExists(_, names [][]byte, tagsSlice []models.Tags) error {
	// TODO(edd): Call on correct Partition.
	return nil
}

// CreateSeriesIfNotExists creates a series if it doesn't exist or is deleted.
func (i *Index) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	// TODO(edd): Call on correct Partition.
	return nil
}

// DropSeries drops the provided series from the index.
func (i *Index) DropSeries(key []byte) error {
	// TODO(edd): Call on correct Partition.
	return nil
}

// MeasurementsSketches returns the two sketches for the index by merging all
// instances of the type sketch types in all the index files.
func (i *Index) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	// TODO(edd): Merge sketches for each partition.
	return nil, nil, nil
}

// SeriesN returns the number of unique non-tombstoned series in the index.
// Since indexes are not shared across shards, the count returned by SeriesN
// cannot be combined with other shard's results. If you need to count series
// across indexes then use SeriesSketches and merge the results from other
// indexes.
func (i *Index) SeriesN() int64 {
	var total int64
	for _, p := range i.partitions {
		total += p.SeriesN()
	}
	return total
}

// HasTagKey returns true if tag key exists. It returns the first error
// encountered if any.
func (i *Index) HasTagKey(name, key []byte) (bool, error) {
	n := i.availableThreads()

	// Store errors
	var found uint32 // Use this to signal we found the tag key.
	errC := make(chan error, n)

	// Check each partition for the tag key concurrently.
	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to check
				if idx > len(i.partitions) {
					return // No more work.
				}

				// Check if the tag key has already been found. If it has, we
				// don't need to check this partition and can just move on.
				if atomic.LoadUint32(&found) == 1 {
					errC <- nil
					continue
				}

				b, err := i.partitions[idx].HasTagKey(name, key)
				errC <- err
				if b {
					atomic.StoreUint32(&found, 1)
				}
			}
		}()
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return false, err
		}
	}

	// Check if we found the tag key.
	return atomic.LoadUint32(&found) == 1, nil
}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (i *Index) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	// TODO(edd): Call on each partition. Merge results.
	return nil, nil
}

// MeasurementTagKeyValuesByExpr returns a set of tag values filtered by an expression.
//
// See tsm1.Engine.MeasurementTagKeyValuesByExpr for a fuller description of this
// method.
func (i *Index) MeasurementTagKeyValuesByExpr(name []byte, keys []string, expr influxql.Expr, keysSorted bool) ([][]string, error) {
	// TODO(edd): Merge results from each Partition.
	return nil, nil
}

// ForEachMeasurementTagKey iterates over all tag keys in a measurement and applies
// the provided function.
func (i *Index) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	n := i.availableThreads()

	// Store results.
	errC := make(chan error, n)

	// Run fn on each partition using a fixed number of goroutines.
	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx > len(i.partitions) {
					return // No more work.
				}
				errC <- i.partitions[idx].ForEachMeasurementTagKey(name, fn)
			}
		}()
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return err
		}
	}
	return nil
}

// TagKeyCardinality always returns zero.
// It is not possible to determine cardinality of tags across index files, and
// thus it cannot be done across partitions.
func (i *Index) TagKeyCardinality(name, key []byte) int {
	return 0
}

// MeasurementSeriesKeysByExpr returns a list of series keys matching expr.
func (i *Index) MeasurementSeriesKeysByExpr(name []byte, expr influxql.Expr) ([][]byte, error) {
	// TODO(edd): Merge results from each Partition.
	return nil, nil
}

// TagSets returns an ordered list of tag sets for a measurement by dimension
// and filtered by an optional conditional expression.
func (i *Index) TagSets(name []byte, opt query.IteratorOptions) ([]*query.TagSet, error) {
	// TODO(edd): Merge results from each Partition.
	return nil, nil
}

// SnapshotTo creates hard links to the file set into path.
func (i *Index) SnapshotTo(path string) error {
	// Store results.
	errC := make(chan error, len(i.partitions))
	for _, p := range i.partitions {
		go func(p *Partition) {
			errC <- p.SnapshotTo(path)
		}(p)
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return err
		}
	}
	return nil
}

// UnassignShard simply calls into DropSeries.
func (i *Index) UnassignShard(k string, shardID uint64) error {
	// This can be called directly once inmem is gone.
	return i.DropSeries([]byte(k))
}

// SeriesPointIterator returns an influxql iterator over all series.
func (i *Index) SeriesPointIterator(opt query.IteratorOptions) (query.Iterator, error) {
	// TODO(edd): Create iterators for each Partition and return a merged
	// iterator.
	return nil, nil
}

// Compact requests a compaction of log files in the index.
func (i *Index) Compact() {
	// Compact using half the available threads.
	// TODO(edd): this might need adjusting.
	n := runtime.GOMAXPROCS(0) / 2

	// Run fn on each partition using a fixed number of goroutines.
	var wg sync.WaitGroup
	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx > len(i.partitions) {
					return // No more work.
				}
				i.partitions[idx].Compact()
			}
		}()
	}

	// Wait for all partitions to complete compactions.
	wg.Wait()
}

// NO-OPS

// Rebuild is a no-op on tsi1.
func (i *Index) Rebuild() {}

// InitializeSeries is a no-op. This only applies to the in-memory index.
func (i *Index) InitializeSeries(key, name []byte, tags models.Tags) error { return nil }

// SetFieldName is a no-op on tsi1.
func (i *Index) SetFieldName(measurement []byte, name string) {}

// RemoveShard is a no-op on tsi1.
func (i *Index) RemoveShard(shardID uint64) {}

// AssignShard is a no-op on tsi1.
func (i *Index) AssignShard(k string, shardID uint64) {}
