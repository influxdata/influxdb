package tsi1

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/influxdata/influxdb/pkg/slices"

	"github.com/influxdata/influxdb/pkg/estimator/hll"

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

func init() {
	// FIXME(edd): Remove this.
	if os.Getenv("TSI_PARTITIONS") != "" {
		i, err := strconv.Atoi(os.Getenv("TSI_PARTITIONS"))
		if err != nil {
			panic(err)
		}
		TotalPartitions = uint64(i)
	}

	tsdb.RegisterIndex(IndexName, func(_ uint64, _, path string, _ tsdb.EngineOptions) tsdb.Index {
		idx := NewIndex(WithPath(path))
		return idx
	})
}

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
//
// FIXME(edd): This is variable for testing purposes during development.
var TotalPartitions uint64 = 16

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
		p.logger = idx.logger.With(zap.String("partition", fmt.Sprint(i+1)))

		idx.partitions[i] = p
	}
	return idx
}

// WithLogger sets the logger on the index after it's been created.
//
// It's not safe to call WithLogger after the index has been opened, or before
// it has been closed.
func (i *Index) WithLogger(l zap.Logger) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for i, p := range i.partitions {
		p.logger = l.With(zap.String("index", "tsi"), zap.String("partition", fmt.Sprint(i+1)))
	}
	i.logger = l.With(zap.String("index", "tsi"))
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

	// Open all the Partitions in parallel.
	partitionN := len(i.partitions)
	n := i.availableThreads()

	// Store results.
	errC := make(chan error, partitionN)

	// Run fn on each partition using a fixed number of goroutines.
	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func(k int) {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx >= partitionN {
					return // No more work.
				}
				errC <- i.partitions[idx].Open()
			}
		}(k)
	}

	// Check for error
	for i := 0; i < partitionN; i++ {
		if err := <-errC; err != nil {
			return err
		}
	}

	// Mark opened.
	i.opened = true
	i.logger.Info(fmt.Sprintf("index opened with %d partitions", partitionN))
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
	return i.partitions[int(xxhash.Sum64(key)&(TotalPartitions-1))]
}

// partitionIdx returns the index of the partition that key belongs in.
func (i *Index) partitionIdx(key []byte) int {
	return int(xxhash.Sum64(key) & (TotalPartitions - 1))
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
	panic("TODO(edd)")
}

// SetFieldSet sets a shared field set from the engine.
func (i *Index) SetFieldSet(fs *tsdb.MeasurementFieldSet) {
	for _, p := range i.partitions {
		p.SetFieldSet(fs)
	}
}

// ForEachMeasurementName iterates over all measurement names in the index,
// applying fn. Note, the provided function may be called concurrently, and it
// must be safe to do so.
//
// It returns the first error encountered, if any.
func (i *Index) ForEachMeasurementName(fn func(name []byte) error) error {
	n := i.availableThreads()

	// Store results.
	errC := make(chan error, TotalPartitions)

	// Run fn on each partition using a fixed number of goroutines.
	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx >= len(i.partitions) {
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
	errC := make(chan error, TotalPartitions)

	// Check each partition for the measurement concurrently.
	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to check
				if idx >= len(i.partitions) {
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

// fetchByteValues is a helper for gathering values from each partition in the index,
// based on some criteria.
//
// fn is a function that works on partition idx and calls into some method on
// the partition that returns some ordered values.
func (i *Index) fetchByteValues(fn func(idx int) ([][]byte, error)) ([][]byte, error) {
	n := i.availableThreads()

	// Store results.
	names := make([][][]byte, TotalPartitions)
	errC := make(chan error, TotalPartitions)

	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx >= len(i.partitions) {
					return // No more work.
				}

				pnames, err := fn(idx)

				// This is safe since there are no readers on names until all
				// the writers are done.
				names[idx] = pnames
				errC <- err
			}
		}()
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return nil, err
		}
	}

	// It's now safe to read from names.
	return slices.MergeSortedBytes(names[:]...), nil
}

// MeasurementNamesByExpr returns measurement names for the provided expression.
func (i *Index) MeasurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
	return i.fetchByteValues(func(idx int) ([][]byte, error) {
		return i.partitions[idx].MeasurementNamesByExpr(expr)
	})
}

// MeasurementNamesByRegex returns measurement names for the provided regex.
func (i *Index) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	return i.fetchByteValues(func(idx int) ([][]byte, error) {
		return i.partitions[idx].MeasurementNamesByRegex(re)
	})
}

// MeasurementSeriesKeysByExpr returns a list of series keys matching expr.
func (i *Index) MeasurementSeriesKeysByExpr(name []byte, expr influxql.Expr) ([][]byte, error) {
	return i.fetchByteValues(func(idx int) ([][]byte, error) {
		return i.partitions[idx].MeasurementSeriesKeysByExpr(name, expr)
	})
}

// DropMeasurement deletes a measurement from the index. It returns the first
// error encountered, if any.
func (i *Index) DropMeasurement(name []byte) error {
	n := i.availableThreads()

	// Store results.
	errC := make(chan error, TotalPartitions)

	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx >= len(i.partitions) {
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
func (i *Index) CreateSeriesListIfNotExists(keys [][]byte, names [][]byte, tagsSlice []models.Tags) error {
	// All slices must be of equal length.
	if len(names) != len(tagsSlice) {
		return errors.New("names/tags length mismatch in index")
	}

	// We need to move different series into collections for each partition
	// to process.
	pNames := make([][][]byte, TotalPartitions)
	pTags := make([][]models.Tags, TotalPartitions)

	// determine appropriate where series shoud live using each series key.
	for k, key := range keys {
		pidx := i.partitionIdx(key)
		pNames[pidx] = append(pNames[pidx], names[k])
		pTags[pidx] = append(pTags[pidx], tagsSlice[k])
	}

	// Process each subset of series on each partition.
	n := i.availableThreads()

	// Store errors.
	errC := make(chan error, TotalPartitions)

	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx >= len(i.partitions) {
					return // No more work.
				}
				errC <- i.partitions[idx].createSeriesListIfNotExists(pNames[idx], pTags[idx])
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

// CreateSeriesIfNotExists creates a series if it doesn't exist or is deleted.
func (i *Index) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	return i.partition(key).createSeriesListIfNotExists([][]byte{name}, []models.Tags{tags})
}

// DropSeries drops the provided series from the index.
func (i *Index) DropSeries(key []byte) error {
	return i.partition(key).DropSeries(key)
}

// MeasurementsSketches returns the two sketches for the index by merging all
// instances of the type sketch types in all the index files.
func (i *Index) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	s, ts := hll.NewDefaultPlus(), hll.NewDefaultPlus()
	for _, p := range i.partitions {
		// Get partition's measurement sketches and merge.
		ps, pts, err := p.MeasurementsSketches()
		if err != nil {
			return nil, nil, err
		}

		if err := s.Merge(ps); err != nil {
			return nil, nil, err
		}

		if err := ts.Merge(pts); err != nil {
			return nil, nil, err
		}
	}

	return s, ts, nil
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
	errC := make(chan error, TotalPartitions)

	// Check each partition for the tag key concurrently.
	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to check
				if idx >= len(i.partitions) {
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
	n := i.availableThreads()

	// Store results.
	keys := make([]map[string]struct{}, TotalPartitions)
	errC := make(chan error, TotalPartitions)

	var pidx uint32 // Index of maximum Partition being worked on.
	var err error
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx >= len(i.partitions) {
					return // No more work.
				}

				// This is safe since there are no readers on keys until all
				// the writers are done.
				keys[idx], err = i.partitions[idx].MeasurementTagKeysByExpr(name, expr)
				errC <- err
			}
		}()
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return nil, err
		}
	}

	// Merge into single map.
	result := keys[0]
	for k := 1; k < len(i.partitions); k++ {
		for k := range keys[k] {
			result[k] = struct{}{}
		}
	}
	return result, nil
}

// MeasurementTagKeyValuesByExpr returns a set of tag values filtered by an expression.
//
// See tsm1.Engine.MeasurementTagKeyValuesByExpr for a fuller description of this
// method.
func (i *Index) MeasurementTagKeyValuesByExpr(name []byte, keys []string, expr influxql.Expr, keysSorted bool) ([][]string, error) {
	panic("TODO(edd)")
}

// ForEachMeasurementTagKey iterates over all tag keys in a measurement and applies
// the provided function.
func (i *Index) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	n := i.availableThreads()

	// Store results.
	errC := make(chan error, TotalPartitions)

	// Run fn on each partition using a fixed number of goroutines.
	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx >= len(i.partitions) {
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

// TagSets returns an ordered list of tag sets for a measurement by dimension
// and filtered by an optional conditional expression.
func (i *Index) TagSets(name []byte, opt query.IteratorOptions) ([]*query.TagSet, error) {
	n := i.availableThreads()

	// Store results.
	sets := make([][]*query.TagSet, TotalPartitions)
	errC := make(chan error, TotalPartitions)

	// Run fn on each partition using a fixed number of goroutines.
	var pidx uint32 // Index of maximum Partition being worked on.
	var err error

	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx >= len(i.partitions) {
					return // No more work.
				}

				// This is safe since there are no readers on sets until all
				// the writers are done.
				sets[idx], err = i.partitions[idx].TagSets(name, opt)
				errC <- err
			}
		}()
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return nil, err
		}
	}

	// TODO(edd): Merge results from each Partition.
	panic("TODO(edd)")
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
	// FIXME(edd): This needs implementing.
	itrs := make([]*seriesPointIterator, len(i.partitions))
	var err error
	for k, p := range i.partitions {
		if itrs[k], err = p.seriesPointIterator(opt); err != nil {
			return nil, err
		}
	}

	return MergeSeriesPointIterators(itrs...), nil
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
				if idx >= len(i.partitions) {
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
