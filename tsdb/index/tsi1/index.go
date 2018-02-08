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

	"github.com/cespare/xxhash"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/pkg/slices"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

// IndexName is the name of the index.
const IndexName = "tsi1"

// ErrCompactionInterrupted is returned if compactions are disabled or
// an index is closed while a compaction is occurring.
var ErrCompactionInterrupted = errors.New("tsi1: compaction interrupted")

func init() {
	// FIXME(edd): Remove this.
	if os.Getenv("TSI_PARTITIONS") != "" {
		i, err := strconv.Atoi(os.Getenv("TSI_PARTITIONS"))
		if err != nil {
			panic(err)
		}
		DefaultPartitionN = uint64(i)
	}

	tsdb.RegisterIndex(IndexName, func(_ uint64, db, path string, _ *tsdb.SeriesIDSet, sfile *tsdb.SeriesFile, _ tsdb.EngineOptions) tsdb.Index {
		idx := NewIndex(sfile, db, WithPath(path))
		return idx
	})
}

// DefaultPartitionN determines how many shards the index will be partitioned into.
//
// NOTE: Currently, this must not be change once a database is created. Further,
// it must also be a power of 2.
//
var DefaultPartitionN uint64 = 8

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

// WithMaximumLogFileSize sets the maximum size of LogFiles before they're
// compacted into IndexFiles.
var WithMaximumLogFileSize = func(size int64) IndexOption {
	return func(i *Index) {
		i.maxLogFileSize = size
	}
}

// Index represents a collection of layered index files and WAL.
type Index struct {
	mu         sync.RWMutex
	partitions []*Partition
	opened     bool

	// The following may be set when initializing an Index.
	path               string      // Root directory of the index partitions.
	disableCompactions bool        // Initially disables compactions on the index.
	maxLogFileSize     int64       // Maximum size of a LogFile before it's compacted.
	logger             *zap.Logger // Index's logger.

	// The following must be set when initializing an Index.
	sfile    *tsdb.SeriesFile // series lookup file
	database string           // Name of database.

	// Index's version.
	version int

	// Number of partitions used by the index.
	PartitionN uint64
}

// NewIndex returns a new instance of Index.
func NewIndex(sfile *tsdb.SeriesFile, database string, options ...IndexOption) *Index {
	idx := &Index{
		maxLogFileSize: DefaultMaxLogFileSize,
		logger:         zap.NewNop(),
		version:        Version,
		sfile:          sfile,
		database:       database,
		PartitionN:     DefaultPartitionN,
	}

	for _, option := range options {
		option(idx)
	}

	return idx
}

// Database returns the name of the database the index was initialized with.
func (i *Index) Database() string {
	return i.database
}

// WithLogger sets the logger on the index after it's been created.
//
// It's not safe to call WithLogger after the index has been opened, or before
// it has been closed.
func (i *Index) WithLogger(l *zap.Logger) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.logger = l.With(zap.String("index", "tsi"))
}

// Type returns the type of Index this is.
func (i *Index) Type() string { return IndexName }

// SeriesFile returns the series file attached to the index.
func (i *Index) SeriesFile() *tsdb.SeriesFile { return i.sfile }

// SeriesIDSet returns the set of series ids associated with series in this
// index. Any series IDs for series no longer present in the index are filtered out.
func (i *Index) SeriesIDSet() *tsdb.SeriesIDSet {
	seriesIDSet := tsdb.NewSeriesIDSet()
	others := make([]*tsdb.SeriesIDSet, 0, i.PartitionN)
	for _, p := range i.partitions {
		others = append(others, p.seriesIDSet)
	}
	seriesIDSet.Merge(others...)
	return seriesIDSet
}

// Open opens the index.
func (i *Index) Open() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.opened {
		return errors.New("index already open")
	}

	// Ensure root exists.
	if err := os.MkdirAll(i.path, 0777); err != nil {
		return err
	}

	// Initialize index partitions.
	i.partitions = make([]*Partition, i.PartitionN)
	for j := 0; j < len(i.partitions); j++ {
		p := NewPartition(i.sfile, filepath.Join(i.path, fmt.Sprint(j)))
		p.MaxLogFileSize = i.maxLogFileSize
		p.Database = i.database
		p.logger = i.logger.With(zap.String("index", "tsi"), zap.String("partition", fmt.Sprint(j+1)))
		i.partitions[j] = p
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
				err := i.partitions[idx].Open()
				errC <- err
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

// Compact requests a compaction of partitions.
func (i *Index) Compact() {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, p := range i.partitions {
		p.Compact()
	}
}

func (i *Index) EnableCompactions() {
	for _, p := range i.partitions {
		p.EnableCompactions()
	}
}

func (i *Index) DisableCompactions() {
	for _, p := range i.partitions {
		p.DisableCompactions()
	}
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

	for _, p := range i.partitions {
		if err := p.Close(); err != nil {
			return err
		}
	}

	// Mark index as closed.
	i.opened = false
	return nil
}

// Path returns the path the index was opened with.
func (i *Index) Path() string { return i.path }

// PartitionAt returns the partition by index.
func (i *Index) PartitionAt(index int) *Partition {
	return i.partitions[index]
}

// partition returns the appropriate Partition for a provided series key.
func (i *Index) partition(key []byte) *Partition {
	return i.partitions[int(xxhash.Sum64(key)&(i.PartitionN-1))]
}

// partitionIdx returns the index of the partition that key belongs in.
func (i *Index) partitionIdx(key []byte) int {
	return int(xxhash.Sum64(key) & (i.PartitionN - 1))
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

// SetFieldSet sets a shared field set from the engine.
func (i *Index) SetFieldSet(fs *tsdb.MeasurementFieldSet) {
	for _, p := range i.partitions {
		p.SetFieldSet(fs)
	}
}

// FieldSet returns the assigned fieldset.
func (i *Index) FieldSet() *tsdb.MeasurementFieldSet {
	if len(i.partitions) == 0 {
		return nil
	}
	return i.partitions[0].FieldSet()
}

// ForEachMeasurementName iterates over all measurement names in the index,
// applying fn. It returns the first error encountered, if any.
//
// ForEachMeasurementName does not call fn on each partition concurrently so the
// call may provide a non-goroutine safe fn.
func (i *Index) ForEachMeasurementName(fn func(name []byte) error) error {
	itr, err := i.MeasurementIterator()
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}
	defer itr.Close()

	// Iterate over all measurements.
	for {
		e, err := itr.Next()
		if err != nil {
			return err
		} else if e == nil {
			break
		}

		if err := fn(e); err != nil {
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
	errC := make(chan error, i.PartitionN)

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
				if b {
					atomic.StoreUint32(&found, 1)
				}
				errC <- err
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

// MeasurementHasSeries returns true if a measurement has non-tombstoned series.
func (i *Index) MeasurementHasSeries(name []byte) (bool, error) {
	for _, p := range i.partitions {
		if v, err := p.MeasurementHasSeries(name); err != nil {
			return false, err
		} else if v {
			return true, nil
		}
	}
	return false, nil
}

// fetchByteValues is a helper for gathering values from each partition in the index,
// based on some criteria.
//
// fn is a function that works on partition idx and calls into some method on
// the partition that returns some ordered values.
func (i *Index) fetchByteValues(fn func(idx int) ([][]byte, error)) ([][]byte, error) {
	n := i.availableThreads()

	// Store results.
	names := make([][][]byte, i.PartitionN)
	errC := make(chan error, i.PartitionN)

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

// MeasurementIterator returns an iterator over all measurements.
func (i *Index) MeasurementIterator() (tsdb.MeasurementIterator, error) {
	itrs := make([]tsdb.MeasurementIterator, 0, len(i.partitions))
	for _, p := range i.partitions {
		itr, err := p.MeasurementIterator()
		if err != nil {
			tsdb.MeasurementIterators(itrs).Close()
			return nil, err
		} else if itr != nil {
			itrs = append(itrs, itr)
		}
	}
	return tsdb.MergeMeasurementIterators(itrs...), nil
}

// MeasurementSeriesIDIterator returns an iterator over all series in a measurement.
func (i *Index) MeasurementSeriesIDIterator(name []byte) (tsdb.SeriesIDIterator, error) {
	itrs := make([]tsdb.SeriesIDIterator, 0, len(i.partitions))
	for _, p := range i.partitions {
		itr, err := p.MeasurementSeriesIDIterator(name)
		if err != nil {
			tsdb.SeriesIDIterators(itrs).Close()
			return nil, err
		} else if itr != nil {
			itrs = append(itrs, itr)
		}
	}
	return tsdb.MergeSeriesIDIterators(itrs...), nil
}

// MeasurementNamesByRegex returns measurement names for the provided regex.
func (i *Index) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	return i.fetchByteValues(func(idx int) ([][]byte, error) {
		return i.partitions[idx].MeasurementNamesByRegex(re)
	})
}

// DropMeasurement deletes a measurement from the index. It returns the first
// error encountered, if any.
func (i *Index) DropMeasurement(name []byte) error {
	n := i.availableThreads()

	// Store results.
	errC := make(chan error, i.PartitionN)

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
	pNames := make([][][]byte, i.PartitionN)
	pTags := make([][]models.Tags, i.PartitionN)

	// Determine partition for series using each series key.
	for ki, key := range keys {
		pidx := i.partitionIdx(key)
		pNames[pidx] = append(pNames[pidx], names[ki])
		pTags[pidx] = append(pTags[pidx], tagsSlice[ki])
	}

	// Process each subset of series on each partition.
	n := i.availableThreads()

	// Store errors.
	errC := make(chan error, i.PartitionN)

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

// InitializeSeries is a no-op. This only applies to the in-memory index.
func (i *Index) InitializeSeries(key, name []byte, tags models.Tags) error {
	return nil
}

// DropSeries drops the provided series from the index.  If cascade is true
// and this is the last series to the measurement, the measurment will also be dropped.
func (i *Index) DropSeries(seriesID uint64, key []byte, cascade bool) error {
	// Remove from partition.
	if err := i.partition(key).DropSeries(seriesID); err != nil {
		return err
	}

	if !cascade {
		return nil
	}

	// Extract measurement name.
	name, _ := models.ParseKeyBytes(key)

	// Check if that was the last series for the measurement in the entire index.
	if ok, err := i.MeasurementHasSeries(name); err != nil {
		return err
	} else if ok {
		return nil
	}

	// If no more series exist in the measurement then delete the measurement.
	if err := i.DropMeasurement(name); err != nil {
		return err
	}
	return nil
}

// DropMeasurementIfSeriesNotExist drops a measurement only if there are no more
// series for the measurment.
func (i *Index) DropMeasurementIfSeriesNotExist(name []byte) error {
	// Check if that was the last series for the measurement in the entire index.
	if ok, err := i.MeasurementHasSeries(name); err != nil {
		return err
	} else if ok {
		return nil
	}

	// If no more series exist in the measurement then delete the measurement.
	return i.DropMeasurement(name)
}

// MeasurementsSketches returns the two sketches for the index by merging all
// instances of the type sketch types in all the partitions.
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
		} else if err := ts.Merge(pts); err != nil {
			return nil, nil, err
		}
	}

	return s, ts, nil
}

// SeriesSketches returns the two sketches for the index by merging all
// instances of the type sketch types in all the partitions.
func (i *Index) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	s, ts := hll.NewDefaultPlus(), hll.NewDefaultPlus()
	for _, p := range i.partitions {
		// Get partition's measurement sketches and merge.
		ps, pts, err := p.SeriesSketches()
		if err != nil {
			return nil, nil, err
		}

		if err := s.Merge(ps); err != nil {
			return nil, nil, err
		} else if err := ts.Merge(pts); err != nil {
			return nil, nil, err
		}
	}

	return s, ts, nil
}

// Since indexes are not shared across shards, the count returned by SeriesN
// cannot be combined with other shard's results. If you need to count series
// across indexes then use either the database-wide series file, or merge the
// index-level bitsets or sketches.
func (i *Index) SeriesN() int64 {
	return int64(i.SeriesIDSet().Cardinality())
}

// HasTagKey returns true if tag key exists. It returns the first error
// encountered if any.
func (i *Index) HasTagKey(name, key []byte) (bool, error) {
	n := i.availableThreads()

	// Store errors
	var found uint32 // Use this to signal we found the tag key.
	errC := make(chan error, i.PartitionN)

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
				if b {
					atomic.StoreUint32(&found, 1)
				}
				errC <- err
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

// HasTagValue returns true if tag value exists.
func (i *Index) HasTagValue(name, key, value []byte) (bool, error) {
	n := i.availableThreads()

	// Store errors
	var found uint32 // Use this to signal we found the tag key.
	errC := make(chan error, i.PartitionN)

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

				b, err := i.partitions[idx].HasTagValue(name, key, value)
				if b {
					atomic.StoreUint32(&found, 1)
				}
				errC <- err
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

// TagKeyIterator returns an iterator for all keys across a single measurement.
func (i *Index) TagKeyIterator(name []byte) (tsdb.TagKeyIterator, error) {
	a := make([]tsdb.TagKeyIterator, 0, len(i.partitions))
	for _, p := range i.partitions {
		itr := p.TagKeyIterator(name)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return tsdb.MergeTagKeyIterators(a...), nil
}

// TagValueIterator returns an iterator for all values across a single key.
func (i *Index) TagValueIterator(name, key []byte) (tsdb.TagValueIterator, error) {
	a := make([]tsdb.TagValueIterator, 0, len(i.partitions))
	for _, p := range i.partitions {
		itr := p.TagValueIterator(name, key)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return tsdb.MergeTagValueIterators(a...), nil
}

// TagKeySeriesIDIterator returns a series iterator for all values across a single key.
func (i *Index) TagKeySeriesIDIterator(name, key []byte) (tsdb.SeriesIDIterator, error) {
	a := make([]tsdb.SeriesIDIterator, 0, len(i.partitions))
	for _, p := range i.partitions {
		itr := p.TagKeySeriesIDIterator(name, key)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return tsdb.MergeSeriesIDIterators(a...), nil
}

// TagValueSeriesIDIterator returns a series iterator for a single tag value.
func (i *Index) TagValueSeriesIDIterator(name, key, value []byte) (tsdb.SeriesIDIterator, error) {
	a := make([]tsdb.SeriesIDIterator, 0, len(i.partitions))
	for _, p := range i.partitions {
		itr := p.TagValueSeriesIDIterator(name, key, value)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return tsdb.MergeSeriesIDIterators(a...), nil
}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (i *Index) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	n := i.availableThreads()

	// Store results.
	keys := make([]map[string]struct{}, i.PartitionN)
	errC := make(chan error, i.PartitionN)

	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx >= len(i.partitions) {
					return // No more work.
				}

				// This is safe since there are no readers on keys until all
				// the writers are done.
				tagKeys, err := i.partitions[idx].MeasurementTagKeysByExpr(name, expr)
				keys[idx] = tagKeys
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

// DiskSizeBytes returns the size of the index on disk.
func (i *Index) DiskSizeBytes() int64 {
	fs, err := i.RetainFileSet()
	if err != nil {
		i.logger.Warn("Index is closing down")
		return 0
	}
	defer fs.Release()

	var manifestSize int64
	// Get MANIFEST sizes from each partition.
	for _, p := range i.partitions {
		manifestSize += p.manifestSize
	}
	return fs.Size() + manifestSize
}

// TagKeyCardinality always returns zero.
// It is not possible to determine cardinality of tags across index files, and
// thus it cannot be done across partitions.
func (i *Index) TagKeyCardinality(name, key []byte) int {
	return 0
}

// RetainFileSet returns the set of all files across all partitions.
// This is only needed when all files need to be retained for an operation.
func (i *Index) RetainFileSet() (*FileSet, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	fs, _ := NewFileSet(i.database, nil, i.sfile, nil)
	for _, p := range i.partitions {
		pfs, err := p.RetainFileSet()
		if err != nil {
			fs.Close()
			return nil, err
		}
		fs.files = append(fs.files, pfs.files...)
	}
	return fs, nil
}

// SetFieldName is a no-op on this index.
func (i *Index) SetFieldName(measurement []byte, name string) {}

// Rebuild rebuilds an index. It's a no-op for this index.
func (i *Index) Rebuild() {}
