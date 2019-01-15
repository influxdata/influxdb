package tsi1

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/slices"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// ErrCompactionInterrupted is returned if compactions are disabled or
// an index is closed while a compaction is occurring.
var ErrCompactionInterrupted = errors.New("tsi1: compaction interrupted")

func init() {
	if os.Getenv("INFLUXDB_EXP_TSI_PARTITIONS") != "" {
		i, err := strconv.Atoi(os.Getenv("INFLUXDB_EXP_TSI_PARTITIONS"))
		if err != nil {
			panic(err)
		}
		DefaultPartitionN = uint64(i)
	}
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

// DisableFsync disables flushing and syncing of underlying files. Primarily this
// impacts the LogFiles. This option can be set when working with the index in
// an offline manner, for cases where a hard failure can be overcome by re-running the tooling.
var DisableFsync = func() IndexOption {
	return func(i *Index) {
		i.disableFsync = true
	}
}

// WithLogFileBufferSize sets the size of the buffer used within LogFiles.
// Typically appending an entry to a LogFile involves writing 11 or 12 bytes, so
// depending on how many new series are being created within a batch, it may
// be appropriate to set this.
var WithLogFileBufferSize = func(sz int) IndexOption {
	return func(i *Index) {
		if sz > 1<<17 { // 128K
			sz = 1 << 17
		} else if sz < 1<<12 {
			sz = 1 << 12 // 4K (runtime default)
		}
		i.logfileBufferSize = sz
	}
}

// DisableMetrics ensures that activity is not collected via the prometheus metrics.
// DisableMetrics must be called before Open.
var DisableMetrics = func() IndexOption {
	return func(i *Index) {
		i.metricsEnabled = false
	}
}

// Index represents a collection of layered index files and WAL.
type Index struct {
	mu         sync.RWMutex
	partitions []*Partition
	opened     bool

	defaultLabels prometheus.Labels

	tagValueCache    *TagValueSeriesIDCache
	partitionMetrics *partitionMetrics // Maintain a single set of partition metrics to be shared by partition.
	metricsEnabled   bool

	// The following may be set when initializing an Index.
	path               string      // Root directory of the index partitions.
	disableCompactions bool        // Initially disables compactions on the index.
	maxLogFileSize     int64       // Maximum size of a LogFile before it's compacted.
	logfileBufferSize  int         // The size of the buffer used by the LogFile.
	disableFsync       bool        // Disables flushing buffers and fsyning files. Used when working with indexes offline.
	logger             *zap.Logger // Index's logger.
	config             Config      // The index configuration

	// The following must be set when initializing an Index.
	sfile *tsdb.SeriesFile // series lookup file

	// Index's version.
	version int

	// Number of partitions used by the index.
	PartitionN uint64
}

func (i *Index) UniqueReferenceID() uintptr {
	return uintptr(unsafe.Pointer(i))
}

// NewIndex returns a new instance of Index.
func NewIndex(sfile *tsdb.SeriesFile, c Config, options ...IndexOption) *Index {
	idx := &Index{
		tagValueCache:    NewTagValueSeriesIDCache(c.SeriesIDSetCacheSize),
		partitionMetrics: newPartitionMetrics(nil),
		metricsEnabled:   true,
		maxLogFileSize:   int64(c.MaxIndexLogFileSize),
		logger:           zap.NewNop(),
		version:          Version,
		config:           c,
		sfile:            sfile,
		PartitionN:       DefaultPartitionN,
	}

	for _, option := range options {
		option(idx)
	}

	return idx
}

// SetDefaultMetricLabels sets the default labels on the trackers.
func (i *Index) SetDefaultMetricLabels(labels prometheus.Labels) {
	i.defaultLabels = make(prometheus.Labels, len(labels))
	for k, v := range labels {
		i.defaultLabels[k] = v
	}
}

// Bytes estimates the memory footprint of this Index, in bytes.
func (i *Index) Bytes() int {
	var b int
	i.mu.RLock()
	b += 24 // mu RWMutex is 24 bytes
	b += int(unsafe.Sizeof(i.partitions))
	for _, p := range i.partitions {
		b += int(unsafe.Sizeof(p)) + p.bytes()
	}
	b += int(unsafe.Sizeof(i.opened))
	b += int(unsafe.Sizeof(i.path)) + len(i.path)
	b += int(unsafe.Sizeof(i.disableCompactions))
	b += int(unsafe.Sizeof(i.maxLogFileSize))
	b += int(unsafe.Sizeof(i.logger))
	b += int(unsafe.Sizeof(i.sfile))
	// Do not count SeriesFile because it belongs to the code that constructed this Index.
	b += int(unsafe.Sizeof(i.version))
	b += int(unsafe.Sizeof(i.PartitionN))
	i.mu.RUnlock()
	return b
}

// WithLogger sets the logger on the index after it's been created.
//
// It's not safe to call WithLogger after the index has been opened, or before
// it has been closed.
func (i *Index) WithLogger(l *zap.Logger) {
	i.logger = l.With(zap.String("index", "tsi"))
}

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

	mmu.Lock()
	if cms == nil && i.metricsEnabled {
		cms = newCacheMetrics(i.defaultLabels)
	}
	if pms == nil && i.metricsEnabled {
		pms = newPartitionMetrics(i.defaultLabels)
	}
	mmu.Unlock()

	// Set the correct shared metrics on the cache
	i.tagValueCache.tracker = newCacheTracker(cms, i.defaultLabels)
	i.tagValueCache.tracker.enabled = i.metricsEnabled

	// Initialize index partitions.
	i.partitions = make([]*Partition, i.PartitionN)
	for j := 0; j < len(i.partitions); j++ {
		p := NewPartition(i.sfile, filepath.Join(i.path, fmt.Sprint(j)))
		p.MaxLogFileSize = i.maxLogFileSize
		p.nosync = i.disableFsync
		p.logbufferSize = i.logfileBufferSize
		p.logger = i.logger.With(zap.String("tsi1_partition", fmt.Sprint(j+1)))

		// Each of the trackers needs to be given slightly different default
		// labels to ensure the correct partition ids are set as labels.
		labels := make(prometheus.Labels, len(i.defaultLabels))
		for k, v := range i.defaultLabels {
			labels[k] = v
		}
		labels["index_partition"] = fmt.Sprint(j)
		p.tracker = newPartitionTracker(pms, labels)
		p.tracker.enabled = i.metricsEnabled
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
	i.logger.Info("Index opened", zap.Int("partitions", partitionN))
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

func (i *Index) MeasurementSeriesByExprIterator(name []byte, expr influxql.Expr) (tsdb.SeriesIDIterator, error) {
	return i.measurementSeriesByExprIterator(name, expr)
}

// measurementSeriesByExprIterator returns a series iterator for a measurement
// that is filtered by expr. See MeasurementSeriesByExprIterator for more details.
//
// measurementSeriesByExprIterator guarantees to never take any locks on the
// series file.
func (i *Index) measurementSeriesByExprIterator(name []byte, expr influxql.Expr) (tsdb.SeriesIDIterator, error) {
	// Return all series for the measurement if there are no tag expressions.

	release := i.sfile.Retain()
	defer release()

	if expr == nil {
		itr, err := i.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}
		return tsdb.FilterUndeletedSeriesIDIterator(i.sfile, itr), nil
	}

	itr, err := i.seriesByExprIterator(name, expr)
	if err != nil {
		return nil, err
	}

	return tsdb.FilterUndeletedSeriesIDIterator(i.sfile, itr), nil
}

// MeasurementSeriesIDIterator returns an iterator over all non-tombstoned series
// for the provided measurement.
func (i *Index) MeasurementSeriesIDIterator(name []byte) (tsdb.SeriesIDIterator, error) {
	itr, err := i.measurementSeriesIDIterator(name)
	if err != nil {
		return nil, err
	}

	release := i.sfile.Retain()
	defer release()
	return tsdb.FilterUndeletedSeriesIDIterator(i.sfile, itr), nil
}

// measurementSeriesIDIterator returns an iterator over all series in a measurement.
func (i *Index) measurementSeriesIDIterator(name []byte) (tsdb.SeriesIDIterator, error) {
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

	// Remove any cached bitmaps for the measurement.
	i.tagValueCache.DeleteMeasurement(name)

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return err
		}
	}
	return nil
}

// CreateSeriesListIfNotExists creates a list of series if they doesn't exist in bulk.
func (i *Index) CreateSeriesListIfNotExists(collection *tsdb.SeriesCollection) error {
	// Create the series list on the series file first. This validates all of the types for
	// the collection.
	err := i.sfile.CreateSeriesListIfNotExists(collection)
	if err != nil {
		return err
	}

	// We need to move different series into collections for each partition
	// to process.
	pCollections := make([]tsdb.SeriesCollection, i.PartitionN)

	// Determine partition for series using each series key.
	for iter := collection.Iterator(); iter.Next(); {
		pCollection := &pCollections[i.partitionIdx(iter.Key())]
		pCollection.Names = append(pCollection.Names, iter.Name())
		pCollection.Tags = append(pCollection.Tags, iter.Tags())
		pCollection.SeriesIDs = append(pCollection.SeriesIDs, iter.SeriesID())
	}

	// Process each subset of series on each partition.
	n := i.availableThreads()

	// Store errors.
	errC := make(chan error, i.PartitionN)

	var pidx uint32 // Index of maximum Partition being worked on.
	for k := 0; k < n; k++ {
		go func() {
			i.mu.RLock()
			partitionN := len(i.partitions)
			i.mu.RUnlock()

			for {
				idx := int(atomic.AddUint32(&pidx, 1) - 1) // Get next partition to work on.
				if idx >= partitionN {
					return // No more work.
				}

				i.mu.RLock()
				partition := i.partitions[idx]
				i.mu.RUnlock()

				ids, err := partition.createSeriesListIfNotExists(&pCollections[idx])
				if len(ids) == 0 {
					errC <- err
					continue
				}

				// Some cached bitset results may need to be updated.
				i.tagValueCache.RLock()
				for j, id := range ids {
					if id.IsZero() {
						continue
					}

					name := pCollections[idx].Names[j]
					tags := pCollections[idx].Tags[j]
					if i.tagValueCache.measurementContainsSets(name) {
						for _, pair := range tags {
							// TODO(edd): It's not clear to me yet whether it will be better to take a lock
							// on every series id set, or whether to gather them all up under the cache rlock
							// and then take the cache lock and update them all at once (without invoking a lock
							// on each series id set).
							//
							// Taking the cache lock will block all queries, but is one lock. Taking each series set
							// lock might be many lock/unlocks but will only block a query that needs that particular set.
							//
							// Need to think on it, but I think taking a lock on each series id set is the way to go.
							//
							// One other option here is to take a lock on the series id set when we first encounter it
							// and then keep it locked until we're done with all the ids.
							//
							// Note: this will only add `id` to the set if it exists.
							i.tagValueCache.addToSet(name, pair.Key, pair.Value, id) // Takes a lock on the series id set
						}
					}
				}
				i.tagValueCache.RUnlock()

				errC <- err
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

// InitializeSeries is a no-op. This only applies to the in-memory index.
func (i *Index) InitializeSeries(*tsdb.SeriesCollection) error {
	return nil
}

// DropSeries drops the provided series from the index.  If cascade is true
// and this is the last series to the measurement, the measurment will also be dropped.
func (i *Index) DropSeries(seriesID tsdb.SeriesID, key []byte, cascade bool) error {
	// Remove from partition.
	if err := i.partition(key).DropSeries(seriesID); err != nil {
		return err
	}

	if !cascade {
		return nil
	}

	// Extract measurement name & tags.
	name, tags := models.ParseKeyBytes(key)

	// If there are cached sets for any of the tag pairs, they will need to be
	// updated with the series id.
	i.tagValueCache.RLock()
	if i.tagValueCache.measurementContainsSets(name) {
		for _, pair := range tags {
			i.tagValueCache.delete(name, pair.Key, pair.Value, seriesID) // Takes a lock on the series id set
		}
	}
	i.tagValueCache.RUnlock()

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

// DropSeriesGlobal is a no-op on the tsi1 index.
func (i *Index) DropSeriesGlobal(key []byte) error { return nil }

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

// SeriesN returns the series cardinality in the index. It is the sum of all
// partition cardinalities.
func (i *Index) SeriesN() int64 {
	var total int64
	for _, p := range i.partitions {
		total += int64(p.seriesIDSet.Cardinality())
	}
	return total
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
	release := i.sfile.Retain()
	defer release()

	itr, err := i.tagKeySeriesIDIterator(name, key)
	if err != nil {
		return nil, err
	}
	return tsdb.FilterUndeletedSeriesIDIterator(i.sfile, itr), nil
}

// tagKeySeriesIDIterator returns a series iterator for all values across a single key.
func (i *Index) tagKeySeriesIDIterator(name, key []byte) (tsdb.SeriesIDIterator, error) {
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
	release := i.sfile.Retain()
	defer release()

	itr, err := i.tagValueSeriesIDIterator(name, key, value)
	if err != nil {
		return nil, err
	}
	return tsdb.FilterUndeletedSeriesIDIterator(i.sfile, itr), nil
}

// tagValueSeriesIDIterator returns a series iterator for a single tag value.
func (i *Index) tagValueSeriesIDIterator(name, key, value []byte) (tsdb.SeriesIDIterator, error) {
	// Check series ID set cache...
	if i.config.SeriesIDSetCacheSize > 0 { // Cache enabled.
		if ss := i.tagValueCache.Get(name, key, value); ss != nil {
			// Return a clone because the set is mutable.
			return tsdb.NewSeriesIDSetIterator(ss.Clone()), nil
		}
	}

	a := make([]tsdb.SeriesIDIterator, 0, len(i.partitions))
	for _, p := range i.partitions {
		itr, err := p.TagValueSeriesIDIterator(name, key, value)
		if err != nil {
			return nil, err
		} else if itr != nil {
			a = append(a, itr)
		}
	}

	itr := tsdb.MergeSeriesIDIterators(a...)
	if i.config.SeriesIDSetCacheSize == 0 { // Cache disabled.
		return itr, nil
	}

	// Check if the iterator contains only series id sets. Cache them...
	if ssitr, ok := itr.(tsdb.SeriesIDSetIterator); ok {
		ss := ssitr.SeriesIDSet()
		ss.SetCOW(true) // This is important to speed the clone up.
		i.tagValueCache.Put(name, key, value, ss)
	}
	return itr, nil
}

func (i *Index) TagSets(name []byte, opt query.IteratorOptions) ([]*query.TagSet, error) {
	release := i.sfile.Retain()
	defer release()

	itr, err := i.MeasurementSeriesByExprIterator(name, opt.Condition)
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}
	defer itr.Close()
	// measurementSeriesByExprIterator filters deleted series IDs; no need to
	// do so here.

	var dims []string
	if len(opt.Dimensions) > 0 {
		dims = make([]string, len(opt.Dimensions))
		copy(dims, opt.Dimensions)
		sort.Strings(dims)
	}

	// For every series, get the tag values for the requested tag keys i.e.
	// dimensions. This is the TagSet for that series. Series with the same
	// TagSet are then grouped together, because for the purpose of GROUP BY
	// they are part of the same composite series.
	tagSets := make(map[string]*query.TagSet, 64)
	var seriesN, maxSeriesN int

	if opt.MaxSeriesN > 0 {
		maxSeriesN = opt.MaxSeriesN
	} else {
		maxSeriesN = int(^uint(0) >> 1)
	}

	// The tag sets require a string for each series key in the set, The series
	// file formatted keys need to be parsed into models format. Since they will
	// end up as strings we can re-use an intermediate buffer for this process.
	var keyBuf []byte
	var tagsBuf models.Tags // Buffer for tags. Tags are not needed outside of each loop iteration.
	for {
		se, err := itr.Next()
		if err != nil {
			return nil, err
		} else if se.SeriesID.IsZero() {
			break
		}

		// Skip if the series has been tombstoned.
		key := i.sfile.SeriesKey(se.SeriesID)
		if len(key) == 0 {
			continue
		}

		if seriesN&0x3fff == 0x3fff {
			// check every 16384 series if the query has been canceled
			select {
			case <-opt.InterruptCh:
				return nil, query.ErrQueryInterrupted
			default:
			}
		}

		if seriesN > maxSeriesN {
			return nil, fmt.Errorf("max-select-series limit exceeded: (%d/%d)", seriesN, opt.MaxSeriesN)
		}

		// NOTE - must not escape this loop iteration.
		_, tagsBuf = tsdb.ParseSeriesKeyInto(key, tagsBuf)
		var tagsAsKey []byte
		if len(dims) > 0 {
			tagsAsKey = tsdb.MakeTagsKey(dims, tagsBuf)
		}

		tagSet, ok := tagSets[string(tagsAsKey)]
		if !ok {
			// This TagSet is new, create a new entry for it.
			tagSet = &query.TagSet{
				Tags: nil,
				Key:  tagsAsKey,
			}
		}

		// Associate the series and filter with the Tagset.
		keyBuf = models.AppendMakeKey(keyBuf, name, tagsBuf)
		tagSet.AddFilter(string(keyBuf), se.Expr)
		keyBuf = keyBuf[:0]

		// Ensure it's back in the map.
		tagSets[string(tagsAsKey)] = tagSet
		seriesN++
	}

	// Sort the series in each tag set.
	for _, t := range tagSets {
		sort.Sort(t)
	}

	// The TagSets have been created, as a map of TagSets. Just send
	// the values back as a slice, sorting for consistency.
	sortedTagsSets := make([]*query.TagSet, 0, len(tagSets))
	for _, v := range tagSets {
		sortedTagsSets = append(sortedTagsSets, v)
	}
	sort.Sort(tsdb.ByTagKey(sortedTagsSets))

	return sortedTagsSets, nil
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

	fs, _ := NewFileSet(nil, i.sfile, nil)
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

// MeasurementCardinalityStats returns cardinality stats for all measurements.
func (i *Index) MeasurementCardinalityStats() MeasurementCardinalityStats {
	i.mu.RLock()
	defer i.mu.RUnlock()

	stats := NewMeasurementCardinalityStats()
	for _, p := range i.partitions {
		stats.Add(p.MeasurementCardinalityStats())
	}
	return stats
}

func (i *Index) seriesByExprIterator(name []byte, expr influxql.Expr) (tsdb.SeriesIDIterator, error) {
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			// Get the series IDs and filter expressions for the LHS.
			litr, err := i.seriesByExprIterator(name, expr.LHS)
			if err != nil {
				return nil, err
			}

			// Get the series IDs and filter expressions for the RHS.
			ritr, err := i.seriesByExprIterator(name, expr.RHS)
			if err != nil {
				if litr != nil {
					litr.Close()
				}
				return nil, err
			}

			// Intersect iterators if expression is "AND".
			if expr.Op == influxql.AND {
				return tsdb.IntersectSeriesIDIterators(litr, ritr), nil
			}

			// Union iterators if expression is "OR".
			return tsdb.UnionSeriesIDIterators(litr, ritr), nil

		default:
			return i.seriesByBinaryExprIterator(name, expr)
		}

	case *influxql.ParenExpr:
		return i.seriesByExprIterator(name, expr.Expr)

	case *influxql.BooleanLiteral:
		if expr.Val {
			return i.measurementSeriesIDIterator(name)
		}
		return nil, nil

	default:
		return nil, nil
	}
}

// seriesByBinaryExprIterator returns a series iterator and a filtering expression.
func (i *Index) seriesByBinaryExprIterator(name []byte, n *influxql.BinaryExpr) (tsdb.SeriesIDIterator, error) {
	// If this binary expression has another binary expression, then this
	// is some expression math and we should just pass it to the underlying query.
	if _, ok := n.LHS.(*influxql.BinaryExpr); ok {
		itr, err := i.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}
		return tsdb.NewSeriesIDExprIterator(itr, n), nil
	} else if _, ok := n.RHS.(*influxql.BinaryExpr); ok {
		itr, err := i.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}
		return tsdb.NewSeriesIDExprIterator(itr, n), nil
	}

	// Retrieve the variable reference from the correct side of the expression.
	key, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		key, ok = n.RHS.(*influxql.VarRef)
		if !ok {
			// This is an expression we do not know how to evaluate. Let the
			// query engine take care of this.
			itr, err := i.measurementSeriesIDIterator(name)
			if err != nil {
				return nil, err
			}
			return tsdb.NewSeriesIDExprIterator(itr, n), nil
		}
		value = n.LHS
	}

	// For fields, return all series from this measurement.
	if key.Val != "_name" && (key.Type == influxql.AnyField || (key.Type != influxql.Tag && key.Type != influxql.Unknown)) {
		itr, err := i.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}
		return tsdb.NewSeriesIDExprIterator(itr, n), nil
	} else if value, ok := value.(*influxql.VarRef); ok {
		// Check if the RHS is a variable and if it is a field.
		if value.Val != "_name" && (key.Type == influxql.AnyField || (value.Type != influxql.Tag && value.Type != influxql.Unknown)) {
			itr, err := i.measurementSeriesIDIterator(name)
			if err != nil {
				return nil, err
			}
			return tsdb.NewSeriesIDExprIterator(itr, n), nil
		}
	}

	// Create iterator based on value type.
	switch value := value.(type) {
	case *influxql.StringLiteral:
		return i.seriesByBinaryExprStringIterator(name, []byte(key.Val), []byte(value.Val), n.Op)
	case *influxql.RegexLiteral:
		return i.seriesByBinaryExprRegexIterator(name, []byte(key.Val), value.Val, n.Op)
	case *influxql.VarRef:
		return i.seriesByBinaryExprVarRefIterator(name, []byte(key.Val), value, n.Op)
	default:
		// We do not know how to evaluate this expression so pass it
		// on to the query engine.
		itr, err := i.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}
		return tsdb.NewSeriesIDExprIterator(itr, n), nil
	}
}

func (i *Index) seriesByBinaryExprStringIterator(name, key, value []byte, op influxql.Token) (tsdb.SeriesIDIterator, error) {
	// Special handling for "_name" to match measurement name.
	if bytes.Equal(key, []byte("_name")) {
		if (op == influxql.EQ && bytes.Equal(value, name)) || (op == influxql.NEQ && !bytes.Equal(value, name)) {
			return i.measurementSeriesIDIterator(name)
		}
		return nil, nil
	}

	if op == influxql.EQ {
		// Match a specific value.
		if len(value) != 0 {
			return i.tagValueSeriesIDIterator(name, key, value)
		}

		mitr, err := i.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}

		kitr, err := i.tagKeySeriesIDIterator(name, key)
		if err != nil {
			if mitr != nil {
				mitr.Close()
			}
			return nil, err
		}

		// Return all measurement series that have no values from this tag key.
		return tsdb.DifferenceSeriesIDIterators(mitr, kitr), nil
	}

	// Return all measurement series without this tag value.
	if len(value) != 0 {
		mitr, err := i.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}

		vitr, err := i.tagValueSeriesIDIterator(name, key, value)
		if err != nil {
			if mitr != nil {
				mitr.Close()
			}
			return nil, err
		}

		return tsdb.DifferenceSeriesIDIterators(mitr, vitr), nil
	}

	// Return all series across all values of this tag key.
	return i.tagKeySeriesIDIterator(name, key)
}

func (i *Index) seriesByBinaryExprRegexIterator(name, key []byte, value *regexp.Regexp, op influxql.Token) (tsdb.SeriesIDIterator, error) {
	// Special handling for "_name" to match measurement name.
	if bytes.Equal(key, []byte("_name")) {
		match := value.Match(name)
		if (op == influxql.EQREGEX && match) || (op == influxql.NEQREGEX && !match) {
			mitr, err := i.measurementSeriesIDIterator(name)
			if err != nil {
				return nil, err
			}
			return tsdb.NewSeriesIDExprIterator(mitr, &influxql.BooleanLiteral{Val: true}), nil
		}
		return nil, nil
	}
	return i.matchTagValueSeriesIDIterator(name, key, value, op == influxql.EQREGEX)
}

func (i *Index) seriesByBinaryExprVarRefIterator(name, key []byte, value *influxql.VarRef, op influxql.Token) (tsdb.SeriesIDIterator, error) {
	itr0, err := i.tagKeySeriesIDIterator(name, key)
	if err != nil {
		return nil, err
	}

	itr1, err := i.tagKeySeriesIDIterator(name, []byte(value.Val))
	if err != nil {
		if itr0 != nil {
			itr0.Close()
		}
		return nil, err
	}

	if op == influxql.EQ {
		return tsdb.IntersectSeriesIDIterators(itr0, itr1), nil
	}
	return tsdb.DifferenceSeriesIDIterators(itr0, itr1), nil
}

// MatchTagValueSeriesIDIterator returns a series iterator for tags which match value.
// If matches is false, returns iterators which do not match value.
func (i *Index) MatchTagValueSeriesIDIterator(name, key []byte, value *regexp.Regexp, matches bool) (tsdb.SeriesIDIterator, error) {
	release := i.sfile.Retain()
	defer release()

	itr, err := i.matchTagValueSeriesIDIterator(name, key, value, matches)
	if err != nil {
		return nil, err
	}
	return tsdb.FilterUndeletedSeriesIDIterator(i.sfile, itr), nil
}

// matchTagValueSeriesIDIterator returns a series iterator for tags which match
// value. See MatchTagValueSeriesIDIterator for more details.
//
// It guarantees to never take any locks on the underlying series file.
func (i *Index) matchTagValueSeriesIDIterator(name, key []byte, value *regexp.Regexp, matches bool) (tsdb.SeriesIDIterator, error) {
	matchEmpty := value.MatchString("")
	if matches {
		if matchEmpty {
			return i.matchTagValueEqualEmptySeriesIDIterator(name, key, value)
		}
		return i.matchTagValueEqualNotEmptySeriesIDIterator(name, key, value)
	}

	if matchEmpty {
		return i.matchTagValueNotEqualEmptySeriesIDIterator(name, key, value)
	}
	return i.matchTagValueNotEqualNotEmptySeriesIDIterator(name, key, value)
}

func (i *Index) matchTagValueEqualEmptySeriesIDIterator(name, key []byte, value *regexp.Regexp) (tsdb.SeriesIDIterator, error) {
	vitr, err := i.TagValueIterator(name, key)
	if err != nil {
		return nil, err
	} else if vitr == nil {
		return i.measurementSeriesIDIterator(name)
	}
	defer vitr.Close()

	var itrs []tsdb.SeriesIDIterator
	if err := func() error {
		for {
			e, err := vitr.Next()
			if err != nil {
				return err
			} else if e == nil {
				break
			}

			if !value.Match(e) {
				itr, err := i.tagValueSeriesIDIterator(name, key, e)
				if err != nil {
					return err
				} else if itr != nil {
					itrs = append(itrs, itr)
				}
			}
		}
		return nil
	}(); err != nil {
		tsdb.SeriesIDIterators(itrs).Close()
		return nil, err
	}

	mitr, err := i.measurementSeriesIDIterator(name)
	if err != nil {
		tsdb.SeriesIDIterators(itrs).Close()
		return nil, err
	}

	return tsdb.DifferenceSeriesIDIterators(mitr, tsdb.MergeSeriesIDIterators(itrs...)), nil
}

func (i *Index) matchTagValueEqualNotEmptySeriesIDIterator(name, key []byte, value *regexp.Regexp) (tsdb.SeriesIDIterator, error) {
	vitr, err := i.TagValueIterator(name, key)
	if err != nil {
		return nil, err
	} else if vitr == nil {
		return nil, nil
	}
	defer vitr.Close()

	var itrs []tsdb.SeriesIDIterator
	for {
		e, err := vitr.Next()
		if err != nil {
			tsdb.SeriesIDIterators(itrs).Close()
			return nil, err
		} else if e == nil {
			break
		}

		if value.Match(e) {
			itr, err := i.tagValueSeriesIDIterator(name, key, e)
			if err != nil {
				tsdb.SeriesIDIterators(itrs).Close()
				return nil, err
			} else if itr != nil {
				itrs = append(itrs, itr)
			}
		}
	}
	return tsdb.MergeSeriesIDIterators(itrs...), nil
}

func (i *Index) matchTagValueNotEqualEmptySeriesIDIterator(name, key []byte, value *regexp.Regexp) (tsdb.SeriesIDIterator, error) {
	vitr, err := i.TagValueIterator(name, key)
	if err != nil {
		return nil, err
	} else if vitr == nil {
		return nil, nil
	}
	defer vitr.Close()

	var itrs []tsdb.SeriesIDIterator
	for {
		e, err := vitr.Next()
		if err != nil {
			tsdb.SeriesIDIterators(itrs).Close()
			return nil, err
		} else if e == nil {
			break
		}

		if !value.Match(e) {
			itr, err := i.tagValueSeriesIDIterator(name, key, e)
			if err != nil {
				tsdb.SeriesIDIterators(itrs).Close()
				return nil, err
			} else if itr != nil {
				itrs = append(itrs, itr)
			}
		}
	}
	return tsdb.MergeSeriesIDIterators(itrs...), nil
}

func (i *Index) matchTagValueNotEqualNotEmptySeriesIDIterator(name, key []byte, value *regexp.Regexp) (tsdb.SeriesIDIterator, error) {
	vitr, err := i.TagValueIterator(name, key)
	if err != nil {
		return nil, err
	} else if vitr == nil {
		return i.measurementSeriesIDIterator(name)
	}
	defer vitr.Close()

	var itrs []tsdb.SeriesIDIterator
	for {
		e, err := vitr.Next()
		if err != nil {
			tsdb.SeriesIDIterators(itrs).Close()
			return nil, err
		} else if e == nil {
			break
		}
		if value.Match(e) {
			itr, err := i.tagValueSeriesIDIterator(name, key, e)
			if err != nil {
				tsdb.SeriesIDIterators(itrs).Close()
				return nil, err
			} else if itr != nil {
				itrs = append(itrs, itr)
			}
		}
	}

	mitr, err := i.measurementSeriesIDIterator(name)
	if err != nil {
		tsdb.SeriesIDIterators(itrs).Close()
		return nil, err
	}
	return tsdb.DifferenceSeriesIDIterators(mitr, tsdb.MergeSeriesIDIterators(itrs...)), nil
}

// IsIndexDir returns true if directory contains at least one partition directory.
func IsIndexDir(path string) (bool, error) {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return false, err
	}
	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		} else if ok, err := IsPartitionDir(filepath.Join(path, fi.Name())); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}
	return false, nil
}
