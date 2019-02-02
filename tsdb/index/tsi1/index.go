package tsi1

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

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
const IndexName = tsdb.TSI1IndexName

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

	tsdb.RegisterIndex(IndexName, func(_ uint64, db, path string, _ *tsdb.SeriesIDSet, sfile *tsdb.SeriesFile, opt tsdb.EngineOptions) tsdb.Index {
		idx := NewIndex(sfile, db,
			WithPath(path),
			WithMaximumLogFileSize(int64(opt.Config.MaxIndexLogFileSize)),
			WithSeriesIDCacheSize(opt.Config.SeriesIDSetCacheSize),
		)
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

// WithSeriesIDCacheSize sets the size of the series id set cache.
// If set to 0, then the cache is disabled.
var WithSeriesIDCacheSize = func(sz int) IndexOption {
	return func(i *Index) {
		i.tagValueCacheSize = sz
	}
}

// Index represents a collection of layered index files and WAL.
type Index struct {
	mu         sync.RWMutex
	partitions []*Partition
	opened     bool

	tagValueCache     *TagValueSeriesIDCache
	tagValueCacheSize int

	// The following may be set when initializing an Index.
	path               string      // Root directory of the index partitions.
	disableCompactions bool        // Initially disables compactions on the index.
	maxLogFileSize     int64       // Maximum size of a LogFile before it's compacted.
	logfileBufferSize  int         // The size of the buffer used by the LogFile.
	disableFsync       bool        // Disables flushing buffers and fsyning files. Used when working with indexes offline.
	logger             *zap.Logger // Index's logger.

	// The following must be set when initializing an Index.
	sfile    *tsdb.SeriesFile // series lookup file
	database string           // Name of database.

	// Cached sketches.
	mSketch, mTSketch estimator.Sketch // Measurement sketches
	sSketch, sTSketch estimator.Sketch // Series sketches

	// Index's version.
	version int

	// Number of partitions used by the index.
	PartitionN uint64
}

func (i *Index) UniqueReferenceID() uintptr {
	return uintptr(unsafe.Pointer(i))
}

// NewIndex returns a new instance of Index.
func NewIndex(sfile *tsdb.SeriesFile, database string, options ...IndexOption) *Index {
	idx := &Index{
		tagValueCacheSize: tsdb.DefaultSeriesIDSetCacheSize,
		maxLogFileSize:    tsdb.DefaultMaxIndexLogFileSize,
		logger:            zap.NewNop(),
		version:           Version,
		sfile:             sfile,
		database:          database,
		mSketch:           hll.NewDefaultPlus(),
		mTSketch:          hll.NewDefaultPlus(),
		sSketch:           hll.NewDefaultPlus(),
		sTSketch:          hll.NewDefaultPlus(),
		PartitionN:        DefaultPartitionN,
	}

	for _, option := range options {
		option(idx)
	}

	idx.tagValueCache = NewTagValueSeriesIDCache(idx.tagValueCacheSize)
	return idx
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
	b += int(unsafe.Sizeof(i.mSketch)) + i.mSketch.Bytes()
	b += int(unsafe.Sizeof(i.mTSketch)) + i.mTSketch.Bytes()
	b += int(unsafe.Sizeof(i.sSketch)) + i.sSketch.Bytes()
	b += int(unsafe.Sizeof(i.sTSketch)) + i.sTSketch.Bytes()
	b += int(unsafe.Sizeof(i.database)) + len(i.database)
	b += int(unsafe.Sizeof(i.version))
	b += int(unsafe.Sizeof(i.PartitionN))
	i.mu.RUnlock()
	return b
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
		p.nosync = i.disableFsync
		p.logbufferSize = i.logfileBufferSize
		p.logger = i.logger.With(zap.String("tsi1_partition", fmt.Sprint(j+1)))
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

	// Refresh cached sketches.
	if err := i.updateSeriesSketches(); err != nil {
		return err
	} else if err := i.updateMeasurementSketches(); err != nil {
		return err
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

// updateMeasurementSketches rebuilds the cached measurement sketches.
func (i *Index) updateMeasurementSketches() error {
	for j := 0; j < int(i.PartitionN); j++ {
		if s, t, err := i.partitions[j].MeasurementsSketches(); err != nil {
			return err
		} else if i.mSketch.Merge(s); err != nil {
			return err
		} else if i.mTSketch.Merge(t); err != nil {
			return err
		}
	}
	return nil
}

// updateSeriesSketches rebuilds the cached series sketches.
func (i *Index) updateSeriesSketches() error {
	for j := 0; j < int(i.PartitionN); j++ {
		if s, t, err := i.partitions[j].SeriesSketches(); err != nil {
			return err
		} else if i.sSketch.Merge(s); err != nil {
			return err
		} else if i.sTSketch.Merge(t); err != nil {
			return err
		}
	}
	return nil
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

	// Update sketches under lock.
	i.mu.Lock()
	defer i.mu.Unlock()

	i.mTSketch.Add(name)
	if err := i.updateSeriesSketches(); err != nil {
		return err
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

				ids, err := i.partitions[idx].createSeriesListIfNotExists(pNames[idx], pTags[idx])

				var updateCache bool
				for _, id := range ids {
					if id != 0 {
						updateCache = true
						break
					}
				}

				if !updateCache {
					errC <- err
					continue
				}

				// Some cached bitset results may need to be updated.
				i.tagValueCache.RLock()
				for j, id := range ids {
					if id == 0 {
						continue
					}

					name := pNames[idx][j]
					tags := pTags[idx][j]
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

	// Update sketches under lock.
	i.mu.Lock()
	defer i.mu.Unlock()

	for _, key := range keys {
		i.sSketch.Add(key)
	}
	for _, name := range names {
		i.mSketch.Add(name)
	}

	return nil
}

// CreateSeriesIfNotExists creates a series if it doesn't exist or is deleted.
func (i *Index) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	ids, err := i.partition(key).createSeriesListIfNotExists([][]byte{name}, []models.Tags{tags})
	if err != nil {
		return err
	}

	i.mu.Lock()
	i.sSketch.Add(key)
	i.mSketch.Add(name)
	i.mu.Unlock()

	if ids[0] == 0 {
		return nil // No new series, nothing further to update.
	}

	// If there are cached sets for any of the tag pairs, they will need to be
	// updated with the series id.
	i.tagValueCache.RLock()
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
			// Note this will only add `id` to the set if it exists.
			i.tagValueCache.addToSet(name, pair.Key, pair.Value, ids[0]) // Takes a lock on the series id set
		}
	}
	i.tagValueCache.RUnlock()
	return nil
}

// InitializeSeries is a no-op. This only applies to the in-memory index.
func (i *Index) InitializeSeries(keys, names [][]byte, tags []models.Tags) error {
	return nil
}

// DropSeries drops the provided series from the index.  If cascade is true
// and this is the last series to the measurement, the measurment will also be dropped.
func (i *Index) DropSeries(seriesID uint64, key []byte, cascade bool) error {
	// Remove from partition.
	if err := i.partition(key).DropSeries(seriesID); err != nil {
		return err
	}

	// Add sketch tombstone.
	i.mu.Lock()
	i.sTSketch.Add(key)
	i.mu.Unlock()

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
func (i *Index) DropMeasurementIfSeriesNotExist(name []byte) (bool, error) {
	// Check if that was the last series for the measurement in the entire index.
	if ok, err := i.MeasurementHasSeries(name); err != nil {
		return false, err
	} else if ok {
		return false, nil
	}

	// If no more series exist in the measurement then delete the measurement.
	return true, i.DropMeasurement(name)
}

// MeasurementsSketches returns the two measurement sketches for the index.
func (i *Index) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.mSketch.Clone(), i.mTSketch.Clone(), nil
}

// SeriesSketches returns the two series sketches for the index.
func (i *Index) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.sSketch.Clone(), i.sTSketch.Clone(), nil
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
	// Check series ID set cache...
	if i.tagValueCacheSize > 0 {
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
	if i.tagValueCacheSize == 0 {
		return itr, nil
	}

	// Check if the iterator contains only series id sets. Cache them...
	if ssitr, ok := itr.(tsdb.SeriesIDSetIterator); ok {
		ss := ssitr.SeriesIDSet()
		i.tagValueCache.Put(name, key, value, ss)
	}
	return itr, nil
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
