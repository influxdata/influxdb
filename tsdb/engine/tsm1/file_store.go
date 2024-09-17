package tsm1

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/pkg/file"
	"github.com/influxdata/influxdb/v2/pkg/limiter"
	"github.com/influxdata/influxdb/v2/pkg/metrics"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// The extension used to describe temporary snapshot files.
	TmpTSMFileExtension = "tmp"

	// The extension used to describe corrupt snapshot files.
	BadTSMFileExtension = "bad"
)

var (
	ErrNewReadersBlocked = errors.New("new readers blocked")
)

// TSMFile represents an on-disk TSM file.
type TSMFile interface {
	// Path returns the underlying file path for the TSMFile.  If the file
	// has not be written or loaded from disk, the zero value is returned.
	Path() string

	// Read returns all the values in the block where time t resides.
	Read(key []byte, t int64) ([]Value, error)

	// ReadAt returns all the values in the block identified by entry.
	ReadAt(entry *IndexEntry, values []Value) ([]Value, error)
	ReadFloatBlockAt(entry *IndexEntry, values *[]FloatValue) ([]FloatValue, error)
	ReadFloatArrayBlockAt(entry *IndexEntry, values *tsdb.FloatArray) error
	ReadIntegerBlockAt(entry *IndexEntry, values *[]IntegerValue) ([]IntegerValue, error)
	ReadIntegerArrayBlockAt(entry *IndexEntry, values *tsdb.IntegerArray) error
	ReadUnsignedBlockAt(entry *IndexEntry, values *[]UnsignedValue) ([]UnsignedValue, error)
	ReadUnsignedArrayBlockAt(entry *IndexEntry, values *tsdb.UnsignedArray) error
	ReadStringBlockAt(entry *IndexEntry, values *[]StringValue) ([]StringValue, error)
	ReadStringArrayBlockAt(entry *IndexEntry, values *tsdb.StringArray) error
	ReadBooleanBlockAt(entry *IndexEntry, values *[]BooleanValue) ([]BooleanValue, error)
	ReadBooleanArrayBlockAt(entry *IndexEntry, values *tsdb.BooleanArray) error

	// Entries returns the index entries for all blocks for the given key.
	Entries(key []byte) []IndexEntry
	ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry

	// Returns true if the TSMFile may contain a value with the specified
	// key and time.
	ContainsValue(key []byte, t int64) bool

	// Contains returns true if the file contains any values for the given
	// key.
	Contains(key []byte) bool

	// OverlapsTimeRange returns true if the time range of the file intersect min and max.
	OverlapsTimeRange(min, max int64) bool

	// OverlapsKeyRange returns true if the key range of the file intersects min and max.
	OverlapsKeyRange(min, max []byte) bool

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (int64, int64)

	// TombstoneRange returns ranges of time that are deleted for the given key.
	TombstoneRange(key []byte) []TimeRange

	// KeyRange returns the min and max keys in the file.
	KeyRange() ([]byte, []byte)

	// KeyCount returns the number of distinct keys in the file.
	KeyCount() int

	// Seek returns the position in the index with the key <= key.
	Seek(key []byte) int

	// KeyAt returns the key located at index position idx.
	KeyAt(idx int) ([]byte, byte)

	// Type returns the block type of the values stored for the key.  Returns one of
	// BlockFloat64, BlockInt64, BlockBoolean, BlockString.  If key does not exist,
	// an error is returned.
	Type(key []byte) (byte, error)

	// BatchDelete return a BatchDeleter that allows for multiple deletes in batches
	// and group commit or rollback.
	BatchDelete() BatchDeleter

	// Delete removes the keys from the set of keys available in this file.
	Delete(keys [][]byte) error

	// DeleteRange removes the values for keys between timestamps min and max.
	DeleteRange(keys [][]byte, min, max int64) error

	// HasTombstones returns true if file contains values that have been deleted.
	HasTombstones() bool

	// TombstoneStats returns the tombstone filestats if there are any tombstones
	// written for this file.
	TombstoneStats() TombstoneStat

	// Close closes the underlying file resources.
	Close() error

	// Size returns the size of the file on disk in bytes.
	Size() uint32

	// Rename renames the existing TSM file to a new name and replaces the mmap backing slice using the new
	// file name. Index and Reader state are not re-initialized.
	Rename(path string) error

	// Remove deletes the file from the filesystem.
	Remove() error

	// InUse returns true if the file is currently in use by queries.
	InUse() bool

	// Ref records that this file is actively in use.
	Ref()

	// Unref records that this file is no longer in use.
	Unref()

	// Stats returns summary information about the TSM file.
	Stats() FileStat

	// BlockIterator returns an iterator pointing to the first block in the file and
	// allows sequential iteration to each and every block.
	BlockIterator() *BlockIterator

	// Free releases any resources held by the FileStore to free up system resources.
	Free() error
}

var (
	floatBlocksDecodedCounter    = metrics.MustRegisterCounter("float_blocks_decoded", metrics.WithGroup(tsmGroup))
	floatBlocksSizeCounter       = metrics.MustRegisterCounter("float_blocks_size_bytes", metrics.WithGroup(tsmGroup))
	integerBlocksDecodedCounter  = metrics.MustRegisterCounter("integer_blocks_decoded", metrics.WithGroup(tsmGroup))
	integerBlocksSizeCounter     = metrics.MustRegisterCounter("integer_blocks_size_bytes", metrics.WithGroup(tsmGroup))
	unsignedBlocksDecodedCounter = metrics.MustRegisterCounter("unsigned_blocks_decoded", metrics.WithGroup(tsmGroup))
	unsignedBlocksSizeCounter    = metrics.MustRegisterCounter("unsigned_blocks_size_bytes", metrics.WithGroup(tsmGroup))
	stringBlocksDecodedCounter   = metrics.MustRegisterCounter("string_blocks_decoded", metrics.WithGroup(tsmGroup))
	stringBlocksSizeCounter      = metrics.MustRegisterCounter("string_blocks_size_bytes", metrics.WithGroup(tsmGroup))
	booleanBlocksDecodedCounter  = metrics.MustRegisterCounter("boolean_blocks_decoded", metrics.WithGroup(tsmGroup))
	booleanBlocksSizeCounter     = metrics.MustRegisterCounter("boolean_blocks_size_bytes", metrics.WithGroup(tsmGroup))
)

// FileStore is an abstraction around multiple TSM files.
type FileStore struct {
	mu           sync.RWMutex
	lastModified time.Time
	// Most recently known file stats. If nil then stats will need to be
	// recalculated
	lastFileStats []FileStat

	currentGeneration int
	dir               string

	files       []TSMFile
	openLimiter limiter.Fixed // limit the number of concurrent opening TSM files.

	logger       *zap.Logger // Logger to be used for important messages
	traceLogger  *zap.Logger // Logger to be used when trace-logging is on.
	traceLogging bool

	stats  *fileStoreMetrics
	purger *purger

	currentTempDirID int

	parseFileName ParseFileNameFunc

	obs tsdb.FileStoreObserver

	copyFiles bool

	// newReaderBlockCount keeps track of the current new reader block requests.
	// If non-zero, no new TSMReader objects may be created.
	newReaderBlockCount int

	readerOptions []tsmReaderOption
}

// FileStat holds information about a TSM file on disk.
type FileStat struct {
	Path             string
	HasTombstone     bool
	Size             uint32
	LastModified     int64
	MinTime, MaxTime int64
	MinKey, MaxKey   []byte
}

// TombstoneStat holds information about a possible tombstone file on disk.
type TombstoneStat struct {
	TombstoneExists bool
	Path            string
	LastModified    int64
	Size            uint32
}

// OverlapsTimeRange returns true if the time range of the file intersect min and max.
func (f FileStat) OverlapsTimeRange(min, max int64) bool {
	return f.MinTime <= max && f.MaxTime >= min
}

// OverlapsKeyRange returns true if the min and max keys of the file overlap the arguments min and max.
func (f FileStat) OverlapsKeyRange(min, max []byte) bool {
	return len(min) != 0 && len(max) != 0 && bytes.Compare(f.MinKey, max) <= 0 && bytes.Compare(f.MaxKey, min) >= 0
}

// ContainsKey returns true if the min and max keys of the file overlap the arguments min and max.
func (f FileStat) ContainsKey(key []byte) bool {
	return bytes.Compare(f.MinKey, key) >= 0 || bytes.Compare(key, f.MaxKey) <= 0
}

// NewFileStore returns a new instance of FileStore based on the given directory.
func NewFileStore(dir string, tags tsdb.EngineTags, options ...tsmReaderOption) *FileStore {
	logger := zap.NewNop()
	fs := &FileStore{
		dir:          dir,
		lastModified: time.Time{},
		logger:       logger,
		traceLogger:  logger,
		openLimiter:  limiter.NewFixed(runtime.GOMAXPROCS(0)),
		stats:        newFileStoreMetrics(tags),
		purger: &purger{
			files:  map[string]TSMFile{},
			logger: logger,
		},
		obs:           noFileStoreObserver{},
		parseFileName: DefaultParseFileName,
		copyFiles:     runtime.GOOS == "windows",
		readerOptions: options,
	}
	fs.purger.fileStore = fs
	return fs
}

// WithObserver sets the observer for the file store.
func (f *FileStore) WithObserver(obs tsdb.FileStoreObserver) {
	f.obs = obs
}

func (f *FileStore) WithParseFileNameFunc(parseFileNameFunc ParseFileNameFunc) {
	f.parseFileName = parseFileNameFunc
}

func (f *FileStore) ParseFileName(path string) (int, int, error) {
	return f.parseFileName(path)
}

// enableTraceLogging must be called before the FileStore is opened.
func (f *FileStore) enableTraceLogging(enabled bool) {
	f.traceLogging = enabled
	if enabled {
		f.traceLogger = f.logger
	}
}

// WithLogger sets the logger on the file store.
func (f *FileStore) WithLogger(log *zap.Logger) {
	f.logger = log.With(zap.String("service", "filestore"))
	f.purger.logger = f.logger

	if f.traceLogging {
		f.traceLogger = f.logger
	}
}

var globalFileStoreMetrics = newAllFileStoreMetrics()

const filesSubsystem = "tsm_files"

type allFileStoreMetrics struct {
	files *prometheus.GaugeVec
	size  *prometheus.GaugeVec
}

type fileStoreMetrics struct {
	files      prometheus.Gauge
	size       prometheus.Gauge
	sizeAtomic int64
}

func (f *fileStoreMetrics) AddSize(n int64) {
	val := atomic.AddInt64(&f.sizeAtomic, n)
	f.size.Set(float64(val))
}

func (f *fileStoreMetrics) SetSize(n int64) {
	atomic.StoreInt64(&f.sizeAtomic, n)
	f.size.Set(float64(n))
}

func (f *fileStoreMetrics) SetFiles(n int64) {
	f.files.Set(float64(n))
}

func newAllFileStoreMetrics() *allFileStoreMetrics {
	labels := tsdb.EngineLabelNames()
	return &allFileStoreMetrics{
		files: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: filesSubsystem,
			Name:      "total",
			Help:      "Gauge of number of files per shard",
		}, labels),
		size: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: filesSubsystem,
			Name:      "disk_bytes",
			Help:      "Gauge of data size in bytes for each shard",
		}, labels),
	}
}

func FileStoreCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		globalFileStoreMetrics.files,
		globalFileStoreMetrics.size,
	}
}

func newFileStoreMetrics(tags tsdb.EngineTags) *fileStoreMetrics {
	labels := tags.GetLabels()
	return &fileStoreMetrics{
		files: globalFileStoreMetrics.files.With(labels),
		size:  globalFileStoreMetrics.size.With(labels),
	}
}

// Count returns the number of TSM files currently loaded.
func (f *FileStore) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.files)
}

// Files returns the slice of TSM files currently loaded. This is only used for
// tests, and the files aren't guaranteed to stay valid in the presence of compactions.
func (f *FileStore) Files() []TSMFile {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.files
}

// Free releases any resources held by the FileStore.  The resources will be re-acquired
// if necessary if they are needed after freeing them.
func (f *FileStore) Free() error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, f := range f.files {
		if err := f.Free(); err != nil {
			return err
		}
	}
	return nil
}

// CurrentGeneration returns the current generation of the TSM files.
func (f *FileStore) CurrentGeneration() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.currentGeneration
}

// NextGeneration increments the max file ID and returns the new value.
func (f *FileStore) NextGeneration() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.currentGeneration++
	return f.currentGeneration
}

// WalkKeys calls fn for every key in every TSM file known to the FileStore.  If the key
// exists in multiple files, it will be invoked for each file.
func (f *FileStore) WalkKeys(seek []byte, fn func(key []byte, typ byte) error) error {
	f.mu.RLock()
	if len(f.files) == 0 {
		f.mu.RUnlock()
		return nil
	}
	if f.newReadersBlocked() {
		f.mu.RUnlock()
		return fmt.Errorf("WalkKeys: %q: %w", f.dir, ErrNewReadersBlocked)
	}

	// Ensure files are not unmapped while we're iterating over them.
	for _, r := range f.files {
		r.Ref()
		defer r.Unref()
	}

	ki := newMergeKeyIterator(f.files, seek)
	f.mu.RUnlock()
	for ki.Next() {
		key, typ := ki.Read()
		if err := fn(key, typ); err != nil {
			return err
		}
	}

	return nil
}

// Keys returns all keys and types for all files in the file store.
func (f *FileStore) Keys() map[string]byte {
	f.mu.RLock()
	defer f.mu.RUnlock()

	uniqueKeys := map[string]byte{}
	if err := f.WalkKeys(nil, func(key []byte, typ byte) error {
		uniqueKeys[string(key)] = typ
		return nil
	}); err != nil {
		return nil
	}

	return uniqueKeys
}

// Type returns the type of values store at the block for key.
func (f *FileStore) Type(key []byte) (byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for _, f := range f.files {
		if f.Contains(key) {
			return f.Type(key)
		}
	}
	return 0, fmt.Errorf("unknown type for %v", key)
}

// Delete removes the keys from the set of keys available in this file.
func (f *FileStore) Delete(keys [][]byte) error {
	return f.DeleteRange(keys, math.MinInt64, math.MaxInt64)
}

func (f *FileStore) Apply(ctx context.Context, fn func(r TSMFile) error) error {
	// Limit apply fn to number of cores
	limiter := limiter.NewFixed(runtime.GOMAXPROCS(0))

	f.mu.RLock()
	if f.newReadersBlocked() {
		f.mu.RUnlock()
		return fmt.Errorf("Apply: %q: %w", f.dir, ErrNewReadersBlocked)
	}
	errC := make(chan error, len(f.files))

	for _, f := range f.files {
		go func(r TSMFile) {
			if err := limiter.Take(ctx); err != nil {
				errC <- err
				return
			}
			defer limiter.Release()

			r.Ref()
			defer r.Unref()
			errC <- fn(r)
		}(f)
	}

	var applyErr error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			applyErr = err
		}
	}
	f.mu.RUnlock()

	f.mu.Lock()
	f.lastModified = time.Now().UTC()
	f.lastFileStats = nil
	f.mu.Unlock()

	return applyErr
}

// DeleteRange removes the values for keys between timestamps min and max.  This should only
// be used with smaller batches of series keys.
func (f *FileStore) DeleteRange(keys [][]byte, min, max int64) error {
	var batches BatchDeleters
	f.mu.RLock()
	for _, f := range f.files {
		if f.OverlapsTimeRange(min, max) {
			batches = append(batches, f.BatchDelete())
		}
	}
	f.mu.RUnlock()

	if len(batches) == 0 {
		return nil
	}

	if err := func() error {
		if err := batches.DeleteRange(keys, min, max); err != nil {
			return err
		}

		return batches.Commit()
	}(); err != nil {
		// Rollback the deletes
		_ = batches.Rollback()
		return err
	}

	f.mu.Lock()
	f.lastModified = time.Now().UTC()
	f.lastFileStats = nil
	f.mu.Unlock()
	return nil
}

// Open loads all the TSM files in the configured directory.
func (f *FileStore) Open(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Not loading files from disk so nothing to do
	if f.dir == "" {
		return nil
	}

	if f.openLimiter == nil {
		return errors.New("cannot open FileStore without an OpenLimiter (is EngineOptions.OpenLimiter set?)")
	}

	// find the current max ID for temp directories
	tmpfiles, err := os.ReadDir(f.dir)
	if err != nil {
		return fmt.Errorf("error calling ReadDir on %q in FileStore.Open: %w", f.dir, err)
	}

	// ascertain the current temp directory number by examining the existing
	// directories and choosing the one with the higest basename when converted
	// to an integer.
	for _, fi := range tmpfiles {
		if !fi.IsDir() || !strings.HasSuffix(fi.Name(), "."+TmpTSMFileExtension) {
			continue
		}

		ss := strings.Split(filepath.Base(fi.Name()), ".")
		if len(ss) != 2 {
			continue
		}

		i, err := strconv.Atoi(ss[0])
		if err != nil || i <= f.currentTempDirID {
			continue
		}

		// i must be a valid integer and greater than f.currentTempDirID at this
		// point
		f.currentTempDirID = i
	}

	pattern := filepath.Join(f.dir, "*."+TSMFileExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("error in Glob for %q in FileStore.Open: %w", pattern, err)
	}

	// struct to hold the result of opening each reader in a goroutine
	type res struct {
		r   *TSMReader
		err error
	}

	readerC := make(chan *res)
	for i, fn := range files {
		// Keep track of the latest ID
		generation, _, err := f.parseFileName(fn)
		if err != nil {
			return fmt.Errorf("error parsing %q in FileStore.Open: %w", fn, err)
		}

		if generation >= f.currentGeneration {
			f.currentGeneration = generation + 1
		}

		file, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("error calling OpenFile on %q in FileStore.Open: %w", fn, err)
		}

		go func(idx int, file *os.File) {
			// Ensure a limited number of TSM files are loaded at once.
			// Systems which have very large datasets (1TB+) can have thousands
			// of TSM files which can cause extremely long load times.
			if err := f.openLimiter.Take(ctx); err != nil {
				f.logger.Error("Failed to open tsm file", zap.String("path", file.Name()), zap.Error(err))
				readerC <- &res{err: fmt.Errorf("failed to open tsm file %q: %w", file.Name(), err)}
				return
			}
			defer f.openLimiter.Release()

			start := time.Now()
			df, err := NewTSMReader(file, f.readerOptions...)
			f.logger.Info("Opened file",
				zap.String("path", file.Name()),
				zap.Int("id", idx),
				zap.Duration("duration", time.Since(start)))

			// If we are unable to read a TSM file then log the error.
			if err != nil {
				if cerr := file.Close(); cerr != nil {
					f.logger.Error("Error closing TSM file after error", zap.String("path", file.Name()), zap.Int("id", idx), zap.Error(cerr))
				}
				if errors.Is(err, MmapError{}) {
					// An MmapError may indicate we have insufficient
					// handles for the mmap call, in which case the file should
					// be left untouched, and the vm.max_map_count be raised.
					f.logger.Error("Cannot read TSM file, system limit for vm.max_map_count may be too low",
						zap.String("path", file.Name()), zap.Int("id", idx), zap.Error(err))
					readerC <- &res{r: df, err: fmt.Errorf("cannot read file %s, system limit for vm.max_map_count may be too low: %w", file.Name(), err)}
					return
				} else {
					// If the file is corrupt, rename it and
					// continue loading the shard without it.
					f.logger.Error("Cannot read corrupt tsm file, renaming", zap.String("path", file.Name()), zap.Int("id", idx), zap.Error(err))
					if e := os.Rename(file.Name(), file.Name()+"."+BadTSMFileExtension); e != nil {
						f.logger.Error("Cannot rename corrupt tsm file", zap.String("path", file.Name()), zap.Int("id", idx), zap.Error(e))
						readerC <- &res{r: df, err: fmt.Errorf("cannot rename corrupt file %s: %w", file.Name(), e)}
						return
					}
					readerC <- &res{r: df, err: fmt.Errorf("cannot read corrupt file %s: %w", file.Name(), err)}
					return
				}
			}
			df.WithObserver(f.obs)
			readerC <- &res{r: df}
		}(i, file)
	}

	var lm int64
	isEmpty := true
	for range files {
		res := <-readerC
		if res.err != nil {
			return res.err
		} else if res.r == nil {
			continue
		}
		f.files = append(f.files, res.r)

		// Accumulate file store size stats
		f.stats.AddSize(int64(res.r.Size()))
		if ts := res.r.TombstoneStats(); ts.TombstoneExists {
			f.stats.AddSize(int64(ts.Size))
		}

		// Re-initialize the lastModified time for the file store
		if res.r.LastModified() > lm {
			lm = res.r.LastModified()
		}
		isEmpty = false
	}
	if isEmpty {
		if fi, err := os.Stat(f.dir); err == nil {
			f.lastModified = fi.ModTime().UTC()
		} else {
			close(readerC)
			return fmt.Errorf("error calling Stat on %q in FileStore.Open: %w", f.dir, err)
		}
	} else {
		f.lastModified = time.Unix(0, lm).UTC()
	}
	close(readerC)

	sort.Sort(tsmReaders(f.files))
	f.stats.SetFiles(int64(len(f.files)))
	return nil
}

// Close closes the file store.
func (f *FileStore) Close() error {
	// Make the object appear closed to other method calls.
	f.mu.Lock()

	files := f.files

	f.lastFileStats = nil
	f.files = nil

	f.stats.SetFiles(0)

	// Let other methods access this closed object while we do the actual closing.
	f.mu.Unlock()

	var errSlice []error
	for _, tsmFile := range files {
		errSlice = append(errSlice, tsmFile.Close())
	}

	return errors.Join(errSlice...)
}

func (f *FileStore) DiskSizeBytes() int64 {
	return atomic.LoadInt64(&f.stats.sizeAtomic)
}

// Read returns the slice of values for the given key and the given timestamp,
// if any file matches those constraints.
func (f *FileStore) Read(key []byte, t int64) ([]Value, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for _, f := range f.files {
		// Can this file possibly contain this key and timestamp?
		if !f.Contains(key) {
			continue
		}

		// May have the key and time we are looking for so try to find
		v, err := f.Read(key, t)
		if err != nil {
			return nil, err
		}

		if len(v) > 0 {
			return v, nil
		}
	}
	return nil, nil
}

func (f *FileStore) Cost(key []byte, min, max int64) query.IteratorCost {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cost(key, min, max)
}

// Reader returns a TSMReader for path if one is currently managed by the FileStore.
// Otherwise it returns nil. If it returns a file, you must call Unref on it when
// you are done, and never use it after that.
func (f *FileStore) TSMReader(path string) (*TSMReader, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.newReadersBlocked() {
		return nil, fmt.Errorf("TSMReader: %q (%q): %w", f.dir, path, ErrNewReadersBlocked)
	}
	for _, r := range f.files {
		if r.Path() == path {
			r.Ref()
			return r.(*TSMReader), nil
		}
	}
	return nil, nil
}

// SetNewReadersBlocked sets if new readers can access the files in this FileStore.
// If blocked is true, the number of reader blocks is incremented and new readers will
// receive an error instead of reader access. If blocked is false, the number
// of reader blocks is decremented. If the reader blocks drops to 0, then
// new readers will be granted access to the files.
func (f *FileStore) SetNewReadersBlocked(block bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if block {
		if f.newReaderBlockCount < 0 {
			return fmt.Errorf("newReaderBlockCount for %q was %d before block operation, block failed", f.dir, f.newReaderBlockCount)
		}
		f.newReaderBlockCount++
	} else {
		if f.newReaderBlockCount <= 0 {
			return fmt.Errorf("newReadersBlockCount for %q was %d before unblock operation, unblock failed", f.dir, f.newReaderBlockCount)
		}
		f.newReaderBlockCount--
	}
	return nil
}

// newReadersBlocked returns true if new references to TSMReader objects are not allowed.
// Must be called with f.mu lock held (reader or writer).
// See SetNewReadersBlocked for interface to allow and block access to TSMReader objects.
func (f *FileStore) newReadersBlocked() bool {
	return f.newReaderBlockCount > 0
}

// InUse returns true if any files in this FileStore are in-use.
// InUse can only be called if a new readers have been blocked using SetNewReadersBlocked.
// This is to avoid a race condition between calling InUse and attempting an operation
// that requires no active readers. Calling InUse without a new readers block results
// in an error.
func (f *FileStore) InUse() (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.newReadersBlocked() {
		return false, fmt.Errorf("InUse called without a new reader block for %q", f.dir)
	}
	for _, r := range f.files {
		if r.InUse() {
			return true, nil
		}
	}
	return false, nil
}

// KeyCursor returns a KeyCursor for key and t across the files in the FileStore.
func (f *FileStore) KeyCursor(ctx context.Context, key []byte, t int64, ascending bool) *KeyCursor {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.newReadersBlocked() {
		// New readers are blocked for this FileStore because there is a delete attempt in progress.
		// Return an empty cursor to appease the callers since they generally don't handle
		// a nil KeyCursor gracefully.
		return &KeyCursor{
			key:       key,
			seeks:     nil,
			ctx:       ctx,
			col:       metrics.GroupFromContext(ctx),
			ascending: ascending,
		}
	}
	return newKeyCursor(ctx, f, key, t, ascending)
}

// Stats returns the stats of the underlying files, preferring the cached version if it is still valid.
func (f *FileStore) Stats() []FileStat {
	f.mu.RLock()
	if len(f.lastFileStats) > 0 {
		defer f.mu.RUnlock()
		return f.lastFileStats
	}
	f.mu.RUnlock()

	// The file stats cache is invalid due to changes to files. Need to
	// recalculate.
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.lastFileStats) > 0 {
		return f.lastFileStats
	}

	// If lastFileStats's capacity is far away from the number of entries
	// we need to add, then we'll reallocate.
	if cap(f.lastFileStats) < len(f.files)/2 {
		f.lastFileStats = make([]FileStat, 0, len(f.files))
	}

	for _, fd := range f.files {
		f.lastFileStats = append(f.lastFileStats, fd.Stats())
	}
	return f.lastFileStats
}

// ReplaceWithCallback replaces oldFiles with newFiles and calls updatedFn with the files to be added the FileStore.
func (f *FileStore) ReplaceWithCallback(oldFiles, newFiles []string, updatedFn func(r []TSMFile)) error {
	return f.replace(oldFiles, newFiles, updatedFn)
}

// Replace replaces oldFiles with newFiles.
func (f *FileStore) Replace(oldFiles, newFiles []string) error {
	return f.replace(oldFiles, newFiles, nil)
}

func (f *FileStore) replace(oldFiles, newFiles []string, updatedFn func(r []TSMFile)) error {
	if len(oldFiles) == 0 && len(newFiles) == 0 {
		return nil
	}

	f.mu.RLock()
	maxTime := f.lastModified
	f.mu.RUnlock()

	updated := make([]TSMFile, 0, len(newFiles))
	tsmTmpExt := fmt.Sprintf("%s.%s", TSMFileExtension, TmpTSMFileExtension)

	// Rename all the new files to make them live on restart
	for _, file := range newFiles {
		if !strings.HasSuffix(file, tsmTmpExt) && !strings.HasSuffix(file, TSMFileExtension) {
			// This isn't a .tsm or .tsm.tmp file.
			continue
		}

		// give the observer a chance to process the file first.
		if err := f.obs.FileFinishing(file); err != nil {
			return err
		}

		var oldName, newName = file, file
		if strings.HasSuffix(oldName, tsmTmpExt) {
			// The new TSM files have a tmp extension.  First rename them.
			newName = file[:len(file)-4]
			if err := os.Rename(oldName, newName); err != nil {
				return err
			}
		}

		// Any error after this point should result in the file being bein named
		// back to the original name. The caller then has the opportunity to
		// remove it.
		fd, err := os.Open(newName)
		if err != nil {
			if newName != oldName {
				if err1 := os.Rename(newName, oldName); err1 != nil {
					return err1
				}
			}
			return err
		}

		// Keep track of the new mod time
		if stat, err := fd.Stat(); err == nil {
			if maxTime.IsZero() || stat.ModTime().UTC().After(maxTime) {
				maxTime = stat.ModTime().UTC()
			}
		}

		tsm, err := NewTSMReader(fd, f.readerOptions...)
		if err != nil {
			if newName != oldName {
				if err1 := os.Rename(newName, oldName); err1 != nil {
					return err1
				}
			}
			return err
		}
		tsm.WithObserver(f.obs)

		updated = append(updated, tsm)
	}

	if updatedFn != nil {
		updatedFn(updated)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Copy the current set of active files while we rename
	// and load the new files.  We copy the pointers here to minimize
	// the time that locks are held as well as to ensure that the replacement
	// is atomic.©

	updated = append(updated, f.files...)

	// We need to prune our set of active files now
	var active, inuse []TSMFile
	for _, file := range updated {
		keep := true
		for _, remove := range oldFiles {
			if remove == file.Path() {
				keep = false

				// give the observer a chance to process the file first.
				if err := f.obs.FileUnlinking(file.Path()); err != nil {
					return err
				}

				if ts := file.TombstoneStats(); ts.TombstoneExists {
					if err := f.obs.FileUnlinking(ts.Path); err != nil {
						return err
					}
				}

				// If queries are running against this file, then we need to move it out of the
				// way and let them complete.  We'll then delete the original file to avoid
				// blocking callers upstream.  If the process crashes, the temp file is
				// cleaned up at startup automatically.
				//
				// In order to ensure that there are no races with this (file held externally calls Ref
				// after we check InUse), we need to maintain the invariant that every handle to a file
				// is handed out in use (Ref'd), and handlers only ever relinquish the file once (call Unref
				// exactly once, and never use it again). InUse is only valid during a write lock, since
				// we allow calls to Ref and Unref under the read lock and no lock at all respectively.
				if file.InUse() {
					// Copy all the tombstones related to this TSM file
					var deletes []string
					if ts := file.TombstoneStats(); ts.TombstoneExists {
						deletes = append(deletes, ts.Path)
					}

					// Rename the TSM file used by this reader
					tempPath := fmt.Sprintf("%s.%s", file.Path(), TmpTSMFileExtension)
					if err := file.Rename(tempPath); err != nil {
						return err
					}

					// Remove the old file and tombstones.  We can't use the normal TSMReader.Remove()
					// because it now refers to our temp file which we can't remove.
					for _, f := range deletes {
						if err := os.Remove(f); err != nil {
							return err
						}
					}

					inuse = append(inuse, file)
					continue
				}

				if err := file.Close(); err != nil {
					return err
				}

				if err := file.Remove(); err != nil {
					return err
				}
				break
			}
		}

		if keep {
			active = append(active, file)
		}
	}

	if err := file.SyncDir(f.dir); err != nil {
		return err
	}

	// Tell the purger about our in-use files we need to remove
	f.purger.add(inuse)

	// If times didn't change (which can happen since file mod times are second level),
	// then add a ns to the time to ensure that lastModified changes since files on disk
	// actually did change
	if maxTime.Equal(f.lastModified) || maxTime.Before(f.lastModified) {
		maxTime = f.lastModified.UTC().Add(1)
	}

	f.lastModified = maxTime.UTC()

	f.lastFileStats = nil
	f.files = active
	sort.Sort(tsmReaders(f.files))
	f.stats.SetFiles(int64(len(f.files)))

	// Recalculate the disk size stat
	var totalSize int64
	for _, file := range f.files {
		totalSize += int64(file.Size())
		if ts := file.TombstoneStats(); ts.TombstoneExists {
			totalSize += int64(ts.Size)
		}
	}
	f.stats.SetSize(totalSize)

	return nil
}

// LastModified returns the last time the file store was updated with new
// TSM files or a delete.
func (f *FileStore) LastModified() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.lastModified
}

// BlockCount returns number of values stored in the block at location idx
// in the file at path.  If path does not match any file in the store, 0 is
// returned.  If idx is out of range for the number of blocks in the file,
// 0 is returned.
func (f *FileStore) BlockCount(path string, idx int) int {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if idx < 0 {
		return 0
	}

	for _, fd := range f.files {
		if fd.Path() == path {
			iter := fd.BlockIterator()
			for i := 0; i < idx; i++ {
				if !iter.Next() {
					return 0
				}
			}
			_, _, _, _, _, block, _ := iter.Read()
			// on Error, BlockCount(block) returns 0 for cnt
			cnt, _ := BlockCount(block)
			return cnt
		}
	}
	return 0
}

// We need to determine the possible files that may be accessed by this query given
// the time range.
func (f *FileStore) cost(key []byte, min, max int64) query.IteratorCost {
	var cache []IndexEntry
	cost := query.IteratorCost{}
	for _, fd := range f.files {
		minTime, maxTime := fd.TimeRange()
		if !(maxTime > min && minTime < max) {
			continue
		}
		skipped := true
		tombstones := fd.TombstoneRange(key)

		entries := fd.ReadEntries(key, &cache)
	ENTRIES:
		for i := 0; i < len(entries); i++ {
			ie := entries[i]

			if !(ie.MaxTime > min && ie.MinTime < max) {
				continue
			}

			// Skip any blocks only contain values that are tombstoned.
			for _, t := range tombstones {
				if t.Min <= ie.MinTime && t.Max >= ie.MaxTime {
					continue ENTRIES
				}
			}

			cost.BlocksRead++
			cost.BlockSize += int64(ie.Size)
			skipped = false
		}

		if !skipped {
			cost.NumFiles++
		}
	}
	return cost
}

// locations returns the files and index blocks for a key and time.  ascending indicates
// whether the key will be scan in ascending time order or descenging time order.
// This function assumes the read-lock has been taken.
func (f *FileStore) locations(key []byte, t int64, ascending bool) []*location {
	var cache []IndexEntry
	locations := make([]*location, 0, len(f.files))
	for _, fd := range f.files {
		minTime, maxTime := fd.TimeRange()

		// If we ascending and the max time of the file is before where we want to start
		// skip it.
		if ascending && maxTime < t {
			continue
			// If we are descending and the min time of the file is after where we want to start,
			// then skip it.
		} else if !ascending && minTime > t {
			continue
		}
		tombstones := fd.TombstoneRange(key)

		// This file could potential contain points we are looking for so find the blocks for
		// the given key.
		entries := fd.ReadEntries(key, &cache)
	LOOP:
		for i := 0; i < len(entries); i++ {
			ie := entries[i]

			// Skip any blocks only contain values that are tombstoned.
			for _, t := range tombstones {
				if t.Min <= ie.MinTime && t.Max >= ie.MaxTime {
					continue LOOP
				}
			}

			// If we ascending and the max time of a block is before where we are looking, skip
			// it since the data is out of our range
			if ascending && ie.MaxTime < t {
				continue
				// If we descending and the min time of a block is after where we are looking, skip
				// it since the data is out of our range
			} else if !ascending && ie.MinTime > t {
				continue
			}

			location := &location{
				r:     fd,
				entry: ie,
			}

			if ascending {
				// For an ascending cursor, mark everything before the seek time as read
				// so we can filter it out at query time
				location.readMin = math.MinInt64
				location.readMax = t - 1
			} else {
				// For an ascending cursort, mark everything after the seek time as read
				// so we can filter it out at query time
				location.readMin = t + 1
				location.readMax = math.MaxInt64
			}
			// Otherwise, add this file and block location
			locations = append(locations, location)
		}
	}
	return locations
}

// MakeSnapshotLinks creates hardlinks from the supplied TSMFiles to
// corresponding files under a supplied directory.
func (f *FileStore) MakeSnapshotLinks(destPath string, files []TSMFile) (returnErr error) {
	for _, tsmf := range files {
		newpath := filepath.Join(destPath, filepath.Base(tsmf.Path()))
		err := f.copyOrLink(tsmf.Path(), newpath)
		if err != nil {
			return err
		}
		if tf := tsmf.TombstoneStats(); tf.TombstoneExists {
			newpath := filepath.Join(destPath, filepath.Base(tf.Path))
			err := f.copyOrLink(tf.Path, newpath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FileStore) copyOrLink(oldpath string, newpath string) error {
	if f.copyFiles {
		f.logger.Info("copying backup snapshots", zap.String("OldPath", oldpath), zap.String("NewPath", newpath))
		if err := f.copyNotLink(oldpath, newpath); err != nil {
			return err
		}
	} else {
		f.logger.Info("linking backup snapshots", zap.String("OldPath", oldpath), zap.String("NewPath", newpath))
		if err := f.linkNotCopy(oldpath, newpath); err != nil {
			return err
		}
	}
	return nil
}

// copyNotLink - use file copies instead of hard links for 2 scenarios:
// Windows does not permit deleting a file with open file handles
// Azure does not support hard links in its default file system
func (f *FileStore) copyNotLink(oldPath, newPath string) (returnErr error) {
	rfd, err := os.Open(oldPath)
	if err != nil {
		return fmt.Errorf("error opening file for backup %s: %q", oldPath, err)
	} else {
		defer func() {
			if e := rfd.Close(); returnErr == nil && e != nil {
				returnErr = fmt.Errorf("error closing source file for backup %s: %w", oldPath, e)
			}
		}()
	}
	fi, err := rfd.Stat()
	if err != nil {
		return fmt.Errorf("error collecting statistics from file for backup %s: %w", oldPath, err)
	}
	wfd, err := os.OpenFile(newPath, os.O_RDWR|os.O_CREATE, fi.Mode())
	if err != nil {
		return fmt.Errorf("error creating temporary file for backup %s:  %w", newPath, err)
	} else {
		defer func() {
			if e := wfd.Close(); returnErr == nil && e != nil {
				returnErr = fmt.Errorf("error closing temporary file for backup %s: %w", newPath, e)
			}
		}()
	}
	if _, err := io.Copy(wfd, rfd); err != nil {
		return fmt.Errorf("unable to copy file for backup from %s to %s: %w", oldPath, newPath, err)
	}
	if err := os.Chtimes(newPath, fi.ModTime(), fi.ModTime()); err != nil {
		return fmt.Errorf("unable to set modification time on temporary backup file %s: %w", newPath, err)
	}
	return nil
}

// linkNotCopy - use hard links for backup snapshots
func (f *FileStore) linkNotCopy(oldPath, newPath string) error {
	if err := os.Link(oldPath, newPath); err != nil {
		if errors.Is(err, syscall.ENOTSUP) {
			if fi, e := os.Stat(oldPath); e == nil && !fi.IsDir() {
				f.logger.Info("file system does not support hard links, switching to copies for backup", zap.String("OldPath", oldPath), zap.String("NewPath", newPath))
				// Force future snapshots to copy
				f.copyFiles = true
				return f.copyNotLink(oldPath, newPath)
			} else if e != nil {
				// Stat failed
				return fmt.Errorf("error creating hard link for backup, cannot determine if %s is a file or directory: %w", oldPath, e)
			} else {
				return fmt.Errorf("error creating hard link for backup - %s is a directory, not a file: %q", oldPath, err)
			}
		} else {
			return fmt.Errorf("error creating hard link for backup from %s to %s: %w", oldPath, newPath, err)
		}
	} else {
		return nil
	}
}

// CreateSnapshot creates hardlinks for all tsm and tombstone files
// in the path provided.
func (f *FileStore) CreateSnapshot() (string, error) {
	f.traceLogger.Info("Creating snapshot", zap.String("dir", f.dir))

	f.mu.Lock()
	if f.newReadersBlocked() {
		f.mu.Unlock()
		return "", fmt.Errorf("CreateSnapshot: %q: %w", f.dir, ErrNewReadersBlocked)
	}
	// create a copy of the files slice and ensure they aren't closed out from
	// under us, nor the slice mutated.
	files := make([]TSMFile, len(f.files))
	copy(files, f.files)

	for _, tsmf := range files {
		tsmf.Ref()
		defer tsmf.Unref()
	}

	// increment and keep track of the current temp dir for when we drop the lock.
	// this ensures we are the only writer to the directory.
	f.currentTempDirID += 1
	tmpPath := fmt.Sprintf("%d.%s", f.currentTempDirID, TmpTSMFileExtension)
	tmpPath = filepath.Join(f.dir, tmpPath)
	f.mu.Unlock()

	// create the tmp directory and add the hard links. there is no longer any shared
	// mutable state.
	err := os.Mkdir(tmpPath, 0777)
	if err != nil {
		return "", err
	}
	if err := f.MakeSnapshotLinks(tmpPath, files); err != nil {
		// remove temporary directory since we couldn't create our hard links.
		_ = os.RemoveAll(tmpPath)
		return "", fmt.Errorf("CreateSnapshot() failed to create links %v: %w", tmpPath, err)
	}

	return tmpPath, nil
}

// FormatFileNameFunc is executed when generating a new TSM filename.
// Source filenames are provided via src.
type FormatFileNameFunc func(generation, sequence int) string

// DefaultFormatFileName is the default implementation to format TSM filenames.
func DefaultFormatFileName(generation, sequence int) string {
	return fmt.Sprintf("%09d-%09d", generation, sequence)
}

// ParseFileNameFunc is executed when parsing a TSM filename into generation & sequence.
type ParseFileNameFunc func(name string) (generation, sequence int, err error)

// DefaultParseFileName is used to parse the filenames of TSM files.
func DefaultParseFileName(name string) (int, int, error) {
	base := filepath.Base(name)
	idx := strings.Index(base, ".")
	if idx == -1 {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	id := base[:idx]

	idx = strings.Index(id, "-")
	if idx == -1 {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	generation, err := strconv.ParseUint(id[:idx], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	sequence, err := strconv.ParseUint(id[idx+1:], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	return int(generation), int(sequence), nil
}

// KeyCursor allows iteration through keys in a set of files within a FileStore.
type KeyCursor struct {
	key []byte

	// seeks is all the file locations that we need to return during iteration.
	seeks []*location

	// current is the set of blocks possibly containing the next set of points.
	// Normally this is just one entry, but there may be multiple if points have
	// been overwritten.
	current []*location
	buf     []Value

	ctx context.Context
	col *metrics.Group

	// pos is the index within seeks.  Based on ascending, it will increment or
	// decrement through the size of seeks slice.
	pos       int
	ascending bool
}

type location struct {
	r     TSMFile
	entry IndexEntry

	readMin, readMax int64
}

func (l *location) read() bool {
	return l.readMin <= l.entry.MinTime && l.readMax >= l.entry.MaxTime
}

func (l *location) markRead(min, max int64) {
	if min < l.readMin {
		l.readMin = min
	}

	if max > l.readMax {
		l.readMax = max
	}
}

type descLocations []*location

// Sort methods
func (a descLocations) Len() int      { return len(a) }
func (a descLocations) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a descLocations) Less(i, j int) bool {
	if a[i].entry.OverlapsTimeRange(a[j].entry.MinTime, a[j].entry.MaxTime) {
		return a[i].r.Path() < a[j].r.Path()
	}
	return a[i].entry.MaxTime < a[j].entry.MaxTime
}

type ascLocations []*location

// Sort methods
func (a ascLocations) Len() int      { return len(a) }
func (a ascLocations) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ascLocations) Less(i, j int) bool {
	if a[i].entry.OverlapsTimeRange(a[j].entry.MinTime, a[j].entry.MaxTime) {
		return a[i].r.Path() < a[j].r.Path()
	}
	return a[i].entry.MinTime < a[j].entry.MinTime
}

// newKeyCursor returns a new instance of KeyCursor.
// This function assumes the read-lock has been taken.
func newKeyCursor(ctx context.Context, fs *FileStore, key []byte, t int64, ascending bool) *KeyCursor {
	c := &KeyCursor{
		key:       key,
		seeks:     fs.locations(key, t, ascending),
		ctx:       ctx,
		col:       metrics.GroupFromContext(ctx),
		ascending: ascending,
	}

	if ascending {
		sort.Sort(ascLocations(c.seeks))
	} else {
		sort.Sort(descLocations(c.seeks))
	}

	// Determine the distinct set of TSM files in use and mark then as in-use
	for _, f := range c.seeks {
		f.r.Ref()
	}

	c.seek(t)
	return c
}

// Close removes all references on the cursor.
func (c *KeyCursor) Close() {
	// Remove all of our in-use references since we're done
	for _, f := range c.seeks {
		f.r.Unref()
	}

	c.buf = nil
	c.seeks = nil
	c.current = nil
}

// seek positions the cursor at the given time.
func (c *KeyCursor) seek(t int64) {
	if len(c.seeks) == 0 {
		return
	}
	c.current = nil

	if c.ascending {
		c.seekAscending(t)
	} else {
		c.seekDescending(t)
	}
}

func (c *KeyCursor) seekAscending(t int64) {
	for i, e := range c.seeks {
		if t < e.entry.MinTime || e.entry.Contains(t) {
			// Record the position of the first block matching our seek time
			if len(c.current) == 0 {
				c.pos = i
			}

			c.current = append(c.current, e)
		}
	}
}

func (c *KeyCursor) seekDescending(t int64) {
	for i := len(c.seeks) - 1; i >= 0; i-- {
		e := c.seeks[i]
		if t > e.entry.MaxTime || e.entry.Contains(t) {
			// Record the position of the first block matching our seek time
			if len(c.current) == 0 {
				c.pos = i
			}
			c.current = append(c.current, e)
		}
	}
}

// Next moves the cursor to the next position.
// Data should be read by the ReadBlock functions.
func (c *KeyCursor) Next() {
	if len(c.current) == 0 {
		return
	}
	// Do we still have unread values in the current block
	if !c.current[0].read() {
		return
	}
	c.current = c.current[:0]
	if c.ascending {
		c.nextAscending()
	} else {
		c.nextDescending()
	}
}

func (c *KeyCursor) nextAscending() {
	for {
		c.pos++
		if c.pos >= len(c.seeks) {
			return
		} else if !c.seeks[c.pos].read() {
			break
		}
	}

	// Append the first matching block
	if len(c.current) == 0 {
		c.current = append(c.current, nil)
	} else {
		c.current = c.current[:1]
	}
	c.current[0] = c.seeks[c.pos]

	// If we have ovelapping blocks, append all their values so we can dedup
	for i := c.pos + 1; i < len(c.seeks); i++ {
		if c.seeks[i].read() {
			continue
		}

		c.current = append(c.current, c.seeks[i])
	}
}

func (c *KeyCursor) nextDescending() {
	for {
		c.pos--
		if c.pos < 0 {
			return
		} else if !c.seeks[c.pos].read() {
			break
		}
	}

	// Append the first matching block
	if len(c.current) == 0 {
		c.current = make([]*location, 1)
	} else {
		c.current = c.current[:1]
	}
	c.current[0] = c.seeks[c.pos]

	// If we have ovelapping blocks, append all their values so we can dedup
	for i := c.pos; i >= 0; i-- {
		if c.seeks[i].read() {
			continue
		}
		c.current = append(c.current, c.seeks[i])
	}
}

type purger struct {
	mu        sync.RWMutex
	fileStore *FileStore
	files     map[string]TSMFile
	running   bool

	logger *zap.Logger
}

func (p *purger) add(files []TSMFile) {
	p.mu.Lock()
	for _, f := range files {
		p.files[f.Path()] = f
	}
	p.mu.Unlock()
	p.purge()
}

func (p *purger) purge() {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return
	}
	p.running = true
	p.mu.Unlock()

	go func() {
		for {
			p.mu.Lock()
			for k, v := range p.files {
				// In order to ensure that there are no races with this (file held externally calls Ref
				// after we check InUse), we need to maintain the invariant that every handle to a file
				// is handed out in use (Ref'd), and handlers only ever relinquish the file once (call Unref
				// exactly once, and never use it again). InUse is only valid during a write lock, since
				// we allow calls to Ref and Unref under the read lock and no lock at all respectively.
				if !v.InUse() {
					if err := v.Close(); err != nil {
						p.logger.Info("Purge: close file", zap.Error(err))
						continue
					}

					if err := v.Remove(); err != nil {
						p.logger.Info("Purge: remove file", zap.Error(err))
						continue
					}
					delete(p.files, k)
				}
			}

			if len(p.files) == 0 {
				p.running = false
				p.mu.Unlock()
				return
			}

			p.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
}

type tsmReaders []TSMFile

func (a tsmReaders) Len() int           { return len(a) }
func (a tsmReaders) Less(i, j int) bool { return a[i].Path() < a[j].Path() }
func (a tsmReaders) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
