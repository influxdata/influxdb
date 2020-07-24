package tsm1

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/pkg/fs"
	"github.com/influxdata/influxdb/v2/pkg/limiter"
	"github.com/influxdata/influxdb/v2/pkg/metrics"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	// The extension used to describe temporary snapshot files.
	TmpTSMFileExtension = "tmp"

	// The extension used to describe corrupt snapshot files.
	BadTSMFileExtension = "bad"
)

type TSMIterator interface {
	Next() bool
	Peek() []byte
	Key() []byte
	Type() byte
	Entries() []IndexEntry
	Err() error
}

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
	ReadFloatArrayBlockAt(entry *IndexEntry, values *cursors.FloatArray) error
	ReadIntegerBlockAt(entry *IndexEntry, values *[]IntegerValue) ([]IntegerValue, error)
	ReadIntegerArrayBlockAt(entry *IndexEntry, values *cursors.IntegerArray) error
	ReadUnsignedBlockAt(entry *IndexEntry, values *[]UnsignedValue) ([]UnsignedValue, error)
	ReadUnsignedArrayBlockAt(entry *IndexEntry, values *cursors.UnsignedArray) error
	ReadStringBlockAt(entry *IndexEntry, values *[]StringValue) ([]StringValue, error)
	ReadStringArrayBlockAt(entry *IndexEntry, values *cursors.StringArray) error
	ReadBooleanBlockAt(entry *IndexEntry, values *[]BooleanValue) ([]BooleanValue, error)
	ReadBooleanArrayBlockAt(entry *IndexEntry, values *cursors.BooleanArray) error

	// Entries returns the index entries for all blocks for the given key.
	ReadEntries(key []byte, entries []IndexEntry) ([]IndexEntry, error)

	// Contains returns true if the file contains any values for the given
	// key.
	Contains(key []byte) bool

	// OverlapsTimeRange returns true if the time range of the file intersect min and max.
	OverlapsTimeRange(min, max int64) bool

	// OverlapsKeyRange returns true if the key range of the file intersects min and max.
	OverlapsKeyRange(min, max []byte) bool

	// OverlapsKeyPrefixRange returns true if the key range of the file
	// intersects min and max, evaluating up to the length of min and max
	// of the key range.
	OverlapsKeyPrefixRange(min, max []byte) bool

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (int64, int64)

	// TombstoneRange returns ranges of time that are deleted for the given key.
	TombstoneRange(key []byte, buf []TimeRange) []TimeRange

	// KeyRange returns the min and max keys in the file.
	KeyRange() ([]byte, []byte)

	// KeyCount returns the number of distinct keys in the file.
	KeyCount() int

	// Iterator returns an iterator over the keys starting at the provided key. You must
	// call Next before calling any of the accessors.
	Iterator([]byte) TSMIterator

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

	// DeletePrefix removes the values for keys beginning with prefix. It calls dead with
	// any keys that became dead as a result of this call.
	DeletePrefix(prefix []byte, min, max int64, pred Predicate, dead func([]byte)) error

	// HasTombstones returns true if file contains values that have been deleted.
	HasTombstones() bool

	// TombstoneFiles returns the tombstone filestats if there are any tombstones
	// written for this file.
	TombstoneFiles() []FileStat

	// Close closes the underlying file resources.
	Close() error

	// Size returns the size of the file on disk in bytes.
	Size() uint32

	// Rename renames the existing TSM file to a new name and replaces the mmap backing slice using the new
	// file name.  Index and Reader state are not re-initialized.
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

	// TimeRangeIterator returns an iterator over the keys, starting at the provided
	// key. Calling the HasData accessor will return true if data exists for the
	// interval [min, max] for the current key.
	// Next must be called before calling any of the accessors.
	TimeRangeIterator(key []byte, min, max int64) *TimeRangeIterator

	// TimeRangeMaxTimeIterator returns an iterator over the keys, starting at the provided
	// key. Calling the HasData and MaxTime accessors will be restricted to the
	// interval [min, max] for the current key.
	// Next must be called before calling any of the accessors.
	TimeRangeMaxTimeIterator(key []byte, min, max int64) *TimeRangeMaxTimeIterator

	// Free releases any resources held by the FileStore to free up system resources.
	Free() error

	// Stats returns the statistics for the file.
	MeasurementStats() (MeasurementStats, error)
}

// FileStoreObserver is passed notifications before the file store adds or deletes files. In this way, it can
// be sure to observe every file that is added or removed even in the presence of process death.
type FileStoreObserver interface {
	// FileFinishing is called before a file is renamed to it's final name.
	FileFinishing(path string) error

	// FileUnlinking is called before a file is unlinked.
	FileUnlinking(path string) error
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

	currentGeneration     int        // internally maintained generation
	currentGenerationFunc func() int // external generation
	dir                   string

	files           []TSMFile
	tsmMMAPWillNeed bool          // If true then the kernel will be advised MMAP_WILLNEED for TSM files.
	openLimiter     limiter.Fixed // limit the number of concurrent opening TSM files.

	logger *zap.Logger // Logger to be used for important messages

	tracker     *fileTracker
	readTracker *readTracker
	purger      *purger

	currentTempDirID int

	parseFileName ParseFileNameFunc

	obs FileStoreObserver

	pageFaultLimiter *rate.Limiter
}

// FileStat holds information about a TSM file on disk.
type FileStat struct {
	Path             string
	HasTombstone     bool
	Size             uint32
	CreatedAt        int64
	LastModified     int64
	MinTime, MaxTime int64
	MinKey, MaxKey   []byte
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
func (f FileStat) MaybeContainsKey(key []byte) bool {
	return bytes.Compare(f.MinKey, key) >= 0 || bytes.Compare(key, f.MaxKey) <= 0
}

// NewFileStore returns a new instance of FileStore based on the given directory.
func NewFileStore(dir string) *FileStore {
	logger := zap.NewNop()
	fs := &FileStore{
		dir:          dir,
		lastModified: time.Time{},
		logger:       logger,
		openLimiter:  limiter.NewFixed(runtime.GOMAXPROCS(0)),
		purger: &purger{
			files:  map[string]TSMFile{},
			logger: logger,
		},
		obs:           noFileStoreObserver{},
		parseFileName: DefaultParseFileName,
		tracker:       newFileTracker(newFileMetrics(nil), nil),
	}
	fs.purger.fileStore = fs
	return fs
}

// WithObserver sets the observer for the file store.
func (f *FileStore) WithObserver(obs FileStoreObserver) {
	if obs == nil {
		obs = noFileStoreObserver{}
	}
	f.obs = obs
}

func (f *FileStore) WithParseFileNameFunc(parseFileNameFunc ParseFileNameFunc) {
	f.parseFileName = parseFileNameFunc
}

func (f *FileStore) ParseFileName(path string) (int, int, error) {
	return f.parseFileName(path)
}

// SetCurrentGenerationFunc must be set before using FileStore.
func (f *FileStore) SetCurrentGenerationFunc(fn func() int) {
	f.currentGenerationFunc = fn
}

// WithPageFaultLimiter sets the rate limiter used for limiting page faults.
func (f *FileStore) WithPageFaultLimiter(limiter *rate.Limiter) {
	f.pageFaultLimiter = limiter
}

// WithLogger sets the logger on the file store.
func (f *FileStore) WithLogger(log *zap.Logger) {
	f.logger = log.With(zap.String("service", "filestore"))
	f.purger.logger = f.logger
}

// FileStoreStatistics keeps statistics about the file store.
type FileStoreStatistics struct {
	SDiskBytes int64
	SFileCount int64
}

// fileTracker tracks file counts and sizes within the FileStore.
//
// As well as being responsible for providing atomic reads and writes to the
// statistics, fileTracker also mirrors any changes to the external prometheus
// metrics, which the Engine exposes.
//
// *NOTE* - fileTracker fields should not be directory modified. Doing so
// could result in the Engine exposing inaccurate metrics.
type fileTracker struct {
	metrics   *fileMetrics
	labels    prometheus.Labels
	diskBytes uint64
}

func newFileTracker(metrics *fileMetrics, defaultLabels prometheus.Labels) *fileTracker {
	return &fileTracker{metrics: metrics, labels: defaultLabels}
}

// Labels returns a copy of the default labels used by the tracker's metrics.
// The returned map is safe for modification.
func (t *fileTracker) Labels() prometheus.Labels {
	labels := make(prometheus.Labels, len(t.labels))
	for k, v := range t.labels {
		labels[k] = v
	}
	return labels
}

// Bytes returns the number of bytes in use on disk.
func (t *fileTracker) Bytes() uint64 { return atomic.LoadUint64(&t.diskBytes) }

// SetBytes sets the number of bytes in use on disk.
func (t *fileTracker) SetBytes(bytes map[int]uint64) {
	total := uint64(0)
	labels := t.Labels()
	sizes := make(map[string]uint64)
	for k, v := range bytes {
		label := formatLevel(uint64(k))
		sizes[label] += v
		total += v
	}
	for k, v := range sizes {
		labels["level"] = k
		t.metrics.DiskSize.With(labels).Set(float64(v))
	}
	atomic.StoreUint64(&t.diskBytes, total)
}

// AddBytes increases the number of bytes.
func (t *fileTracker) AddBytes(bytes uint64, level int) {
	atomic.AddUint64(&t.diskBytes, bytes)

	labels := t.Labels()
	labels["level"] = formatLevel(uint64(level))
	t.metrics.DiskSize.With(labels).Add(float64(bytes))
}

// SetFileCount sets the number of files in the FileStore.
func (t *fileTracker) SetFileCount(files map[int]uint64) {
	labels := t.Labels()
	counts := make(map[string]uint64)
	for k, v := range files {
		label := formatLevel(uint64(k))
		counts[label] += v
	}
	for k, v := range counts {
		labels["level"] = k
		t.metrics.Files.With(labels).Set(float64(v))
	}
}

func (t *fileTracker) ClearFileCounts() {
	labels := t.Labels()
	for i := uint64(1); i <= 4; i++ {
		labels["level"] = formatLevel(i)
		t.metrics.Files.With(labels).Set(float64(0))
	}
}

func (t *fileTracker) ClearDiskSizes() {
	labels := t.Labels()
	for i := uint64(1); i <= 4; i++ {
		labels["level"] = formatLevel(i)
		t.metrics.DiskSize.With(labels).Set(float64(0))
	}
}

func formatLevel(level uint64) string {
	if level >= 4 {
		return "4+"
	} else {
		return fmt.Sprintf("%d", level)
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

// CurrentGeneration returns the current generation of the TSM files.
// Delegates to currentGenerationFunc, if set. Only called by tests.
func (f *FileStore) CurrentGeneration() int {
	if fn := f.currentGenerationFunc; fn != nil {
		return fn()
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.currentGeneration
}

// NextGeneration increments the max file ID and returns the new value.
// Delegates to currentGenerationFunc, if set.
func (f *FileStore) NextGeneration() int {
	if fn := f.currentGenerationFunc; fn != nil {
		return fn()
	}

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

	return ki.Err()
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

type unrefs []TSMFile

func (u *unrefs) Unref() {
	for _, f := range *u {
		f.Unref()
	}
}

// ForEachFile calls fn for all TSM files or until fn returns false.
// fn is called on the same goroutine as the caller.
func (f *FileStore) ForEachFile(fn func(f TSMFile) bool) {
	f.mu.RLock()
	files := make(unrefs, 0, len(f.files))
	defer files.Unref()

	for _, f := range f.files {
		f.Ref()
		files = append(files, f)
		if !fn(f) {
			break
		}
	}
	f.mu.RUnlock()
}

// Apply calls fn on each TSMFile in the store concurrently. The level of
// concurrency is set to GOMAXPROCS.
func (f *FileStore) Apply(fn func(r TSMFile) error) error {
	// Limit apply fn to number of cores
	limiter := limiter.NewFixed(runtime.GOMAXPROCS(0))

	f.mu.RLock()
	errC := make(chan error, len(f.files))

	for _, f := range f.files {
		go func(r TSMFile) {
			limiter.Take()
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

	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// find the current max ID for temp directories
	tmpfiles, err := ioutil.ReadDir(f.dir)
	if err != nil {
		return err
	}
	ext := fmt.Sprintf(".%s", TmpTSMFileExtension)
	for _, fi := range tmpfiles {
		if fi.IsDir() && strings.HasSuffix(fi.Name(), ext) {
			ss := strings.Split(filepath.Base(fi.Name()), ".")
			if len(ss) == 2 {
				if i, err := strconv.Atoi(ss[0]); err != nil {
					if i > f.currentTempDirID {
						f.currentTempDirID = i
					}
				}
			}
		}
	}

	files, err := filepath.Glob(filepath.Join(f.dir, fmt.Sprintf("*.%s", TSMFileExtension)))
	if err != nil {
		return err
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
			return err
		}

		if f.currentGenerationFunc == nil && generation >= f.currentGeneration {
			f.currentGeneration = generation + 1
		}

		file, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("error opening file %s: %v", fn, err)
		}

		go func(idx int, file *os.File) {
			// Ensure a limited number of TSM files are loaded at once.
			// Systems which have very large datasets (1TB+) can have thousands
			// of TSM files which can cause extremely long load times.
			f.openLimiter.Take()
			defer f.openLimiter.Release()

			start := time.Now()
			df, err := NewTSMReader(file,
				WithMadviseWillNeed(f.tsmMMAPWillNeed),
				WithTSMReaderPageFaultLimiter(f.pageFaultLimiter, f.readTracker.AddPageFaults),
				WithTSMReaderLogger(f.logger))
			f.logger.Info("Opened file",
				zap.String("path", file.Name()),
				zap.Int("id", idx),
				zap.Duration("duration", time.Since(start)))

			// If we are unable to read a TSM file then log the error, rename
			// the file, and continue loading the shard without it.
			if err != nil {
				f.logger.Error("Cannot read corrupt tsm file, renaming", zap.String("path", file.Name()), zap.Int("id", idx), zap.Error(err))
				if e := fs.RenameFile(file.Name(), file.Name()+"."+BadTSMFileExtension); e != nil {
					f.logger.Error("Cannot rename corrupt tsm file", zap.String("path", file.Name()), zap.Int("id", idx), zap.Error(e))
					readerC <- &res{r: df, err: fmt.Errorf("cannot rename corrupt file %s: %v", file.Name(), e)}
					return
				}
			}

			df.WithObserver(f.obs)
			readerC <- &res{r: df}
		}(i, file)
	}

	var lm int64
	counts := make(map[int]uint64, 4)
	sizes := make(map[int]uint64, 4)
	for i := 1; i <= 4; i++ {
		counts[i] = 0
		sizes[i] = 0
	}
	for range files {
		res := <-readerC
		if res.err != nil {
			return res.err
		} else if res.r == nil {
			continue
		}
		f.files = append(f.files, res.r)
		name := filepath.Base(res.r.Stats().Path)
		_, seq, err := f.parseFileName(name)
		if err != nil {
			return err
		}
		counts[seq]++

		// Accumulate file store size stats
		totalSize := uint64(res.r.Size())
		for _, ts := range res.r.TombstoneFiles() {
			totalSize += uint64(ts.Size)
		}
		sizes[seq] += totalSize

		// Re-initialize the lastModified time for the file store
		if res.r.LastModified() > lm {
			lm = res.r.LastModified()
		}

	}
	f.lastModified = time.Unix(0, lm).UTC()
	close(readerC)

	sort.Sort(tsmReaders(f.files))
	f.tracker.SetBytes(sizes)
	f.tracker.SetFileCount(counts)
	return nil
}

// Close closes the file store.
func (f *FileStore) Close() error {
	// Make the object appear closed to other method calls.
	f.mu.Lock()

	files := f.files

	f.lastFileStats = nil
	f.files = nil
	f.tracker.ClearFileCounts()

	// Let other methods access this closed object while we do the actual closing.
	f.mu.Unlock()

	for _, file := range files {
		err := file.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// DiskSizeBytes returns the total number of bytes consumed by the files in the FileStore.
func (f *FileStore) DiskSizeBytes() int64 { return int64(f.tracker.Bytes()) }

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
func (f *FileStore) TSMReader(path string) *TSMReader {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, r := range f.files {
		if r.Path() == path {
			r.Ref()
			return r.(*TSMReader)
		}
	}
	return nil
}

// KeyCursor returns a KeyCursor for key and t across the files in the FileStore.
func (f *FileStore) KeyCursor(ctx context.Context, key []byte, t int64, ascending bool) *KeyCursor {
	f.mu.RLock()
	defer f.mu.RUnlock()
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

		// Observe the associated statistics file, if available.
		statsFile := StatsFilename(file)
		if _, err := os.Stat(statsFile); err == nil {
			if err := f.obs.FileFinishing(statsFile); err != nil {
				return err
			}
		}

		var newName = file
		if strings.HasSuffix(file, tsmTmpExt) {
			// The new TSM files have a tmp extension.  First rename them.
			newName = file[:len(file)-4]
			if err := fs.RenameFile(file, newName); err != nil {
				return err
			}
		}

		fd, err := os.Open(newName)
		if err != nil {
			return err
		}

		// Keep track of the new mod time
		if stat, err := fd.Stat(); err == nil {
			if maxTime.IsZero() || stat.ModTime().UTC().After(maxTime) {
				maxTime = stat.ModTime().UTC()
			}
		}

		tsm, err := NewTSMReader(fd,
			WithMadviseWillNeed(f.tsmMMAPWillNeed),
			WithTSMReaderPageFaultLimiter(f.pageFaultLimiter, f.readTracker.AddPageFaults),
			WithTSMReaderLogger(f.logger))
		if err != nil {
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

				// Remove associated stats file.
				statsFile := StatsFilename(file.Path())
				if _, err := os.Stat(statsFile); err == nil {
					if err := f.obs.FileUnlinking(statsFile); err != nil {
						return err
					}
				}

				for _, t := range file.TombstoneFiles() {
					if err := f.obs.FileUnlinking(t.Path); err != nil {
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
					for _, t := range file.TombstoneFiles() {
						deletes = append(deletes, t.Path)
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

	if err := fs.SyncDir(f.dir); err != nil {
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
	f.tracker.ClearFileCounts()
	f.tracker.ClearDiskSizes()

	// Recalculate the disk size stat
	sizes := make(map[int]uint64, 4)
	counts := make(map[int]uint64, 4)
	for _, file := range f.files {
		size := uint64(file.Size())
		for _, ts := range file.TombstoneFiles() {
			size += uint64(ts.Size)
		}
		_, seq, err := f.parseFileName(file.Path())
		if err != nil {
			return err
		}
		sizes[seq] += size
		counts[seq]++
	}
	f.tracker.SetBytes(sizes)
	f.tracker.SetFileCount(counts)

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
			return BlockCount(block)
		}
	}
	return 0
}

// We need to determine the possible files that may be accessed by this query given
// the time range.
func (f *FileStore) cost(key []byte, min, max int64) query.IteratorCost {
	var entries []IndexEntry
	var err error
	var trbuf []TimeRange

	cost := query.IteratorCost{}
	for _, fd := range f.files {
		minTime, maxTime := fd.TimeRange()
		if !(maxTime > min && minTime < max) {
			continue
		}
		skipped := true
		trbuf = fd.TombstoneRange(key, trbuf[:0])

		entries, err = fd.ReadEntries(key, entries)
		if err != nil {
			// TODO(jeff): log this somehow? we have an invalid entry in the tsm index
			continue
		}

	ENTRIES:
		for i := 0; i < len(entries); i++ {
			ie := entries[i]

			if !(ie.MaxTime > min && ie.MinTime < max) {
				continue
			}

			// Skip any blocks only contain values that are tombstoned.
			for _, t := range trbuf {
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
	var entries []IndexEntry
	var err error
	var trbuf []TimeRange

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
		trbuf = fd.TombstoneRange(key, trbuf[:0])

		// This file could potential contain points we are looking for so find the blocks for
		// the given key.
		entries, err = fd.ReadEntries(key, entries)
		if err != nil {
			// TODO(jeff): log this somehow? we have an invalid entry in the tsm index
			continue
		}

	LOOP:
		for i := 0; i < len(entries); i++ {
			ie := entries[i]

			// Skip any blocks only contain values that are tombstoned.
			for _, t := range trbuf {
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

// CreateSnapshot creates hardlinks for all tsm and tombstone files
// in the path provided.
func (f *FileStore) CreateSnapshot(ctx context.Context) (backupID int, backupDirFullPath string, err error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	span.LogKV("dir", f.dir)

	f.mu.Lock()
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
	backupID = f.currentTempDirID
	f.mu.Unlock()

	backupDirFullPath = f.InternalBackupPath(backupID)

	// create the tmp directory and add the hard links. there is no longer any shared
	// mutable state.
	err = os.Mkdir(backupDirFullPath, 0777)
	if err != nil {
		return 0, "", err
	}
	for _, tsmf := range files {
		newpath := filepath.Join(backupDirFullPath, filepath.Base(tsmf.Path()))
		if err := os.Link(tsmf.Path(), newpath); err != nil {
			return 0, "", fmt.Errorf("error creating tsm hard link: %q", err)
		}
		for _, tf := range tsmf.TombstoneFiles() {
			newpath := filepath.Join(backupDirFullPath, filepath.Base(tf.Path))
			if err := os.Link(tf.Path, newpath); err != nil {
				return 0, "", fmt.Errorf("error creating tombstone hard link: %q", err)
			}
		}
	}

	return backupID, backupDirFullPath, nil
}

func (f *FileStore) InternalBackupPath(backupID int) string {
	return filepath.Join(f.dir, fmt.Sprintf("%d.%s", backupID, TmpTSMFileExtension))
}

// MeasurementStats returns the sum of all measurement stats within the store.
func (f *FileStore) MeasurementStats() (MeasurementStats, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	stats := NewMeasurementStats()
	for _, file := range f.files {
		s, err := file.MeasurementStats()
		if err != nil {
			return nil, err
		}
		stats.Add(s)
	}
	return stats, nil
}

// FormatFileNameFunc is executed when generating a new TSM filename.
// Source filenames are provided via src.
type FormatFileNameFunc func(generation, sequence int) string

// DefaultFormatFileName is the default implementation to format TSM filenames.
func DefaultFormatFileName(generation, sequence int) string {
	return fmt.Sprintf("%015d-%09d", generation, sequence)
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

	generation, err := strconv.ParseUint(id[:idx], 10, 64)
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

	// trbuf is scratch allocation space for tombstones
	trbuf []TimeRange

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

// seekN returns the number of seek locations.
func (c *KeyCursor) seekN() int {
	return len(c.seeks)
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

	// If we have overlapping blocks, append all their values so we can dedup
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
		c.current = append(c.current, nil)
	} else {
		c.current = c.current[:1]
	}
	c.current[0] = c.seeks[c.pos]

	// If we have overlapping blocks, append all their values so we can dedup
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
