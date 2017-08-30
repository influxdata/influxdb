// Package tsm1 provides a TSDB in the Time Structured Merge tree format.
package tsm1 // import "github.com/influxdata/influxdb/tsdb/engine/tsm1"

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb/index/inmem"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bytesutil"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/tsdb"
	_ "github.com/influxdata/influxdb/tsdb/index"
	"github.com/uber-go/zap"
)

//go:generate tmpl -data=@iterator.gen.go.tmpldata iterator.gen.go.tmpl
//go:generate tmpl -data=@file_store.gen.go.tmpldata file_store.gen.go.tmpl
//go:generate tmpl -data=@encoding.gen.go.tmpldata encoding.gen.go.tmpl
//go:generate tmpl -data=@compact.gen.go.tmpldata compact.gen.go.tmpl

func init() {
	tsdb.RegisterEngine("tsm1", NewEngine)
}

var (
	// Ensure Engine implements the interface.
	_ tsdb.Engine = &Engine{}
	// Static objects to prevent small allocs.
	timeBytes              = []byte("time")
	keyFieldSeparatorBytes = []byte(keyFieldSeparator)
)

const (
	// keyFieldSeparator separates the series key from the field name in the composite key
	// that identifies a specific field in series
	keyFieldSeparator = "#!~#"
)

// Statistics gathered by the engine.
const (
	statCacheCompactions        = "cacheCompactions"
	statCacheCompactionsActive  = "cacheCompactionsActive"
	statCacheCompactionError    = "cacheCompactionErr"
	statCacheCompactionDuration = "cacheCompactionDuration"

	statTSMLevel1Compactions        = "tsmLevel1Compactions"
	statTSMLevel1CompactionsActive  = "tsmLevel1CompactionsActive"
	statTSMLevel1CompactionError    = "tsmLevel1CompactionErr"
	statTSMLevel1CompactionDuration = "tsmLevel1CompactionDuration"

	statTSMLevel2Compactions        = "tsmLevel2Compactions"
	statTSMLevel2CompactionsActive  = "tsmLevel2CompactionsActive"
	statTSMLevel2CompactionError    = "tsmLevel2CompactionErr"
	statTSMLevel2CompactionDuration = "tsmLevel2CompactionDuration"

	statTSMLevel3Compactions        = "tsmLevel3Compactions"
	statTSMLevel3CompactionsActive  = "tsmLevel3CompactionsActive"
	statTSMLevel3CompactionError    = "tsmLevel3CompactionErr"
	statTSMLevel3CompactionDuration = "tsmLevel3CompactionDuration"

	statTSMOptimizeCompactions        = "tsmOptimizeCompactions"
	statTSMOptimizeCompactionsActive  = "tsmOptimizeCompactionsActive"
	statTSMOptimizeCompactionError    = "tsmOptimizeCompactionErr"
	statTSMOptimizeCompactionDuration = "tsmOptimizeCompactionDuration"

	statTSMFullCompactions        = "tsmFullCompactions"
	statTSMFullCompactionsActive  = "tsmFullCompactionsActive"
	statTSMFullCompactionError    = "tsmFullCompactionErr"
	statTSMFullCompactionDuration = "tsmFullCompactionDuration"
)

// Engine represents a storage engine with compressed blocks.
type Engine struct {
	mu sync.RWMutex

	// The following group of fields is used to track the state of level compactions within the
	// Engine. The WaitGroup is used to monitor the compaction goroutines, the 'done' channel is
	// used to signal those goroutines to shutdown. Every request to disable level compactions will
	// call 'Wait' on 'wg', with the first goroutine to arrive (levelWorkers == 0 while holding the
	// lock) will close the done channel and re-assign 'nil' to the variable. Re-enabling will
	// decrease 'levelWorkers', and when it decreases to zero, level compactions will be started
	// back up again.

	wg           sync.WaitGroup // waitgroup for active level compaction goroutines
	done         chan struct{}  // channel to signal level compactions to stop
	levelWorkers int            // Number of "workers" that expect compactions to be in a disabled state

	snapDone chan struct{}  // channel to signal snapshot compactions to stop
	snapWG   sync.WaitGroup // waitgroup for running snapshot compactions

	id           uint64
	database     string
	path         string
	logger       zap.Logger // Logger to be used for important messages
	traceLogger  zap.Logger // Logger to be used when trace-logging is on.
	traceLogging bool

	index    tsdb.Index
	fieldset *tsdb.MeasurementFieldSet

	WAL            *WAL
	Cache          *Cache
	Compactor      *Compactor
	CompactionPlan CompactionPlanner
	FileStore      *FileStore

	MaxPointsPerBlock int

	// CacheFlushMemorySizeThreshold specifies the minimum size threshodl for
	// the cache when the engine should write a snapshot to a TSM file
	CacheFlushMemorySizeThreshold uint64

	// CacheFlushWriteColdDuration specifies the length of time after which if
	// no writes have been committed to the WAL, the engine will write
	// a snapshot of the cache to a TSM file
	CacheFlushWriteColdDuration time.Duration

	// Controls whether to enabled compactions when the engine is open
	enableCompactionsOnOpen bool

	stats *EngineStatistics

	// The limiter for concurrent compactions
	compactionLimiter limiter.Fixed
}

// NewEngine returns a new instance of Engine.
func NewEngine(id uint64, idx tsdb.Index, database, path string, walPath string, opt tsdb.EngineOptions) tsdb.Engine {
	w := NewWAL(walPath)
	w.syncDelay = time.Duration(opt.Config.WALFsyncDelay)

	fs := NewFileStore(path)
	cache := NewCache(uint64(opt.Config.CacheMaxMemorySize), path)

	c := &Compactor{
		Dir:       path,
		FileStore: fs,
	}

	logger := zap.New(zap.NullEncoder())
	e := &Engine{
		id:           id,
		database:     database,
		path:         path,
		index:        idx,
		logger:       logger,
		traceLogger:  logger,
		traceLogging: opt.Config.TraceLoggingEnabled,

		fieldset: tsdb.NewMeasurementFieldSet(),

		WAL:   w,
		Cache: cache,

		FileStore:      fs,
		Compactor:      c,
		CompactionPlan: NewDefaultPlanner(fs, time.Duration(opt.Config.CompactFullWriteColdDuration)),

		CacheFlushMemorySizeThreshold: opt.Config.CacheSnapshotMemorySize,
		CacheFlushWriteColdDuration:   time.Duration(opt.Config.CacheSnapshotWriteColdDuration),
		enableCompactionsOnOpen:       true,
		stats:             &EngineStatistics{},
		compactionLimiter: opt.CompactionLimiter,
	}

	// Attach fieldset to index.
	e.index.SetFieldSet(e.fieldset)

	if e.traceLogging {
		fs.enableTraceLogging(true)
		w.enableTraceLogging(true)
	}

	return e
}

// SetEnabled sets whether the engine is enabled.
func (e *Engine) SetEnabled(enabled bool) {
	e.enableCompactionsOnOpen = enabled
	e.SetCompactionsEnabled(enabled)
}

// SetCompactionsEnabled enables compactions on the engine.  When disabled
// all running compactions are aborted and new compactions stop running.
func (e *Engine) SetCompactionsEnabled(enabled bool) {
	if enabled {
		e.enableSnapshotCompactions()
		e.enableLevelCompactions(false)
	} else {
		e.disableSnapshotCompactions()
		e.disableLevelCompactions(false)
	}
}

// enableLevelCompactions will request that level compactions start back up again
//
// 'wait' signifies that a corresponding call to disableLevelCompactions(true) was made at some
// point, and the associated task that required disabled compactions is now complete
func (e *Engine) enableLevelCompactions(wait bool) {
	// If we don't need to wait, see if we're already enabled
	if !wait {
		e.mu.RLock()
		if e.done != nil {
			e.mu.RUnlock()
			return
		}
		e.mu.RUnlock()
	}

	e.mu.Lock()
	if wait {
		e.levelWorkers -= 1
	}
	if e.levelWorkers != 0 || e.done != nil {
		// still waiting on more workers or already enabled
		e.mu.Unlock()
		return
	}

	// last one to enable, start things back up
	e.Compactor.EnableCompactions()
	quit := make(chan struct{})
	e.done = quit

	e.wg.Add(4)
	e.mu.Unlock()

	go func() { defer e.wg.Done(); e.compactTSMFull(quit) }()
	go func() { defer e.wg.Done(); e.compactTSMLevel(true, 1, quit) }()
	go func() { defer e.wg.Done(); e.compactTSMLevel(true, 2, quit) }()
	go func() { defer e.wg.Done(); e.compactTSMLevel(false, 3, quit) }()
}

// disableLevelCompactions will stop level compactions before returning.
//
// If 'wait' is set to true, then a corresponding call to enableLevelCompactions(true) will be
// required before level compactions will start back up again.
func (e *Engine) disableLevelCompactions(wait bool) {
	e.mu.Lock()
	old := e.levelWorkers
	if wait {
		e.levelWorkers += 1
	}

	if old == 0 && e.done != nil {
		// Prevent new compactions from starting
		e.Compactor.DisableCompactions()

		// Stop all background compaction goroutines
		close(e.done)
		e.done = nil

	}

	e.mu.Unlock()
	e.wg.Wait()
}

func (e *Engine) enableSnapshotCompactions() {
	// Check if already enabled under read lock
	e.mu.RLock()
	if e.snapDone != nil {
		e.mu.RUnlock()
		return
	}
	e.mu.RUnlock()

	// Check again under write lock
	e.mu.Lock()
	if e.snapDone != nil {
		e.mu.Unlock()
		return
	}

	e.Compactor.EnableSnapshots()
	quit := make(chan struct{})
	e.snapDone = quit
	e.snapWG.Add(1)
	e.mu.Unlock()

	go func() { defer e.snapWG.Done(); e.compactCache(quit) }()
}

func (e *Engine) disableSnapshotCompactions() {
	e.mu.Lock()

	if e.snapDone != nil {
		close(e.snapDone)
		e.snapDone = nil
		e.Compactor.DisableSnapshots()
	}

	e.mu.Unlock()
	e.snapWG.Wait()
}

// Path returns the path the engine was opened with.
func (e *Engine) Path() string { return e.path }

func (e *Engine) SetFieldName(measurement []byte, name string) {
	e.index.SetFieldName(measurement, name)
}

func (e *Engine) MeasurementExists(name []byte) (bool, error) {
	return e.index.MeasurementExists(name)
}

func (e *Engine) MeasurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
	return e.index.MeasurementNamesByExpr(expr)
}

func (e *Engine) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	return e.index.MeasurementNamesByRegex(re)
}

// MeasurementFields returns the measurement fields for a measurement.
func (e *Engine) MeasurementFields(measurement []byte) *tsdb.MeasurementFields {
	return e.fieldset.CreateFieldsIfNotExists(measurement)
}

func (e *Engine) HasTagKey(name, key []byte) (bool, error) {
	return e.index.HasTagKey(name, key)
}

func (e *Engine) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	return e.index.MeasurementTagKeysByExpr(name, expr)
}

// MeasurementTagKeyValuesByExpr returns a set of tag values filtered by an expression.
//
// MeasurementTagKeyValuesByExpr relies on the provided tag keys being sorted.
// The caller can indicate the tag keys have been sorted by setting the
// keysSorted argument appropriately. Tag values are returned in a slice that
// is indexible according to the sorted order of the tag keys, e.g., the values
// for the earliest tag k will be available in index 0 of the returned values
// slice.
//
func (e *Engine) MeasurementTagKeyValuesByExpr(name []byte, keys []string, expr influxql.Expr, keysSorted bool) ([][]string, error) {
	return e.index.MeasurementTagKeyValuesByExpr(name, keys, expr, keysSorted)
}

func (e *Engine) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	return e.index.ForEachMeasurementTagKey(name, fn)
}

func (e *Engine) TagKeyCardinality(name, key []byte) int {
	return e.index.TagKeyCardinality(name, key)
}

// SeriesN returns the unique number of series in the index.
func (e *Engine) SeriesN() int64 {
	return e.index.SeriesN()
}

func (e *Engine) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	return e.index.SeriesSketches()
}

func (e *Engine) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	return e.index.MeasurementsSketches()
}

// LastModified returns the time when this shard was last modified.
func (e *Engine) LastModified() time.Time {
	walTime := e.WAL.LastWriteTime()
	fsTime := e.FileStore.LastModified()

	if walTime.After(fsTime) {
		return walTime
	}

	return fsTime
}

// EngineStatistics maintains statistics for the engine.
type EngineStatistics struct {
	CacheCompactions        int64 // Counter of cache compactions that have ever run.
	CacheCompactionsActive  int64 // Gauge of cache compactions currently running.
	CacheCompactionErrors   int64 // Counter of cache compactions that have failed due to error.
	CacheCompactionDuration int64 // Counter of number of wall nanoseconds spent in cache compactions.

	TSMCompactions        [3]int64 // Counter of TSM compactions (by level) that have ever run.
	TSMCompactionsActive  [3]int64 // Gauge of TSM compactions (by level) currently running.
	TSMCompactionErrors   [3]int64 // Counter of TSM compcations (by level) that have failed due to error.
	TSMCompactionDuration [3]int64 // Counter of number of wall nanoseconds spent in TSM compactions (by level).

	TSMOptimizeCompactions        int64 // Counter of optimize compactions that have ever run.
	TSMOptimizeCompactionsActive  int64 // Gauge of optimize compactions currently running.
	TSMOptimizeCompactionErrors   int64 // Counter of optimize compactions that have failed due to error.
	TSMOptimizeCompactionDuration int64 // Counter of number of wall nanoseconds spent in optimize compactions.

	TSMFullCompactions        int64 // Counter of full compactions that have ever run.
	TSMFullCompactionsActive  int64 // Gauge of full compactions currently running.
	TSMFullCompactionErrors   int64 // Counter of full compactions that have failed due to error.
	TSMFullCompactionDuration int64 // Counter of number of wall nanoseconds spent in full compactions.
}

// Statistics returns statistics for periodic monitoring.
func (e *Engine) Statistics(tags map[string]string) []models.Statistic {
	statistics := make([]models.Statistic, 0, 4)
	statistics = append(statistics, models.Statistic{
		Name: "tsm1_engine",
		Tags: tags,
		Values: map[string]interface{}{
			statCacheCompactions:        atomic.LoadInt64(&e.stats.CacheCompactions),
			statCacheCompactionsActive:  atomic.LoadInt64(&e.stats.CacheCompactionsActive),
			statCacheCompactionError:    atomic.LoadInt64(&e.stats.CacheCompactionErrors),
			statCacheCompactionDuration: atomic.LoadInt64(&e.stats.CacheCompactionDuration),

			statTSMLevel1Compactions:        atomic.LoadInt64(&e.stats.TSMCompactions[0]),
			statTSMLevel1CompactionsActive:  atomic.LoadInt64(&e.stats.TSMCompactionsActive[0]),
			statTSMLevel1CompactionError:    atomic.LoadInt64(&e.stats.TSMCompactionErrors[0]),
			statTSMLevel1CompactionDuration: atomic.LoadInt64(&e.stats.TSMCompactionDuration[0]),

			statTSMLevel2Compactions:        atomic.LoadInt64(&e.stats.TSMCompactions[1]),
			statTSMLevel2CompactionsActive:  atomic.LoadInt64(&e.stats.TSMCompactionsActive[1]),
			statTSMLevel2CompactionError:    atomic.LoadInt64(&e.stats.TSMCompactionErrors[1]),
			statTSMLevel2CompactionDuration: atomic.LoadInt64(&e.stats.TSMCompactionDuration[1]),

			statTSMLevel3Compactions:        atomic.LoadInt64(&e.stats.TSMCompactions[2]),
			statTSMLevel3CompactionsActive:  atomic.LoadInt64(&e.stats.TSMCompactionsActive[2]),
			statTSMLevel3CompactionError:    atomic.LoadInt64(&e.stats.TSMCompactionErrors[2]),
			statTSMLevel3CompactionDuration: atomic.LoadInt64(&e.stats.TSMCompactionDuration[2]),

			statTSMOptimizeCompactions:        atomic.LoadInt64(&e.stats.TSMOptimizeCompactions),
			statTSMOptimizeCompactionsActive:  atomic.LoadInt64(&e.stats.TSMOptimizeCompactionsActive),
			statTSMOptimizeCompactionError:    atomic.LoadInt64(&e.stats.TSMOptimizeCompactionErrors),
			statTSMOptimizeCompactionDuration: atomic.LoadInt64(&e.stats.TSMOptimizeCompactionDuration),

			statTSMFullCompactions:        atomic.LoadInt64(&e.stats.TSMFullCompactions),
			statTSMFullCompactionsActive:  atomic.LoadInt64(&e.stats.TSMFullCompactionsActive),
			statTSMFullCompactionError:    atomic.LoadInt64(&e.stats.TSMFullCompactionErrors),
			statTSMFullCompactionDuration: atomic.LoadInt64(&e.stats.TSMFullCompactionDuration),
		},
	})

	statistics = append(statistics, e.Cache.Statistics(tags)...)
	statistics = append(statistics, e.FileStore.Statistics(tags)...)
	statistics = append(statistics, e.WAL.Statistics(tags)...)
	return statistics
}

// DiskSize returns the total size in bytes of all TSM and WAL segments on disk.
func (e *Engine) DiskSize() int64 {
	return e.FileStore.DiskSizeBytes() + e.WAL.DiskSizeBytes()
}

// Open opens and initializes the engine.
func (e *Engine) Open() error {
	if err := os.MkdirAll(e.path, 0777); err != nil {
		return err
	}

	if err := e.cleanup(); err != nil {
		return err
	}

	if err := e.WAL.Open(); err != nil {
		return err
	}

	if err := e.FileStore.Open(); err != nil {
		return err
	}

	if err := e.reloadCache(); err != nil {
		return err
	}

	e.Compactor.Open()

	if e.enableCompactionsOnOpen {
		e.SetCompactionsEnabled(true)
	}

	return nil
}

// Close closes the engine. Subsequent calls to Close are a nop.
func (e *Engine) Close() error {
	e.SetCompactionsEnabled(false)

	// Lock now and close everything else down.
	e.mu.Lock()
	defer e.mu.Unlock()
	e.done = nil // Ensures that the channel will not be closed again.

	if err := e.FileStore.Close(); err != nil {
		return err
	}
	return e.WAL.Close()
}

// WithLogger sets the logger for the engine.
func (e *Engine) WithLogger(log zap.Logger) {
	e.logger = log.With(zap.String("engine", "tsm1"))

	if e.traceLogging {
		e.traceLogger = e.logger
	}

	e.WAL.WithLogger(e.logger)
	e.FileStore.WithLogger(e.logger)
}

// LoadMetadataIndex loads the shard metadata into memory.
func (e *Engine) LoadMetadataIndex(shardID uint64, index tsdb.Index) error {
	now := time.Now()

	// Save reference to index for iterator creation.
	e.index = index

	if err := e.FileStore.WalkKeys(func(key []byte, typ byte) error {
		fieldType, err := tsmFieldTypeToInfluxQLDataType(typ)
		if err != nil {
			return err
		}

		if err := e.addToIndexFromKey(key, fieldType); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// load metadata from the Cache
	if err := e.Cache.ApplyEntryFn(func(key []byte, entry *entry) error {
		fieldType, err := entry.values.InfluxQLType()
		if err != nil {
			e.logger.Info(fmt.Sprintf("error getting the data type of values for key %s: %s", key, err.Error()))
		}

		if err := e.addToIndexFromKey(key, fieldType); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	e.traceLogger.Info(fmt.Sprintf("Meta data index for shard %d loaded in %v", shardID, time.Since(now)))
	return nil
}

// IsIdle returns true if the cache is empty, there are no running compactions and the
// shard is fully compacted.
func (e *Engine) IsIdle() bool {
	cacheEmpty := e.Cache.Size() == 0

	runningCompactions := atomic.LoadInt64(&e.stats.CacheCompactionsActive)
	runningCompactions += atomic.LoadInt64(&e.stats.TSMCompactionsActive[0])
	runningCompactions += atomic.LoadInt64(&e.stats.TSMCompactionsActive[1])
	runningCompactions += atomic.LoadInt64(&e.stats.TSMCompactionsActive[2])
	runningCompactions += atomic.LoadInt64(&e.stats.TSMFullCompactionsActive)
	runningCompactions += atomic.LoadInt64(&e.stats.TSMOptimizeCompactionsActive)

	return cacheEmpty && runningCompactions == 0 && e.CompactionPlan.FullyCompacted()
}

// Backup writes a tar archive of any TSM files modified since the passed
// in time to the passed in writer. The basePath will be prepended to the names
// of the files in the archive. It will force a snapshot of the WAL first
// then perform the backup with a read lock against the file store. This means
// that new TSM files will not be able to be created in this shard while the
// backup is running. For shards that are still acively getting writes, this
// could cause the WAL to backup, increasing memory usage and evenutally rejecting writes.
func (e *Engine) Backup(w io.Writer, basePath string, since time.Time) error {
	path, err := e.CreateSnapshot()
	if err != nil {
		return err
	}

	if err := e.index.SnapshotTo(path); err != nil {
		return err
	}

	tw := tar.NewWriter(w)
	defer tw.Close()

	// Remove the temporary snapshot dir
	defer os.RemoveAll(path)

	// Recursively read all files from path.
	files, err := readDir(path, "")
	if err != nil {
		return err
	}

	// Filter paths to only changed files.
	var filtered []string
	for _, file := range files {
		fi, err := os.Stat(filepath.Join(path, file))
		if err != nil {
			return err
		} else if !fi.ModTime().After(since) {
			continue
		}
		filtered = append(filtered, file)
	}
	if len(filtered) == 0 {
		return nil
	}

	for _, f := range filtered {
		if err := e.writeFileToBackup(f, basePath, filepath.Join(path, f), tw); err != nil {
			return err
		}
	}

	return nil
}

// writeFileToBackup copies the file into the tar archive. Files will use the shardRelativePath
// in their names. This should be the <db>/<retention policy>/<id> part of the path.
func (e *Engine) writeFileToBackup(name string, shardRelativePath, fullPath string, tw *tar.Writer) error {
	f, err := os.Stat(fullPath)
	if err != nil {
		return err
	}

	h := &tar.Header{
		Name:    filepath.ToSlash(filepath.Join(shardRelativePath, name)),
		ModTime: f.ModTime(),
		Size:    f.Size(),
		Mode:    int64(f.Mode()),
	}
	if err := tw.WriteHeader(h); err != nil {
		return err
	}
	fr, err := os.Open(fullPath)
	if err != nil {
		return err
	}

	defer fr.Close()

	_, err = io.CopyN(tw, fr, h.Size)

	return err
}

// Restore reads a tar archive generated by Backup().
// Only files that match basePath will be copied into the directory. This obtains
// a write lock so no operations can be performed while restoring.
func (e *Engine) Restore(r io.Reader, basePath string) error {
	return e.overlay(r, basePath, false)
}

// Import reads a tar archive generated by Backup() and adds each
// file matching basePath as a new TSM file.  This obtains
// a write lock so no operations can be performed while Importing.
func (e *Engine) Import(r io.Reader, basePath string) error {
	return e.overlay(r, basePath, true)
}

// overlay reads a tar archive generated by Backup() and adds each file
// from the archive matching basePath to the shard.
// If asNew is true, each file will be installed as a new TSM file even if an
// existing file with the same name in the backup exists.
func (e *Engine) overlay(r io.Reader, basePath string, asNew bool) error {
	// Copy files from archive while under lock to prevent reopening.
	newFiles, err := func() ([]string, error) {
		e.mu.Lock()
		defer e.mu.Unlock()

		var newFiles []string
		tr := tar.NewReader(r)
		for {
			if fileName, err := e.readFileFromBackup(tr, basePath, asNew); err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			} else if fileName != "" {
				newFiles = append(newFiles, fileName)
			}
		}

		if err := syncDir(e.path); err != nil {
			return nil, err
		}

		if err := e.FileStore.Replace(nil, newFiles); err != nil {
			return nil, err
		}
		return newFiles, nil
	}()

	if err != nil {
		return err
	}

	// Load any new series keys to the index
	readers := make([]chan seriesKey, 0, len(newFiles))
	for _, f := range newFiles {
		ch := make(chan seriesKey, 1)
		readers = append(readers, ch)

		// If asNew is true, the files created from readFileFromBackup will be new ones
		// having a temp extension.
		f = strings.TrimSuffix(f, ".tmp")

		fd, err := os.Open(f)
		if err != nil {
			return err
		}

		r, err := NewTSMReader(fd)
		if err != nil {
			return err
		}
		defer r.Close()

		go func(c chan seriesKey, r *TSMReader) {
			n := r.KeyCount()
			for i := 0; i < n; i++ {
				key, typ := r.KeyAt(i)
				c <- seriesKey{key, typ}
			}
			close(c)
		}(ch, r)
	}

	// Merge and dedup all the series keys across each reader to reduce
	// lock contention on the index.
	merged := merge(readers...)
	for v := range merged {
		fieldType, err := tsmFieldTypeToInfluxQLDataType(v.typ)
		if err != nil {
			return err
		}

		if err := e.addToIndexFromKey(v.key, fieldType); err != nil {
			return err
		}
	}
	return nil
}

// readFileFromBackup copies the next file from the archive into the shard.
// The file is skipped if it does not have a matching shardRelativePath prefix.
// If asNew is true, each file will be installed as a new TSM file even if an
// existing file with the same name in the backup exists.
func (e *Engine) readFileFromBackup(tr *tar.Reader, shardRelativePath string, asNew bool) (string, error) {
	// Read next archive file.
	hdr, err := tr.Next()
	if err != nil {
		return "", err
	}

	nativeFileName := filepath.FromSlash(hdr.Name)

	// Skip file if it does not have a matching prefix.
	if !filepath.HasPrefix(nativeFileName, shardRelativePath) {
		return "", nil
	}
	filename, err := filepath.Rel(shardRelativePath, nativeFileName)
	if err != nil {
		return "", err
	}

	if asNew {
		filename = fmt.Sprintf("%09d-%09d.%s", e.FileStore.NextGeneration(), 1, TSMFileExtension)
	}

	destPath := filepath.Join(e.path, filename)
	tmp := destPath + ".tmp"

	// Create new file on disk.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// Copy from archive to the file.
	if _, err := io.CopyN(f, tr, hdr.Size); err != nil {
		return "", err
	}

	// Sync to disk & close.
	if err := f.Sync(); err != nil {
		return "", err
	}

	return tmp, nil
}

// addToIndexFromKey will pull the measurement name, series key, and field name from a composite key and add it to the
// database index and measurement fields
func (e *Engine) addToIndexFromKey(key []byte, fieldType influxql.DataType) error {
	seriesKey, field := SeriesAndFieldFromCompositeKey(key)
	name := tsdb.MeasurementFromSeriesKey(seriesKey)

	mf := e.fieldset.CreateFieldsIfNotExists(name)
	if err := mf.CreateFieldIfNotExists(field, fieldType, false); err != nil {
		return err
	}

	// Build in-memory index, if necessary.
	if e.index.Type() == inmem.IndexName {
		tags, _ := models.ParseTags(seriesKey)
		if err := e.index.InitializeSeries(seriesKey, name, tags); err != nil {
			return err
		}
	}

	return nil
}

// WritePoints writes metadata and point data into the engine.
// It returns an error if new points are added to an existing key.
func (e *Engine) WritePoints(points []models.Point) error {
	values := make(map[string][]Value, len(points))
	var keyBuf []byte
	var baseLen int
	for _, p := range points {
		keyBuf = append(keyBuf[:0], p.Key()...)
		keyBuf = append(keyBuf, keyFieldSeparator...)
		baseLen = len(keyBuf)
		iter := p.FieldIterator()
		t := p.Time().UnixNano()
		for iter.Next() {
			// Skip fields name "time", they are illegal
			if bytes.Equal(iter.FieldKey(), timeBytes) {
				continue
			}

			keyBuf = append(keyBuf[:baseLen], iter.FieldKey()...)
			var v Value
			switch iter.Type() {
			case models.Float:
				fv, err := iter.FloatValue()
				if err != nil {
					return err
				}
				v = NewFloatValue(t, fv)
			case models.Integer:
				iv, err := iter.IntegerValue()
				if err != nil {
					return err
				}
				v = NewIntegerValue(t, iv)
			case models.Unsigned:
				iv, err := iter.UnsignedValue()
				if err != nil {
					return err
				}
				v = NewUnsignedValue(t, iv)
			case models.String:
				v = NewStringValue(t, iter.StringValue())
			case models.Boolean:
				bv, err := iter.BooleanValue()
				if err != nil {
					return err
				}
				v = NewBooleanValue(t, bv)
			default:
				return fmt.Errorf("unknown field type for %s: %s", string(iter.FieldKey()), p.String())
			}
			values[string(keyBuf)] = append(values[string(keyBuf)], v)
		}
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// first try to write to the cache
	err := e.Cache.WriteMulti(values)
	if err != nil {
		return err
	}

	_, err = e.WAL.WriteMulti(values)
	return err
}

// containsSeries returns a map of keys indicating whether the key exists and
// has values or not.
func (e *Engine) containsSeries(keys [][]byte) (map[string]bool, error) {
	// keyMap is used to see if a given key exists.  keys
	// are the measurement + tagset (minus separate & field)
	keyMap := map[string]bool{}
	for _, k := range keys {
		keyMap[string(k)] = false
	}

	for _, k := range e.Cache.unsortedKeys() {
		seriesKey, _ := SeriesAndFieldFromCompositeKey([]byte(k))
		keyMap[string(seriesKey)] = true
	}

	if err := e.FileStore.WalkKeys(func(k []byte, _ byte) error {
		seriesKey, _ := SeriesAndFieldFromCompositeKey(k)
		if _, ok := keyMap[string(seriesKey)]; ok {
			keyMap[string(seriesKey)] = true
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return keyMap, nil
}

// deleteSeries removes all series keys from the engine.
func (e *Engine) deleteSeries(seriesKeys [][]byte) error {
	return e.DeleteSeriesRange(seriesKeys, math.MinInt64, math.MaxInt64)
}

// DeleteSeriesRange removes the values between min and max (inclusive) from all series.
func (e *Engine) DeleteSeriesRange(seriesKeys [][]byte, min, max int64) error {
	if len(seriesKeys) == 0 {
		return nil
	}

	// Ensure keys are sorted since lower layers require them to be.
	if !bytesutil.IsSorted(seriesKeys) {
		bytesutil.Sort(seriesKeys)
	}

	// Disable and abort running compactions so that tombstones added existing tsm
	// files don't get removed.  This would cause deleted measurements/series to
	// re-appear once the compaction completed.  We only disable the level compactions
	// so that snapshotting does not stop while writing out tombstones.  If it is stopped,
	// and writing tombstones takes a long time, writes can get rejected due to the cache
	// filling up.
	e.disableLevelCompactions(true)
	defer e.enableLevelCompactions(true)

	tempKeys := seriesKeys[:]
	deleteKeys := make([][]byte, 0, len(seriesKeys))
	// go through the keys in the file store
	if err := e.FileStore.WalkKeys(func(k []byte, _ byte) error {
		seriesKey, _ := SeriesAndFieldFromCompositeKey(k)

		// Both tempKeys and keys walked are sorted, skip any passed in keys
		// that don't exist in our key set.
		for len(tempKeys) > 0 && bytes.Compare(tempKeys[0], seriesKey) < 0 {
			tempKeys = tempKeys[1:]
		}

		// Keys match, add the full series key to delete.
		if len(tempKeys) > 0 && bytes.Equal(tempKeys[0], seriesKey) {
			deleteKeys = append(deleteKeys, k)
		}

		return nil
	}); err != nil {
		return err
	}

	if err := e.FileStore.DeleteRange(deleteKeys, min, max); err != nil {
		return err
	}

	// find the keys in the cache and remove them
	walKeys := deleteKeys[:0]

	// ApplySerialEntryFn cannot return an error in this invocation.
	_ = e.Cache.ApplyEntryFn(func(k []byte, _ *entry) error {
		seriesKey, _ := SeriesAndFieldFromCompositeKey([]byte(k))

		// Cache does not walk keys in sorted order, so search the sorted
		// series we need to delete to see if any of the cache keys match.
		i := bytesutil.SearchBytes(seriesKeys, seriesKey)
		if i < len(seriesKeys) && bytes.Equal(seriesKey, seriesKeys[i]) {
			// k is the measurement + tags + sep + field
			walKeys = append(walKeys, k)
		}
		return nil
	})

	e.Cache.DeleteRange(walKeys, min, max)

	// delete from the WAL
	if _, err := e.WAL.DeleteRange(walKeys, min, max); err != nil {
		return err
	}

	// Have we deleted all points for the series? If so, we need to remove
	// the series from the index.
	existing, err := e.containsSeries(seriesKeys)
	if err != nil {
		return err
	}

	for k, exists := range existing {
		if !exists {
			if err := e.index.UnassignShard(k, e.id); err != nil {
				return err
			}
		}
	}

	return nil
}

// DeleteMeasurement deletes a measurement and all related series.
func (e *Engine) DeleteMeasurement(name []byte) error {
	// Delete the bulk of data outside of the fields lock.
	if err := e.deleteMeasurement(name); err != nil {
		return err
	}

	// Under lock, delete any series created deletion.
	if err := e.fieldset.DeleteWithLock(string(name), func() error {
		return e.deleteMeasurement(name)
	}); err != nil {
		return err
	}

	return nil
}

// DeleteMeasurement deletes a measurement and all related series.
func (e *Engine) deleteMeasurement(name []byte) error {
	// Attempt to find the series keys.
	keys, err := e.index.MeasurementSeriesKeysByExpr(name, nil)
	if err != nil {
		return err
	} else if len(keys) > 0 {
		if err := e.deleteSeries(keys); err != nil {
			return err
		}
	}
	return nil
}

// ForEachMeasurementName iterates over each measurement name in the engine.
func (e *Engine) ForEachMeasurementName(fn func(name []byte) error) error {
	return e.index.ForEachMeasurementName(fn)
}

// MeasurementSeriesKeysByExpr returns a list of series keys matching expr.
func (e *Engine) MeasurementSeriesKeysByExpr(name []byte, expr influxql.Expr) ([][]byte, error) {
	return e.index.MeasurementSeriesKeysByExpr(name, expr)
}

func (e *Engine) CreateSeriesListIfNotExists(keys, names [][]byte, tagsSlice []models.Tags) error {
	return e.index.CreateSeriesListIfNotExists(keys, names, tagsSlice)
}

func (e *Engine) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	return e.index.CreateSeriesIfNotExists(key, name, tags)
}

// WriteTo is not implemented.
func (e *Engine) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }

// WriteSnapshot will snapshot the cache and write a new TSM file with its contents, releasing the snapshot when done.
func (e *Engine) WriteSnapshot() error {
	// Lock and grab the cache snapshot along with all the closed WAL
	// filenames associated with the snapshot

	var started *time.Time

	defer func() {
		if started != nil {
			e.Cache.UpdateCompactTime(time.Since(*started))
			e.logger.Info(fmt.Sprintf("Snapshot for path %s written in %v", e.path, time.Since(*started)))
		}
	}()

	closedFiles, snapshot, err := func() ([]string, *Cache, error) {
		e.mu.Lock()
		defer e.mu.Unlock()

		now := time.Now()
		started = &now

		if err := e.WAL.CloseSegment(); err != nil {
			return nil, nil, err
		}

		segments, err := e.WAL.ClosedSegments()
		if err != nil {
			return nil, nil, err
		}

		snapshot, err := e.Cache.Snapshot()
		if err != nil {
			return nil, nil, err
		}

		return segments, snapshot, nil
	}()

	if err != nil {
		return err
	}

	if snapshot.Size() == 0 {
		e.Cache.ClearSnapshot(true)
		return nil
	}

	// The snapshotted cache may have duplicate points and unsorted data.  We need to deduplicate
	// it before writing the snapshot.  This can be very expensive so it's done while we are not
	// holding the engine write lock.
	dedup := time.Now()
	snapshot.Deduplicate()
	e.traceLogger.Info(fmt.Sprintf("Snapshot for path %s deduplicated in %v", e.path, time.Since(dedup)))

	return e.writeSnapshotAndCommit(closedFiles, snapshot)
}

// CreateSnapshot will create a temp directory that holds
// temporary hardlinks to the underylyng shard files.
func (e *Engine) CreateSnapshot() (string, error) {
	if err := e.WriteSnapshot(); err != nil {
		return "", err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.FileStore.CreateSnapshot()
}

// writeSnapshotAndCommit will write the passed cache to a new TSM file and remove the closed WAL segments.
func (e *Engine) writeSnapshotAndCommit(closedFiles []string, snapshot *Cache) (err error) {
	defer func() {
		if err != nil {
			e.Cache.ClearSnapshot(false)
		}
	}()

	// write the new snapshot files
	newFiles, err := e.Compactor.WriteSnapshot(snapshot)
	if err != nil {
		e.logger.Info(fmt.Sprintf("error writing snapshot from compactor: %v", err))
		return err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// update the file store with these new files
	if err := e.FileStore.Replace(nil, newFiles); err != nil {
		e.logger.Info(fmt.Sprintf("error adding new TSM files from snapshot: %v", err))
		return err
	}

	// clear the snapshot from the in-memory cache, then the old WAL files
	e.Cache.ClearSnapshot(true)

	if err := e.WAL.Remove(closedFiles); err != nil {
		e.logger.Info(fmt.Sprintf("error removing closed wal segments: %v", err))
	}

	return nil
}

// compactCache continually checks if the WAL cache should be written to disk.
func (e *Engine) compactCache(quit <-chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-quit:
			return

		case <-t.C:
			e.Cache.UpdateAge()
			if e.ShouldCompactCache(e.WAL.LastWriteTime()) {
				start := time.Now()
				e.traceLogger.Info(fmt.Sprintf("Compacting cache for %s", e.path))
				err := e.WriteSnapshot()
				if err != nil && err != errCompactionsDisabled {
					e.logger.Info(fmt.Sprintf("error writing snapshot: %v", err))
					atomic.AddInt64(&e.stats.CacheCompactionErrors, 1)
				} else {
					atomic.AddInt64(&e.stats.CacheCompactions, 1)
				}
				atomic.AddInt64(&e.stats.CacheCompactionDuration, time.Since(start).Nanoseconds())
			}
		}
	}
}

// ShouldCompactCache returns true if the Cache is over its flush threshold
// or if the passed in lastWriteTime is older than the write cold threshold.
func (e *Engine) ShouldCompactCache(lastWriteTime time.Time) bool {
	sz := e.Cache.Size()

	if sz == 0 {
		return false
	}

	return sz > e.CacheFlushMemorySizeThreshold ||
		time.Since(lastWriteTime) > e.CacheFlushWriteColdDuration
}

func (e *Engine) compactTSMLevel(fast bool, level int, quit <-chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-quit:
			return

		case <-t.C:
			s := e.levelCompactionStrategy(fast, level)
			if s != nil {
				s.Apply()
				// Release the files in the compaction plan
				e.CompactionPlan.Release(s.compactionGroups)
			}

		}
	}
}

func (e *Engine) compactTSMFull(quit <-chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-quit:
			return

		case <-t.C:
			s := e.fullCompactionStrategy()
			if s != nil {
				s.Apply()
				// Release the files in the compaction plan
				e.CompactionPlan.Release(s.compactionGroups)
			}
		}
	}
}

// onFileStoreReplace is callback handler invoked when the FileStore
// has replaced one set of TSM files with a new set.
func (e *Engine) onFileStoreReplace(newFiles []TSMFile) {
	// Load any new series keys to the index
	readers := make([]chan seriesKey, 0, len(newFiles))
	for _, r := range newFiles {
		ch := make(chan seriesKey, 1)
		readers = append(readers, ch)

		go func(c chan seriesKey, r TSMFile) {
			n := r.KeyCount()
			for i := 0; i < n; i++ {
				key, typ := r.KeyAt(i)
				c <- seriesKey{key, typ}
			}
			close(c)
		}(ch, r)
	}

	// Merge and dedup all the series keys across each reader to reduce
	// lock contention on the index.
	merged := merge(readers...)
	for v := range merged {
		fieldType, err := tsmFieldTypeToInfluxQLDataType(v.typ)
		if err != nil {
			e.logger.Error(fmt.Sprintf("refresh index (1): %v", err))
			continue
		}

		if err := e.addToIndexFromKey(v.key, fieldType); err != nil {
			e.logger.Error(fmt.Sprintf("refresh index (2): %v", err))
			continue
		}
	}

	// load metadata from the Cache
	e.Cache.ApplyEntryFn(func(key []byte, entry *entry) error {
		fieldType, err := entry.values.InfluxQLType()
		if err != nil {
			e.logger.Error(fmt.Sprintf("refresh index (3): %v", err))
			return nil
		}

		if err := e.addToIndexFromKey(key, fieldType); err != nil {
			e.logger.Error(fmt.Sprintf("refresh index (4): %v", err))
			return nil
		}
		return nil
	})
}

// compactionStrategy holds the details of what to do in a compaction.
type compactionStrategy struct {
	compactionGroups []CompactionGroup

	// concurrency determines how many compactions groups will be started
	// concurrently.  These groups may be limited by the global limiter if
	// enabled.
	concurrency int
	fast        bool
	description string

	durationStat *int64
	activeStat   *int64
	successStat  *int64
	errorStat    *int64

	logger    zap.Logger
	compactor *Compactor
	fileStore *FileStore
	limiter   limiter.Fixed
	engine    *Engine
}

// Apply concurrently compacts all the groups in a compaction strategy.
func (s *compactionStrategy) Apply() {
	start := time.Now()

	// cap concurrent compaction groups to no more than 4 at a time.
	concurrency := s.concurrency
	if concurrency == 0 {
		concurrency = 4
	}

	throttle := limiter.NewFixed(concurrency)
	var wg sync.WaitGroup
	for i := range s.compactionGroups {
		wg.Add(1)
		go func(groupNum int) {
			defer wg.Done()

			// limit concurrent compaction groups
			throttle.Take()
			defer throttle.Release()

			s.compactGroup(groupNum)
		}(i)
	}
	wg.Wait()

	atomic.AddInt64(s.durationStat, time.Since(start).Nanoseconds())
}

// compactGroup executes the compaction strategy against a single CompactionGroup.
func (s *compactionStrategy) compactGroup(groupNum int) {
	// Limit concurrent compactions if we have a limiter
	if cap(s.limiter) > 0 {
		s.limiter.Take()
		defer s.limiter.Release()
	}

	group := s.compactionGroups[groupNum]
	start := time.Now()
	s.logger.Info(fmt.Sprintf("beginning %s compaction of group %d, %d TSM files", s.description, groupNum, len(group)))
	for i, f := range group {
		s.logger.Info(fmt.Sprintf("compacting %s group (%d) %s (#%d)", s.description, groupNum, f, i))
	}

	files, err := func() ([]string, error) {
		// Count the compaction as active only while the compaction is actually running.
		atomic.AddInt64(s.activeStat, 1)
		defer atomic.AddInt64(s.activeStat, -1)

		if s.fast {
			return s.compactor.CompactFast(group)
		} else {
			return s.compactor.CompactFull(group)
		}
	}()

	if err != nil {
		_, inProgress := err.(errCompactionInProgress)
		if err == errCompactionsDisabled || inProgress {
			s.logger.Info(fmt.Sprintf("aborted %s compaction group (%d). %v", s.description, groupNum, err))

			if _, ok := err.(errCompactionInProgress); ok {
				time.Sleep(time.Second)
			}
			return
		}

		s.logger.Info(fmt.Sprintf("error compacting TSM files: %v", err))
		atomic.AddInt64(s.errorStat, 1)
		time.Sleep(time.Second)
		return
	}

	if err := s.fileStore.ReplaceWithCallback(group, files, s.engine.onFileStoreReplace); err != nil {
		s.logger.Info(fmt.Sprintf("error replacing new TSM files: %v", err))
		atomic.AddInt64(s.errorStat, 1)
		time.Sleep(time.Second)
		return
	}

	for i, f := range files {
		s.logger.Info(fmt.Sprintf("compacted %s group (%d) into %s (#%d)", s.description, groupNum, f, i))
	}
	s.logger.Info(fmt.Sprintf("compacted %s %d files into %d files in %s", s.description, len(group), len(files), time.Since(start)))
	atomic.AddInt64(s.successStat, 1)
}

// levelCompactionStrategy returns a compactionStrategy for the given level.
// It returns nil if there are no TSM files to compact.
func (e *Engine) levelCompactionStrategy(fast bool, level int) *compactionStrategy {
	compactionGroups := e.CompactionPlan.PlanLevel(level)

	if len(compactionGroups) == 0 {
		return nil
	}

	return &compactionStrategy{
		concurrency:      4,
		compactionGroups: compactionGroups,
		logger:           e.logger,
		fileStore:        e.FileStore,
		compactor:        e.Compactor,
		fast:             fast,
		limiter:          e.compactionLimiter,
		engine:           e,

		description:  fmt.Sprintf("level %d", level),
		activeStat:   &e.stats.TSMCompactionsActive[level-1],
		successStat:  &e.stats.TSMCompactions[level-1],
		errorStat:    &e.stats.TSMCompactionErrors[level-1],
		durationStat: &e.stats.TSMCompactionDuration[level-1],
	}
}

// fullCompactionStrategy returns a compactionStrategy for higher level generations of TSM files.
// It returns nil if there are no TSM files to compact.
func (e *Engine) fullCompactionStrategy() *compactionStrategy {
	optimize := false
	compactionGroups := e.CompactionPlan.Plan(e.WAL.LastWriteTime())

	if len(compactionGroups) == 0 {
		optimize = true
		compactionGroups = e.CompactionPlan.PlanOptimize()
	}

	if len(compactionGroups) == 0 {
		return nil
	}

	s := &compactionStrategy{
		concurrency:      1,
		compactionGroups: compactionGroups,
		logger:           e.logger,
		fileStore:        e.FileStore,
		compactor:        e.Compactor,
		fast:             optimize,
		limiter:          e.compactionLimiter,
		engine:           e,
	}

	if optimize {
		s.description = "optimize"
		s.activeStat = &e.stats.TSMOptimizeCompactionsActive
		s.successStat = &e.stats.TSMOptimizeCompactions
		s.errorStat = &e.stats.TSMOptimizeCompactionErrors
		s.durationStat = &e.stats.TSMOptimizeCompactionDuration
	} else {
		s.description = "full"
		s.activeStat = &e.stats.TSMFullCompactionsActive
		s.successStat = &e.stats.TSMFullCompactions
		s.errorStat = &e.stats.TSMFullCompactionErrors
		s.durationStat = &e.stats.TSMFullCompactionDuration
	}

	return s
}

// reloadCache reads the WAL segment files and loads them into the cache.
func (e *Engine) reloadCache() error {
	now := time.Now()
	files, err := segmentFileNames(e.WAL.Path())
	if err != nil {
		return err
	}

	limit := e.Cache.MaxSize()
	defer func() {
		e.Cache.SetMaxSize(limit)
	}()

	// Disable the max size during loading
	e.Cache.SetMaxSize(0)

	loader := NewCacheLoader(files)
	loader.WithLogger(e.logger)
	if err := loader.Load(e.Cache); err != nil {
		return err
	}

	e.traceLogger.Info(fmt.Sprintf("Reloaded WAL cache %s in %v", e.WAL.Path(), time.Since(now)))
	return nil
}

// cleanup removes all temp files and dirs that exist on disk.  This is should only be run at startup to avoid
// removing tmp files that are still in use.
func (e *Engine) cleanup() error {
	allfiles, err := ioutil.ReadDir(e.path)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	for _, f := range allfiles {
		// Check to see if there are any `.tmp` directories that were left over from failed shard snapshots
		if f.IsDir() && strings.HasSuffix(f.Name(), ".tmp") {
			if err := os.RemoveAll(filepath.Join(e.path, f.Name())); err != nil {
				return fmt.Errorf("error removing tmp snapshot directory %q: %s", f.Name(), err)
			}
		}
	}

	return e.cleanupTempTSMFiles()
}

func (e *Engine) cleanupTempTSMFiles() error {
	files, err := filepath.Glob(filepath.Join(e.path, fmt.Sprintf("*.%s", CompactionTempExtension)))
	if err != nil {
		return fmt.Errorf("error getting compaction temp files: %s", err.Error())
	}

	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return fmt.Errorf("error removing temp compaction files: %v", err)
		}
	}
	return nil
}

// KeyCursor returns a KeyCursor for the given key starting at time t.
func (e *Engine) KeyCursor(key []byte, t int64, ascending bool) *KeyCursor {
	return e.FileStore.KeyCursor(key, t, ascending)
}

// CreateIterator returns an iterator for the measurement based on opt.
func (e *Engine) CreateIterator(measurement string, opt query.IteratorOptions) (query.Iterator, error) {
	if call, ok := opt.Expr.(*influxql.Call); ok {
		if opt.Interval.IsZero() {
			if call.Name == "first" || call.Name == "last" {
				refOpt := opt
				refOpt.Limit = 1
				refOpt.Ascending = call.Name == "first"
				refOpt.Ordered = true
				refOpt.Expr = call.Args[0]

				itrs, err := e.createVarRefIterator(measurement, refOpt)
				if err != nil {
					return nil, err
				}
				return newMergeFinalizerIterator(itrs, opt, e.logger)
			}
		}

		inputs, err := e.createCallIterator(measurement, call, opt)
		if err != nil {
			return nil, err
		} else if len(inputs) == 0 {
			return nil, nil
		}
		return newMergeFinalizerIterator(inputs, opt, e.logger)
	}

	itrs, err := e.createVarRefIterator(measurement, opt)
	if err != nil {
		return nil, err
	}
	return newMergeFinalizerIterator(itrs, opt, e.logger)
}

func (e *Engine) createCallIterator(measurement string, call *influxql.Call, opt query.IteratorOptions) ([]query.Iterator, error) {
	ref, _ := call.Args[0].(*influxql.VarRef)

	if exists, err := e.index.MeasurementExists([]byte(measurement)); err != nil {
		return nil, err
	} else if !exists {
		return nil, nil
	}

	// Determine tagsets for this measurement based on dimensions and filters.
	tagSets, err := e.index.TagSets([]byte(measurement), opt)
	if err != nil {
		return nil, err
	}

	// Reverse the tag sets if we are ordering by descending.
	if !opt.Ascending {
		for _, t := range tagSets {
			t.Reverse()
		}
	}

	// Calculate tag sets and apply SLIMIT/SOFFSET.
	tagSets = query.LimitTagSets(tagSets, opt.SLimit, opt.SOffset)

	itrs := make([]query.Iterator, 0, len(tagSets))
	if err := func() error {
		for _, t := range tagSets {
			// Abort if the query was killed
			select {
			case <-opt.InterruptCh:
				query.Iterators(itrs).Close()
				return err
			default:
			}

			inputs, err := e.createTagSetIterators(ref, measurement, t, opt)
			if err != nil {
				return err
			} else if len(inputs) == 0 {
				continue
			}

			// Wrap each series in a call iterator.
			for i, input := range inputs {
				if opt.InterruptCh != nil {
					input = query.NewInterruptIterator(input, opt.InterruptCh)
				}

				itr, err := query.NewCallIterator(input, opt)
				if err != nil {
					query.Iterators(inputs).Close()
					return err
				}
				inputs[i] = itr
			}

			itr := query.NewParallelMergeIterator(inputs, opt, runtime.GOMAXPROCS(0))
			itrs = append(itrs, itr)
		}
		return nil
	}(); err != nil {
		query.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// createVarRefIterator creates an iterator for a variable reference.
func (e *Engine) createVarRefIterator(measurement string, opt query.IteratorOptions) ([]query.Iterator, error) {
	ref, _ := opt.Expr.(*influxql.VarRef)

	if exists, err := e.index.MeasurementExists([]byte(measurement)); err != nil {
		return nil, err
	} else if !exists {
		return nil, nil
	}

	// Determine tagsets for this measurement based on dimensions and filters.
	tagSets, err := e.index.TagSets([]byte(measurement), opt)
	if err != nil {
		return nil, err
	}

	// Reverse the tag sets if we are ordering by descending.
	if !opt.Ascending {
		for _, t := range tagSets {
			t.Reverse()
		}
	}

	// Calculate tag sets and apply SLIMIT/SOFFSET.
	tagSets = query.LimitTagSets(tagSets, opt.SLimit, opt.SOffset)

	itrs := make([]query.Iterator, 0, len(tagSets))
	if err := func() error {
		for _, t := range tagSets {
			inputs, err := e.createTagSetIterators(ref, measurement, t, opt)
			if err != nil {
				return err
			} else if len(inputs) == 0 {
				continue
			}

			// If we have a LIMIT or OFFSET and the grouping of the outer query
			// is different than the current grouping, we need to perform the
			// limit on each of the individual series keys instead to improve
			// performance.
			if (opt.Limit > 0 || opt.Offset > 0) && len(opt.Dimensions) != len(opt.GroupBy) {
				for i, input := range inputs {
					inputs[i] = newLimitIterator(input, opt)
				}
			}

			itr, err := query.Iterators(inputs).Merge(opt)
			if err != nil {
				query.Iterators(inputs).Close()
				return err
			}

			// Apply a limit on the merged iterator.
			if opt.Limit > 0 || opt.Offset > 0 {
				if len(opt.Dimensions) == len(opt.GroupBy) {
					// When the final dimensions and the current grouping are
					// the same, we will only produce one series so we can use
					// the faster limit iterator.
					itr = newLimitIterator(itr, opt)
				} else {
					// When the dimensions are different than the current
					// grouping, we need to account for the possibility there
					// will be multiple series. The limit iterator in the
					// influxql package handles that scenario.
					itr = query.NewLimitIterator(itr, opt)
				}
			}
			itrs = append(itrs, itr)
		}
		return nil
	}(); err != nil {
		query.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// createTagSetIterators creates a set of iterators for a tagset.
func (e *Engine) createTagSetIterators(ref *influxql.VarRef, name string, t *query.TagSet, opt query.IteratorOptions) ([]query.Iterator, error) {
	// Set parallelism by number of logical cpus.
	parallelism := runtime.GOMAXPROCS(0)
	if parallelism > len(t.SeriesKeys) {
		parallelism = len(t.SeriesKeys)
	}

	// Create series key groupings w/ return error.
	groups := make([]struct {
		keys    []string
		filters []influxql.Expr
		itrs    []query.Iterator
		err     error
	}, parallelism)

	// Group series keys.
	n := len(t.SeriesKeys) / parallelism
	for i := 0; i < parallelism; i++ {
		group := &groups[i]

		if i < parallelism-1 {
			group.keys = t.SeriesKeys[i*n : (i+1)*n]
			group.filters = t.Filters[i*n : (i+1)*n]
		} else {
			group.keys = t.SeriesKeys[i*n:]
			group.filters = t.Filters[i*n:]
		}
	}

	// Read series groups in parallel.
	var wg sync.WaitGroup
	for i := range groups {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			groups[i].itrs, groups[i].err = e.createTagSetGroupIterators(ref, name, groups[i].keys, t, groups[i].filters, opt)
		}(i)
	}
	wg.Wait()

	// Determine total number of iterators so we can allocate only once.
	var itrN int
	for _, group := range groups {
		itrN += len(group.itrs)
	}

	// Combine all iterators together and check for errors.
	var err error
	itrs := make([]query.Iterator, 0, itrN)
	for _, group := range groups {
		if group.err != nil {
			err = group.err
		}
		itrs = append(itrs, group.itrs...)
	}

	// If an error occurred, make sure we close all created iterators.
	if err != nil {
		query.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// createTagSetGroupIterators creates a set of iterators for a subset of a tagset's series.
func (e *Engine) createTagSetGroupIterators(ref *influxql.VarRef, name string, seriesKeys []string, t *query.TagSet, filters []influxql.Expr, opt query.IteratorOptions) ([]query.Iterator, error) {
	conditionFields := make([]influxql.VarRef, len(influxql.ExprNames(opt.Condition)))

	itrs := make([]query.Iterator, 0, len(seriesKeys))
	for i, seriesKey := range seriesKeys {
		fields := 0
		if filters[i] != nil {
			// Retrieve non-time fields from this series filter and filter out tags.
			for _, f := range influxql.ExprNames(filters[i]) {
				conditionFields[fields] = f
				fields++
			}
		}

		itr, err := e.createVarRefSeriesIterator(ref, name, seriesKey, t, filters[i], conditionFields[:fields], opt)
		if err != nil {
			return itrs, err
		} else if itr == nil {
			continue
		}
		itrs = append(itrs, itr)

		// Abort if the query was killed
		select {
		case <-opt.InterruptCh:
			query.Iterators(itrs).Close()
			return nil, err
		default:
		}

		// Enforce series limit at creation time.
		if opt.MaxSeriesN > 0 && len(itrs) > opt.MaxSeriesN {
			query.Iterators(itrs).Close()
			return nil, fmt.Errorf("max-select-series limit exceeded: (%d/%d)", len(itrs), opt.MaxSeriesN)
		}

	}
	return itrs, nil
}

// createVarRefSeriesIterator creates an iterator for a variable reference for a series.
func (e *Engine) createVarRefSeriesIterator(ref *influxql.VarRef, name string, seriesKey string, t *query.TagSet, filter influxql.Expr, conditionFields []influxql.VarRef, opt query.IteratorOptions) (query.Iterator, error) {
	_, tfs := models.ParseKey([]byte(seriesKey))
	tags := query.NewTags(tfs.Map())

	// Create options specific for this series.
	itrOpt := opt
	itrOpt.Condition = filter

	// Build auxilary cursors.
	// Tag values should be returned if the field doesn't exist.
	var aux []cursorAt
	if len(opt.Aux) > 0 {
		aux = make([]cursorAt, len(opt.Aux))
		for i, ref := range opt.Aux {
			// Create cursor from field if a tag wasn't requested.
			if ref.Type != influxql.Tag {
				cur := e.buildCursor(name, seriesKey, tfs, &ref, opt)
				if cur != nil {
					aux[i] = newBufCursor(cur, opt.Ascending)
					continue
				}

				// If a field was requested, use a nil cursor of the requested type.
				switch ref.Type {
				case influxql.Float, influxql.AnyField:
					aux[i] = nilFloatLiteralValueCursor
					continue
				case influxql.Integer:
					aux[i] = nilIntegerLiteralValueCursor
					continue
				case influxql.Unsigned:
					aux[i] = nilUnsignedLiteralValueCursor
					continue
				case influxql.String:
					aux[i] = nilStringLiteralValueCursor
					continue
				case influxql.Boolean:
					aux[i] = nilBooleanLiteralValueCursor
					continue
				}
			}

			// If field doesn't exist, use the tag value.
			if v := tags.Value(ref.Val); v == "" {
				// However, if the tag value is blank then return a null.
				aux[i] = nilStringLiteralValueCursor
			} else {
				aux[i] = &literalValueCursor{value: v}
			}
		}
	}

	// Remove _tagKey condition field.
	// We can't seach on it because we can't join it to _tagValue based on time.
	if varRefSliceContains(conditionFields, "_tagKey") {
		conditionFields = varRefSliceRemove(conditionFields, "_tagKey")

		// Remove _tagKey conditional references from iterator.
		itrOpt.Condition = influxql.RewriteExpr(influxql.CloneExpr(itrOpt.Condition), func(expr influxql.Expr) influxql.Expr {
			switch expr := expr.(type) {
			case *influxql.BinaryExpr:
				if ref, ok := expr.LHS.(*influxql.VarRef); ok && ref.Val == "_tagKey" {
					return &influxql.BooleanLiteral{Val: true}
				}
				if ref, ok := expr.RHS.(*influxql.VarRef); ok && ref.Val == "_tagKey" {
					return &influxql.BooleanLiteral{Val: true}
				}
			}
			return expr
		})
	}

	// Build conditional field cursors.
	// If a conditional field doesn't exist then ignore the series.
	var conds []cursorAt
	if len(conditionFields) > 0 {
		conds = make([]cursorAt, len(conditionFields))
		for i, ref := range conditionFields {
			// Create cursor from field if a tag wasn't requested.
			if ref.Type != influxql.Tag {
				cur := e.buildCursor(name, seriesKey, tfs, &ref, opt)
				if cur != nil {
					conds[i] = newBufCursor(cur, opt.Ascending)
					continue
				}

				// If a field was requested, use a nil cursor of the requested type.
				switch ref.Type {
				case influxql.Float, influxql.AnyField:
					conds[i] = nilFloatLiteralValueCursor
					continue
				case influxql.Integer:
					conds[i] = nilIntegerLiteralValueCursor
					continue
				case influxql.Unsigned:
					conds[i] = nilUnsignedLiteralValueCursor
					continue
				case influxql.String:
					conds[i] = nilStringLiteralValueCursor
					continue
				case influxql.Boolean:
					conds[i] = nilBooleanLiteralValueCursor
					continue
				}
			}

			// If field doesn't exist, use the tag value.
			if v := tags.Value(ref.Val); v == "" {
				// However, if the tag value is blank then return a null.
				conds[i] = nilStringLiteralValueCursor
			} else {
				conds[i] = &literalValueCursor{value: v}
			}
		}
	}
	condNames := influxql.VarRefs(conditionFields).Strings()

	// Limit tags to only the dimensions selected.
	dimensions := opt.GetDimensions()
	tags = tags.Subset(dimensions)

	// If it's only auxiliary fields then it doesn't matter what type of iterator we use.
	if ref == nil {
		if opt.StripName {
			name = ""
		}
		return newFloatIterator(name, tags, itrOpt, nil, aux, conds, condNames), nil
	}

	// Build main cursor.
	cur := e.buildCursor(name, seriesKey, tfs, ref, opt)

	// If the field doesn't exist then don't build an iterator.
	if cur == nil {
		cursorsAt(aux).close()
		cursorsAt(conds).close()
		return nil, nil
	}

	// Remove name if requested.
	if opt.StripName {
		name = ""
	}

	switch cur := cur.(type) {
	case floatCursor:
		return newFloatIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	case integerCursor:
		return newIntegerIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	case unsignedCursor:
		return newUnsignedIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	case stringCursor:
		return newStringIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	case booleanCursor:
		return newBooleanIterator(name, tags, itrOpt, cur, aux, conds, condNames), nil
	default:
		panic("unreachable")
	}
}

// buildCursor creates an untyped cursor for a field.
func (e *Engine) buildCursor(measurement, seriesKey string, tags models.Tags, ref *influxql.VarRef, opt query.IteratorOptions) cursor {
	// Check if this is a system field cursor.
	switch ref.Val {
	case "_name":
		return &stringSliceCursor{values: []string{measurement}}
	case "_tagKey":
		return &stringSliceCursor{values: tags.Keys()}
	case "_tagValue":
		return &stringSliceCursor{values: matchTagValues(tags, opt.Condition)}
	case "_seriesKey":
		return &stringSliceCursor{values: []string{seriesKey}}
	}

	// Look up fields for measurement.
	mf := e.fieldset.Fields(measurement)
	if mf == nil {
		return nil
	}

	// Check for system field for field keys.
	if ref.Val == "_fieldKey" {
		return &stringSliceCursor{values: mf.FieldKeys()}
	}

	// Find individual field.
	f := mf.Field(ref.Val)
	if f == nil {
		return nil
	}

	// Check if we need to perform a cast. Performing a cast in the
	// engine (if it is possible) is much more efficient than an automatic cast.
	if ref.Type != influxql.Unknown && ref.Type != influxql.AnyField && ref.Type != f.Type {
		switch ref.Type {
		case influxql.Float:
			switch f.Type {
			case influxql.Integer:
				cur := e.buildIntegerCursor(measurement, seriesKey, ref.Val, opt)
				return &floatCastIntegerCursor{cursor: cur}
			case influxql.Unsigned:
				cur := e.buildUnsignedCursor(measurement, seriesKey, ref.Val, opt)
				return &floatCastUnsignedCursor{cursor: cur}
			}
		case influxql.Integer:
			switch f.Type {
			case influxql.Float:
				cur := e.buildFloatCursor(measurement, seriesKey, ref.Val, opt)
				return &integerCastFloatCursor{cursor: cur}
			case influxql.Unsigned:
				cur := e.buildUnsignedCursor(measurement, seriesKey, ref.Val, opt)
				return &integerCastUnsignedCursor{cursor: cur}
			}
		case influxql.Unsigned:
			switch f.Type {
			case influxql.Float:
				cur := e.buildFloatCursor(measurement, seriesKey, ref.Val, opt)
				return &unsignedCastFloatCursor{cursor: cur}
			case influxql.Integer:
				cur := e.buildIntegerCursor(measurement, seriesKey, ref.Val, opt)
				return &unsignedCastIntegerCursor{cursor: cur}
			}
		}
		return nil
	}

	// Return appropriate cursor based on type.
	switch f.Type {
	case influxql.Float:
		return e.buildFloatCursor(measurement, seriesKey, ref.Val, opt)
	case influxql.Integer:
		return e.buildIntegerCursor(measurement, seriesKey, ref.Val, opt)
	case influxql.Unsigned:
		return e.buildUnsignedCursor(measurement, seriesKey, ref.Val, opt)
	case influxql.String:
		return e.buildStringCursor(measurement, seriesKey, ref.Val, opt)
	case influxql.Boolean:
		return e.buildBooleanCursor(measurement, seriesKey, ref.Val, opt)
	default:
		panic("unreachable")
	}
}

func matchTagValues(tags models.Tags, condition influxql.Expr) []string {
	if condition == nil {
		return tags.Values()
	}

	// Populate map with tag values.
	data := map[string]interface{}{}
	for _, tag := range tags {
		data[string(tag.Key)] = string(tag.Value)
	}

	// Match against each specific tag.
	var values []string
	for _, tag := range tags {
		data["_tagKey"] = string(tag.Key)
		if influxql.EvalBool(condition, data) {
			values = append(values, string(tag.Value))
		}
	}
	return values
}

// buildFloatCursor creates a cursor for a float field.
func (e *Engine) buildFloatCursor(measurement, seriesKey, field string, opt query.IteratorOptions) floatCursor {
	key := SeriesFieldKeyBytes(seriesKey, field)
	cacheValues := e.Cache.Values(key)
	keyCursor := e.KeyCursor(key, opt.SeekTime(), opt.Ascending)
	return newFloatCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

// buildIntegerCursor creates a cursor for an integer field.
func (e *Engine) buildIntegerCursor(measurement, seriesKey, field string, opt query.IteratorOptions) integerCursor {
	key := SeriesFieldKeyBytes(seriesKey, field)
	cacheValues := e.Cache.Values(key)
	keyCursor := e.KeyCursor(key, opt.SeekTime(), opt.Ascending)
	return newIntegerCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

// buildUnsignedCursor creates a cursor for an unsigned field.
func (e *Engine) buildUnsignedCursor(measurement, seriesKey, field string, opt query.IteratorOptions) unsignedCursor {
	key := SeriesFieldKeyBytes(seriesKey, field)
	cacheValues := e.Cache.Values(key)
	keyCursor := e.KeyCursor(key, opt.SeekTime(), opt.Ascending)
	return newUnsignedCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

// buildStringCursor creates a cursor for a string field.
func (e *Engine) buildStringCursor(measurement, seriesKey, field string, opt query.IteratorOptions) stringCursor {
	key := SeriesFieldKeyBytes(seriesKey, field)
	cacheValues := e.Cache.Values(key)
	keyCursor := e.KeyCursor(key, opt.SeekTime(), opt.Ascending)
	return newStringCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

// buildBooleanCursor creates a cursor for a boolean field.
func (e *Engine) buildBooleanCursor(measurement, seriesKey, field string, opt query.IteratorOptions) booleanCursor {
	key := SeriesFieldKeyBytes(seriesKey, field)
	cacheValues := e.Cache.Values(key)
	keyCursor := e.KeyCursor(key, opt.SeekTime(), opt.Ascending)
	return newBooleanCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

func (e *Engine) SeriesPointIterator(opt query.IteratorOptions) (query.Iterator, error) {
	return e.index.SeriesPointIterator(opt)
}

// SeriesFieldKey combine a series key and field name for a unique string to be hashed to a numeric ID.
func SeriesFieldKey(seriesKey, field string) string {
	return seriesKey + keyFieldSeparator + field
}

func SeriesFieldKeyBytes(seriesKey, field string) []byte {
	b := make([]byte, len(seriesKey)+len(keyFieldSeparator)+len(field))
	i := copy(b[:], seriesKey)
	i += copy(b[i:], keyFieldSeparatorBytes)
	copy(b[i:], field)
	return b
}

func tsmFieldTypeToInfluxQLDataType(typ byte) (influxql.DataType, error) {
	switch typ {
	case BlockFloat64:
		return influxql.Float, nil
	case BlockInteger:
		return influxql.Integer, nil
	case BlockUnsigned:
		return influxql.Unsigned, nil
	case BlockBoolean:
		return influxql.Boolean, nil
	case BlockString:
		return influxql.String, nil
	default:
		return influxql.Unknown, fmt.Errorf("unknown block type: %v", typ)
	}
}

// SeriesAndFieldFromCompositeKey returns the series key and the field key extracted from the composite key.
func SeriesAndFieldFromCompositeKey(key []byte) ([]byte, []byte) {
	sep := bytes.Index(key, keyFieldSeparatorBytes)
	if sep == -1 {
		// No field???
		return key, nil
	}
	return key[:sep], key[sep+len(keyFieldSeparator):]
}

// readDir recursively reads all files from a path.
func readDir(root, rel string) ([]string, error) {
	// Open root.
	f, err := os.Open(filepath.Join(root, rel))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read all files.
	fis, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}

	// Read all subdirectories and append to the end.
	var paths []string
	for _, fi := range fis {
		// Simply append if it's a file.
		if !fi.IsDir() {
			paths = append(paths, filepath.Join(rel, fi.Name()))
			continue
		}

		// Read and append nested file paths.
		children, err := readDir(root, filepath.Join(rel, fi.Name()))
		if err != nil {
			return nil, err
		}
		paths = append(paths, children...)
	}
	return paths, nil
}

func varRefSliceContains(a []influxql.VarRef, v string) bool {
	for _, ref := range a {
		if ref.Val == v {
			return true
		}
	}
	return false
}

func varRefSliceRemove(a []influxql.VarRef, v string) []influxql.VarRef {
	if !varRefSliceContains(a, v) {
		return a
	}

	other := make([]influxql.VarRef, 0, len(a))
	for _, ref := range a {
		if ref.Val != v {
			other = append(other, ref)
		}
	}
	return other
}
