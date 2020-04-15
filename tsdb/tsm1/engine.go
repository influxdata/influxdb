// Package tsm1 provides a TSDB in the Time Structured Merge tree format.
package tsm1

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/lifecycle"
	"github.com/influxdata/influxdb/v2/pkg/limiter"
	"github.com/influxdata/influxdb/v2/pkg/metrics"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/influxdata/influxdb/v2/tsdb/seriesfile"
	"github.com/influxdata/influxdb/v2/tsdb/tsi1"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@array_cursor.gen.go.tmpldata array_cursor.gen.go.tmpl array_cursor_iterator.gen.go.tmpl
//go:generate env GO111MODULE=on go run github.com/influxdata/influxdb/v2/tools/tmpl -i -data=file_store.gen.go.tmpldata file_store.gen.go.tmpl=file_store.gen.go
//go:generate env GO111MODULE=on go run github.com/influxdata/influxdb/v2/tools/tmpl -i -d isArray=y -data=file_store.gen.go.tmpldata file_store.gen.go.tmpl=file_store_array.gen.go
//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@encoding.gen.go.tmpldata encoding.gen.go.tmpl
//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@compact.gen.go.tmpldata compact.gen.go.tmpl
//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@reader.gen.go.tmpldata reader.gen.go.tmpl
//go:generate stringer -type=CacheStatus

var (
	// Static objects to prevent small allocs.
	KeyFieldSeparatorBytes = []byte(keyFieldSeparator)
)

var (
	tsmGroup                  = metrics.MustRegisterGroup("platform-tsm1")
	numberOfRefCursorsCounter = metrics.MustRegisterCounter("cursors_ref", metrics.WithGroup(tsmGroup))
)

// NewContextWithMetricsGroup creates a new context with a tsm1 metrics.Group for tracking
// various metrics when accessing TSM data.
func NewContextWithMetricsGroup(ctx context.Context) context.Context {
	group := metrics.NewGroup(tsmGroup)
	return metrics.NewContextWithGroup(ctx, group)
}

// MetricsGroupFromContext returns the tsm1 metrics.Group associated with the context
// or nil if no group has been assigned.
func MetricsGroupFromContext(ctx context.Context) *metrics.Group {
	return metrics.GroupFromContext(ctx)
}

const (
	// keyFieldSeparator separates the series key from the field name in the composite key
	// that identifies a specific field in series
	keyFieldSeparator = "#!~#"

	// MaxPointsPerBlock is the maximum number of points in an encoded block in a TSM file
	MaxPointsPerBlock = 1000
)

// An EngineOption is a functional option for changing the configuration of
// an Engine.
type EngineOption func(i *Engine)

// WithCompactionPlanner sets the compaction planner for the engine.
func WithCompactionPlanner(planner CompactionPlanner) EngineOption {
	return func(e *Engine) {
		planner.SetFileStore(e.FileStore)
		e.CompactionPlan = planner
	}
}

// Snapshotter allows upward signaling of the tsm1 engine to the storage engine. Hopefully
// it can be removed one day. The weird interface is due to the weird inversion of locking
// that has to happen.
type Snapshotter interface {
	AcquireSegments(context.Context, func(segments []string) error) error
	CommitSegments(ctx context.Context, segments []string, fn func() error) error
}

type noSnapshotter struct{}

func (noSnapshotter) AcquireSegments(_ context.Context, fn func([]string) error) error {
	return fn(nil)
}
func (noSnapshotter) CommitSegments(_ context.Context, _ []string, fn func() error) error {
	return fn()
}

// WithSnapshotter sets the callbacks for the engine to use when creating snapshots.
func WithSnapshotter(snapshotter Snapshotter) EngineOption {
	return func(e *Engine) {
		e.snapshotter = snapshotter
	}
}

// Engine represents a storage engine with compressed blocks.
type Engine struct {
	mu sync.RWMutex

	index    *tsi1.Index
	indexref *lifecycle.Reference

	// The following group of fields is used to track the state of level compactions within the
	// Engine. The WaitGroup is used to monitor the compaction goroutines, the 'done' channel is
	// used to signal those goroutines to shutdown. Every request to disable level compactions will
	// call 'Wait' on 'wg', with the first goroutine to arrive (levelWorkers == 0 while holding the
	// lock) will close the done channel and re-assign 'nil' to the variable. Re-enabling will
	// decrease 'levelWorkers', and when it decreases to zero, level compactions will be started
	// back up again.

	wg           *sync.WaitGroup // waitgroup for active level compaction goroutines
	done         chan struct{}   // channel to signal level compactions to stop
	levelWorkers int             // Number of "workers" that expect compactions to be in a disabled state

	snapDone chan struct{}   // channel to signal snapshot compactions to stop
	snapWG   *sync.WaitGroup // waitgroup for running snapshot compactions

	path     string
	sfile    *seriesfile.SeriesFile
	sfileref *lifecycle.Reference
	logger   *zap.Logger // Logger to be used for important messages

	Cache          *Cache
	Compactor      *Compactor
	CompactionPlan CompactionPlanner
	FileStore      *FileStore

	MaxPointsPerBlock int

	// CacheFlushMemorySizeThreshold specifies the minimum size threshold for
	// the cache when the engine should write a snapshot to a TSM file
	CacheFlushMemorySizeThreshold uint64

	// CacheFlushAgeDurationThreshold specified the maximum age a cache can reach
	// before it is snapshotted, regardless of its size.
	CacheFlushAgeDurationThreshold time.Duration

	// CacheFlushWriteColdDuration specifies the length of time after which if
	// no writes have been committed to the WAL, the engine will write
	// a snapshot of the cache to a TSM file
	CacheFlushWriteColdDuration time.Duration

	// Invoked when creating a backup file "as new".
	formatFileName FormatFileNameFunc

	// Controls whether to enabled compactions when the engine is open
	enableCompactionsOnOpen bool

	compactionTracker   *compactionTracker // Used to track state of compactions.
	readTracker         *readTracker       // Used to track number of reads.
	defaultMetricLabels prometheus.Labels  // N.B this must not be mutated after Open is called.

	// Limiter for concurrent compactions.
	compactionLimiter limiter.Fixed
	// A semaphore for limiting full compactions across multiple engines.
	fullCompactionSemaphore influxdb.Semaphore
	// Tracks how long the last full compaction took. Should be accessed atomically.
	lastFullCompactionDuration int64

	scheduler   *scheduler
	snapshotter Snapshotter
}

// NewEngine returns a new instance of Engine.
func NewEngine(path string, idx *tsi1.Index, config Config, options ...EngineOption) *Engine {
	fs := NewFileStore(path)
	fs.openLimiter = limiter.NewFixed(config.MaxConcurrentOpens)
	fs.tsmMMAPWillNeed = config.MADVWillNeed

	cache := NewCache(uint64(config.Cache.MaxMemorySize))

	c := NewCompactor()
	c.Dir = path
	c.FileStore = fs
	c.RateLimit = limiter.NewRate(
		int(config.Compaction.Throughput),
		int(config.Compaction.ThroughputBurst))

	// determine max concurrent compactions informed by the system
	maxCompactions := config.Compaction.MaxConcurrent
	if maxCompactions == 0 {
		maxCompactions = runtime.GOMAXPROCS(0) / 2 // Default to 50% of cores for compactions

		// On systems with more cores, cap at 4 to reduce disk utilization.
		if maxCompactions > 4 {
			maxCompactions = 4
		}

		if maxCompactions < 1 {
			maxCompactions = 1
		}
	}

	// Don't allow more compactions to run than cores.
	if maxCompactions > runtime.GOMAXPROCS(0) {
		maxCompactions = runtime.GOMAXPROCS(0)
	}

	logger := zap.NewNop()
	e := &Engine{
		path:   path,
		index:  idx,
		sfile:  idx.SeriesFile(),
		logger: logger,

		Cache: cache,

		FileStore: fs,
		Compactor: c,
		CompactionPlan: NewDefaultPlanner(fs,
			time.Duration(config.Compaction.FullWriteColdDuration)),

		CacheFlushMemorySizeThreshold:  uint64(config.Cache.SnapshotMemorySize),
		CacheFlushWriteColdDuration:    time.Duration(config.Cache.SnapshotWriteColdDuration),
		CacheFlushAgeDurationThreshold: time.Duration(config.Cache.SnapshotAgeDuration),
		enableCompactionsOnOpen:        true,
		formatFileName:                 DefaultFormatFileName,
		compactionLimiter:              limiter.NewFixed(maxCompactions),
		fullCompactionSemaphore:        influxdb.NopSemaphore,
		scheduler:                      newScheduler(maxCompactions),
		snapshotter:                    new(noSnapshotter),
	}

	for _, option := range options {
		option(e)
	}

	return e
}

// SetSemaphore sets the semaphore used to coordinate full compactions across
// multiple engines.
func (e *Engine) SetSemaphore(s influxdb.Semaphore) {
	e.fullCompactionSemaphore = s
}

// WithCompactionLimiter sets the compaction limiter, which is used to limit the
// number of concurrent compactions.
func (e *Engine) WithCompactionLimiter(limiter limiter.Fixed) {
	e.compactionLimiter = limiter
}

func (e *Engine) WithFormatFileNameFunc(formatFileNameFunc FormatFileNameFunc) {
	e.Compactor.WithFormatFileNameFunc(formatFileNameFunc)
	e.formatFileName = formatFileNameFunc
}

func (e *Engine) WithParseFileNameFunc(parseFileNameFunc ParseFileNameFunc) {
	e.FileStore.WithParseFileNameFunc(parseFileNameFunc)
	e.Compactor.WithParseFileNameFunc(parseFileNameFunc)
}

func (e *Engine) WithCurrentGenerationFunc(fn func() int) {
	e.Compactor.FileStore.SetCurrentGenerationFunc(fn)
}

func (e *Engine) WithFileStoreObserver(obs FileStoreObserver) {
	e.FileStore.WithObserver(obs)
}

func (e *Engine) WithCompactionPlanner(planner CompactionPlanner) {
	planner.SetFileStore(e.FileStore)
	e.CompactionPlan = planner
}

// SetDefaultMetricLabels sets the default labels for metrics on the engine.
// It must be called before the Engine is opened.
func (e *Engine) SetDefaultMetricLabels(labels prometheus.Labels) {
	e.defaultMetricLabels = labels
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
	e.done = make(chan struct{})
	wg := new(sync.WaitGroup)
	wg.Add(1)
	e.wg = wg
	e.mu.Unlock()

	go func() { defer wg.Done(); e.compact(wg) }()
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

	// Hold onto the current done channel so we can wait on it if necessary
	waitCh := e.done
	wg := e.wg

	if old == 0 && e.done != nil {
		// It's possible we have closed the done channel and released the lock and another
		// goroutine has attempted to disable compactions.  We're current in the process of
		// disabling them so check for this and wait until the original completes.
		select {
		case <-e.done:
			e.mu.Unlock()
			return
		default:
		}

		// Prevent new compactions from starting
		e.Compactor.DisableCompactions()

		// Stop all background compaction goroutines
		close(e.done)
		e.mu.Unlock()
		wg.Wait()

		// Signal that all goroutines have exited.
		e.mu.Lock()
		e.done = nil
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	// Compaction were already disabled.
	if waitCh == nil {
		return
	}

	// We were not the first caller to disable compactions and they were in the process
	// of being disabled.  Wait for them to complete before returning.
	<-waitCh
	wg.Wait()
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
	e.snapDone = make(chan struct{})
	wg := new(sync.WaitGroup)
	wg.Add(1)
	e.snapWG = wg
	e.mu.Unlock()

	go func() { defer wg.Done(); e.compactCache() }()
}

func (e *Engine) disableSnapshotCompactions() {
	e.mu.Lock()
	if e.snapDone == nil {
		e.mu.Unlock()
		return
	}

	// We may be in the process of stopping snapshots.  See if the channel
	// was closed.
	select {
	case <-e.snapDone:
		e.mu.Unlock()
		return
	default:
	}

	// first one here, disable and wait for completion
	close(e.snapDone)
	e.Compactor.DisableSnapshots()
	wg := e.snapWG
	e.mu.Unlock()

	// Wait for the snapshot goroutine to exit.
	wg.Wait()

	// Signal that the goroutines are exit and everything is stopped by setting
	// snapDone to nil.
	e.mu.Lock()
	e.snapDone = nil
	e.mu.Unlock()
}

// ScheduleFullCompaction will force the engine to fully compact all data stored.
// This will cancel and running compactions and snapshot any data in the cache to
// TSM files.  This is an expensive operation.
func (e *Engine) ScheduleFullCompaction(ctx context.Context) error {
	// Snapshot any data in the cache
	if err := e.WriteSnapshot(ctx, CacheStatusFullCompaction); err != nil {
		return err
	}

	// Cancel running compactions
	e.SetCompactionsEnabled(false)

	// Ensure compactions are restarted
	defer e.SetCompactionsEnabled(true)

	// Force the planner to only create a full plan.
	e.CompactionPlan.ForceFull()
	return nil
}

// Path returns the path the engine was opened with.
func (e *Engine) Path() string { return e.path }

func (e *Engine) SetFieldName(measurement []byte, name string) {
	e.index.SetFieldName(measurement, name)
}

func (e *Engine) MeasurementExists(name []byte) (bool, error) {
	return e.index.MeasurementExists(name)
}

func (e *Engine) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	return e.index.MeasurementNamesByRegex(re)
}

func (e *Engine) HasTagKey(name, key []byte) (bool, error) {
	return e.index.HasTagKey(name, key)
}

func (e *Engine) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	return e.index.MeasurementTagKeysByExpr(name, expr)
}

func (e *Engine) TagKeyCardinality(name, key []byte) int {
	return e.index.TagKeyCardinality(name, key)
}

// SeriesN returns the unique number of series in the index.
func (e *Engine) SeriesN() int64 {
	return e.index.SeriesN()
}

// MeasurementStats returns the current measurement stats for the engine.
func (e *Engine) MeasurementStats() (MeasurementStats, error) {
	return e.FileStore.MeasurementStats()
}

func (e *Engine) initTrackers() {
	mmu.Lock()
	defer mmu.Unlock()

	if bms == nil {
		// Initialise metrics if an engine has not done so already.
		bms = newBlockMetrics(e.defaultMetricLabels)
	}

	// Propagate prometheus metrics down into trackers.
	e.compactionTracker = newCompactionTracker(bms.compactionMetrics, e.defaultMetricLabels)
	e.FileStore.tracker = newFileTracker(bms.fileMetrics, e.defaultMetricLabels)
	e.Cache.tracker = newCacheTracker(bms.cacheMetrics, e.defaultMetricLabels)
	e.readTracker = newReadTracker(bms.readMetrics, e.defaultMetricLabels)

	e.scheduler.setCompactionTracker(e.compactionTracker)
}

// Open opens and initializes the engine.
func (e *Engine) Open(ctx context.Context) (err error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	defer func() {
		if err != nil {
			e.Close()
		}
	}()

	e.indexref, err = e.index.Acquire()
	if err != nil {
		return err
	}

	e.sfileref, err = e.sfile.Acquire()
	if err != nil {
		return err
	}

	e.initTrackers()

	if err := os.MkdirAll(e.path, 0777); err != nil {
		return err
	}

	if err := e.cleanup(); err != nil {
		return err
	}

	if err := e.FileStore.Open(ctx); err != nil {
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

	// Ensures that the channel will not be closed again.
	e.done = nil

	if err := e.FileStore.Close(); err != nil {
		return err
	}

	// Release our references.
	if e.sfileref != nil {
		e.sfileref.Release()
		e.sfileref = nil
	}

	if e.indexref != nil {
		e.indexref.Release()
		e.indexref = nil
	}

	return nil
}

// WithLogger sets the logger for the engine.
func (e *Engine) WithLogger(log *zap.Logger) {
	e.logger = log.With(zap.String("engine", "tsm1"))

	e.FileStore.WithLogger(e.logger)
}

// IsIdle returns true if the cache is empty, there are no running compactions and the
// shard is fully compacted.
func (e *Engine) IsIdle() bool {
	cacheEmpty := e.Cache.Size() == 0
	return cacheEmpty && e.compactionTracker.AllActive() == 0 && e.CompactionPlan.FullyCompacted()
}

// WritePoints saves the set of points in the engine.
func (e *Engine) WritePoints(points []models.Point) error {
	collection := tsdb.NewSeriesCollection(points)

	values, err := CollectionToValues(collection)
	if err != nil {
		return err
	}

	if err := e.WriteValues(values); err != nil {
		return err
	}

	return collection.PartialWriteError()
}

// WriteValues saves the set of values in the engine.
func (e *Engine) WriteValues(values map[string][]Value) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if err := e.Cache.WriteMulti(values); err != nil {
		return err
	}

	return nil
}

// ForEachMeasurementName iterates over each measurement name in the engine.
func (e *Engine) ForEachMeasurementName(fn func(name []byte) error) error {
	return e.index.ForEachMeasurementName(fn)
}

// compactionLevel describes a snapshot or levelled compaction.
type compactionLevel int

func (l compactionLevel) String() string {
	switch l {
	case 0:
		return "snapshot"
	case 1, 2, 3:
		return fmt.Sprint(int(l))
	case 4:
		return "optimize"
	case 5:
		return "full"
	default:
		panic("unsupported compaction level")
	}
}

// compactionTracker tracks compactions and snapshots within the Engine.
//
// As well as being responsible for providing atomic reads and writes to the
// statistics tracking the various compaction operations, compactionTracker also
// mirrors any writes to the prometheus block metrics, which the Engine exposes.
//
// *NOTE* - compactionTracker fields should not be directory modified. Doing so
// could result in the Engine exposing inaccurate metrics.
type compactionTracker struct {
	metrics *compactionMetrics
	labels  prometheus.Labels
	// Note: Compactions are levelled as follows:
	// 0   	– Snapshots
	// 1-3 	– Levelled compactions
	// 4 	– Optimize compactions
	// 5	– Full compactions

	ok     [6]uint64 // Counter of TSM compactions (by level) that have successfully completed.
	active [6]uint64 // Gauge of TSM compactions (by level) currently running.
	errors [6]uint64 // Counter of TSM compcations (by level) that have failed due to error.
	queue  [6]uint64 // Gauge of TSM compactions queues (by level).
}

func newCompactionTracker(metrics *compactionMetrics, defaultLables prometheus.Labels) *compactionTracker {
	return &compactionTracker{metrics: metrics, labels: defaultLables}
}

// Labels returns a copy of the default labels used by the tracker's metrics.
// The returned map is safe for modification.
func (t *compactionTracker) Labels(level compactionLevel) prometheus.Labels {
	labels := make(prometheus.Labels, len(t.labels))
	for k, v := range t.labels {
		labels[k] = v
	}

	// All metrics have a level label.
	labels["level"] = fmt.Sprint(level)
	return labels
}

// Completed returns the total number of compactions for the provided level.
func (t *compactionTracker) Completed(level int) uint64 { return atomic.LoadUint64(&t.ok[level]) }

// Active returns the number of active snapshots (level 0),
// level 1, 2 or 3 compactions, optimize compactions (level 4), or full
// compactions (level 5).
func (t *compactionTracker) Active(level int) uint64 {
	return atomic.LoadUint64(&t.active[level])
}

// AllActive returns the number of active snapshots and compactions.
func (t *compactionTracker) AllActive() uint64 {
	var total uint64
	for i := 0; i < len(t.active); i++ {
		total += atomic.LoadUint64(&t.active[i])
	}
	return total
}

// ActiveOptimise returns the number of active Optimise compactions.
//
// ActiveOptimise is a helper for Active(4).
func (t *compactionTracker) ActiveOptimise() uint64 { return t.Active(4) }

// ActiveFull returns the number of active Full compactions.
//
// ActiveFull is a helper for Active(5).
func (t *compactionTracker) ActiveFull() uint64 { return t.Active(5) }

// Errors returns the total number of errors encountered attempting compactions
// for the provided level.
func (t *compactionTracker) Errors(level int) uint64 { return atomic.LoadUint64(&t.errors[level]) }

// IncActive increments the number of active compactions for the provided level.
func (t *compactionTracker) IncActive(level compactionLevel) {
	atomic.AddUint64(&t.active[level], 1)

	labels := t.Labels(level)
	t.metrics.CompactionsActive.With(labels).Inc()
}

// IncFullActive increments the number of active Full compactions.
func (t *compactionTracker) IncFullActive() { t.IncActive(5) }

// DecActive decrements the number of active compactions for the provided level.
func (t *compactionTracker) DecActive(level compactionLevel) {
	atomic.AddUint64(&t.active[level], ^uint64(0))

	labels := t.Labels(level)
	t.metrics.CompactionsActive.With(labels).Dec()
}

// DecFullActive decrements the number of active Full compactions.
func (t *compactionTracker) DecFullActive() { t.DecActive(5) }

// Attempted updates the number of compactions attempted for the provided level.
func (t *compactionTracker) Attempted(level compactionLevel, success bool, reason string, duration time.Duration) {
	if success {
		atomic.AddUint64(&t.ok[level], 1)

		labels := t.Labels(level)
		t.metrics.CompactionDuration.With(labels).Observe(duration.Seconds())

		// Total compactions metric has reason and status.
		labels["reason"] = reason
		labels["status"] = "ok"
		t.metrics.Compactions.With(labels).Inc()
		return
	}

	atomic.AddUint64(&t.errors[level], 1)

	labels := t.Labels(level)
	labels["status"] = "error"
	labels["reason"] = reason
	t.metrics.Compactions.With(labels).Inc()
}

// SnapshotAttempted updates the number of snapshots attempted.
func (t *compactionTracker) SnapshotAttempted(success bool, reason CacheStatus, duration time.Duration) {
	t.Attempted(0, success, reason.String(), duration)
}

// SetQueue sets the compaction queue depth for the provided level.
func (t *compactionTracker) SetQueue(level compactionLevel, length uint64) {
	atomic.StoreUint64(&t.queue[level], length)

	labels := t.Labels(level)
	t.metrics.CompactionQueue.With(labels).Set(float64(length))
}

// SetOptimiseQueue sets the queue depth for Optimisation compactions.
func (t *compactionTracker) SetOptimiseQueue(length uint64) { t.SetQueue(4, length) }

// SetFullQueue sets the queue depth for Full compactions.
func (t *compactionTracker) SetFullQueue(length uint64) { t.SetQueue(5, length) }

func (e *Engine) WriteSnapshot(ctx context.Context, status CacheStatus) error {
	start := time.Now()
	err := e.writeSnapshot(ctx)
	if err != nil && err != errCompactionsDisabled {
		e.logger.Info("Error writing snapshot", zap.Error(err))
	}
	e.compactionTracker.SnapshotAttempted(
		err == nil || err == errCompactionsDisabled || err == ErrSnapshotInProgress,
		status, time.Since(start))

	if err != nil {
		return err
	}
	return nil
}

// WriteSnapshot will snapshot the cache and write a new TSM file with its contents, releasing the snapshot when done.
func (e *Engine) writeSnapshot(ctx context.Context) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Lock and grab the cache snapshot along with all the closed WAL
	// filenames associated with the snapshot

	started := time.Now()

	log, logEnd := logger.NewOperation(ctx, e.logger, "Cache snapshot", "tsm1_cache_snapshot")
	defer func() {
		elapsed := time.Since(started)
		log.Info("Snapshot for path written",
			zap.String("path", e.path),
			zap.Duration("duration", elapsed))
		logEnd()
	}()

	var (
		snapshot *Cache
		segments []string
	)
	if err := e.snapshotter.AcquireSegments(ctx, func(segs []string) (err error) {
		segments = segs

		e.mu.Lock()
		snapshot, err = e.Cache.Snapshot()
		e.mu.Unlock()
		return err
	}); err != nil {
		return err
	}

	if snapshot.Size() == 0 {
		e.Cache.ClearSnapshot(true)
		return nil
	}

	// The snapshotted cache may have duplicate points and unsorted data.  We need to deduplicate
	// it before writing the snapshot.  This can be very expensive so it's done while we are not
	// holding the engine write lock.
	snapshot.Deduplicate()

	return e.writeSnapshotAndCommit(ctx, log, snapshot, segments)
}

// writeSnapshotAndCommit will write the passed cache to a new TSM file and remove the closed WAL segments.
func (e *Engine) writeSnapshotAndCommit(ctx context.Context, log *zap.Logger, snapshot *Cache, segments []string) (err error) {
	defer func() {
		if err != nil {
			e.Cache.ClearSnapshot(false)
		}
	}()

	// write the new snapshot files
	newFiles, err := e.Compactor.WriteSnapshot(ctx, snapshot)
	if err != nil {
		log.Info("Error writing snapshot from compactor", zap.Error(err))
		return err
	}

	return e.snapshotter.CommitSegments(ctx, segments, func() error {
		e.mu.RLock()
		defer e.mu.RUnlock()

		// update the file store with these new files
		if err := e.FileStore.Replace(nil, newFiles); err != nil {
			log.Info("Error adding new TSM files from snapshot", zap.Error(err))
			return err
		}

		// clear the snapshot from the in-memory cache
		e.Cache.ClearSnapshot(true)
		return nil
	})
}

// compactCache checks once per second if the in-memory cache should be
// snapshotted to a TSM file.
func (e *Engine) compactCache() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		e.mu.RLock()
		quit := e.snapDone
		e.mu.RUnlock()

		select {
		case <-quit:
			return

		case <-t.C:
			e.Cache.UpdateAge()
			status := e.ShouldCompactCache(time.Now())
			if status == CacheStatusOkay {
				continue
			}

			span, ctx := tracing.StartSpanFromContextWithOperationName(context.Background(), "compact cache")
			span.LogKV("path", e.path)

			err := e.WriteSnapshot(ctx, status)
			if err != nil && err != errCompactionsDisabled && err != ErrSnapshotInProgress {
				e.logger.Info("Error writing snapshot", zap.Error(err))
			}

			span.Finish()
		}
	}
}

// CacheStatus describes the current state of the cache, with respect to whether
// it is ready to be snapshotted or not.
type CacheStatus int

// Possible types of Cache status
const (
	CacheStatusOkay           CacheStatus = iota // Cache is Okay - do not snapshot.
	CacheStatusSizeExceeded                      // The cache is large enough to be snapshotted.
	CacheStatusAgeExceeded                       // The cache is past the age threshold to be snapshotted.
	CacheStatusColdNoWrites                      // The cache has not been written to for long enough that it should be snapshotted.
	CacheStatusRetention                         // The cache was snapshotted before running retention.
	CacheStatusFullCompaction                    // The cache was snapshotted as part of a full compaction.
	CacheStatusBackup                            // The cache was snapshotted before running backup.
)

// ShouldCompactCache returns a status indicating if the Cache should be
// snapshotted. There are three situations when the cache should be snapshotted:
//
// - the Cache size is over its flush size threshold;
// - the Cache has not been snapshotted for longer than its flush time threshold; or
// - the Cache has not been written since the write cold threshold.
//
func (e *Engine) ShouldCompactCache(t time.Time) CacheStatus {
	sz := e.Cache.Size()
	if sz == 0 {
		return 0
	}

	// Cache is now big enough to snapshot.
	if sz > e.CacheFlushMemorySizeThreshold {
		return CacheStatusSizeExceeded
	}

	// Cache is now old enough to snapshot, regardless of last write or age.
	if e.CacheFlushAgeDurationThreshold > 0 && e.Cache.Age() > e.CacheFlushAgeDurationThreshold {
		return CacheStatusAgeExceeded
	}

	// Cache has not been written to for a long time.
	if t.Sub(e.Cache.LastWriteTime()) > e.CacheFlushWriteColdDuration {
		return CacheStatusColdNoWrites
	}
	return CacheStatusOkay
}

func (e *Engine) lastModified() time.Time {
	fsTime := e.FileStore.LastModified()
	cacheTime := e.Cache.LastWriteTime()

	if cacheTime.After(fsTime) {
		return cacheTime
	}

	return fsTime
}

func (e *Engine) compact(wg *sync.WaitGroup) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		e.mu.RLock()
		quit := e.done
		e.mu.RUnlock()

		select {
		case <-quit:
			return

		case <-t.C:

			span, ctx := tracing.StartSpanFromContext(context.Background())

			// Find our compaction plans
			level1Groups := e.CompactionPlan.PlanLevel(1)
			level2Groups := e.CompactionPlan.PlanLevel(2)
			level3Groups := e.CompactionPlan.PlanLevel(3)
			level4Groups := e.CompactionPlan.Plan(e.lastModified())
			e.compactionTracker.SetOptimiseQueue(uint64(len(level4Groups)))

			// If no full compactions are need, see if an optimize is needed
			if len(level4Groups) == 0 {
				level4Groups = e.CompactionPlan.PlanOptimize()
				e.compactionTracker.SetOptimiseQueue(uint64(len(level4Groups)))
			}

			// Update the level plan queue stats
			e.compactionTracker.SetQueue(1, uint64(len(level1Groups)))
			e.compactionTracker.SetQueue(2, uint64(len(level2Groups)))
			e.compactionTracker.SetQueue(3, uint64(len(level3Groups)))

			// Set the queue depths on the scheduler
			e.scheduler.setDepth(1, len(level1Groups))
			e.scheduler.setDepth(2, len(level2Groups))
			e.scheduler.setDepth(3, len(level3Groups))
			e.scheduler.setDepth(4, len(level4Groups))

			// Find the next compaction that can run and try to kick it off
			level, runnable := e.scheduler.next()
			if runnable {
				span.LogKV("level", level)
				switch level {
				case 1:
					if e.compactHiPriorityLevel(ctx, level1Groups[0], 1, false, wg) {
						level1Groups = level1Groups[1:]
					}
				case 2:
					if e.compactHiPriorityLevel(ctx, level2Groups[0], 2, false, wg) {
						level2Groups = level2Groups[1:]
					}
				case 3:
					if e.compactLoPriorityLevel(ctx, level3Groups[0], 3, true, wg) {
						level3Groups = level3Groups[1:]
					}
				case 4:
					if e.compactFull(ctx, level4Groups[0], wg) {
						level4Groups = level4Groups[1:]
					}
				}
			}

			// Release all the plans we didn't start.
			e.CompactionPlan.Release(level1Groups)
			e.CompactionPlan.Release(level2Groups)
			e.CompactionPlan.Release(level3Groups)
			e.CompactionPlan.Release(level4Groups)

			if runnable {
				span.Finish()
			}
		}
	}
}

// compactHiPriorityLevel kicks off compactions using the high priority policy. It returns
// true if the compaction was started
func (e *Engine) compactHiPriorityLevel(ctx context.Context, grp CompactionGroup, level compactionLevel, fast bool, wg *sync.WaitGroup) bool {
	s := e.levelCompactionStrategy(grp, fast, level)
	if s == nil {
		return false
	}

	// Try hi priority limiter, otherwise steal a little from the low priority if we can.
	if e.compactionLimiter.TryTake() {
		e.compactionTracker.IncActive(level)

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer e.compactionTracker.DecActive(level)
			defer e.compactionLimiter.Release()
			s.Apply(ctx)
			// Release the files in the compaction plan
			e.CompactionPlan.Release([]CompactionGroup{s.group})
		}()
		return true
	}

	// Return the unused plans
	return false
}

// compactLoPriorityLevel kicks off compactions using the lo priority policy. It returns
// the plans that were not able to be started
func (e *Engine) compactLoPriorityLevel(ctx context.Context, grp CompactionGroup, level compactionLevel, fast bool, wg *sync.WaitGroup) bool {
	s := e.levelCompactionStrategy(grp, fast, level)
	if s == nil {
		return false
	}

	// Try the lo priority limiter, otherwise steal a little from the high priority if we can.
	if e.compactionLimiter.TryTake() {
		e.compactionTracker.IncActive(level)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer e.compactionTracker.DecActive(level)
			defer e.compactionLimiter.Release()
			s.Apply(ctx)
			// Release the files in the compaction plan
			e.CompactionPlan.Release([]CompactionGroup{s.group})
		}()
		return true
	}
	return false
}

// compactFull kicks off full and optimize compactions using the lo priority policy. It returns
// the plans that were not able to be started.
func (e *Engine) compactFull(ctx context.Context, grp CompactionGroup, wg *sync.WaitGroup) bool {
	s := e.fullCompactionStrategy(grp, false)
	if s == nil {
		return false
	}

	// Try the lo priority limiter, otherwise steal a little from the high priority if we can.
	if e.compactionLimiter.TryTake() {
		// Attempt to get ownership of the semaphore for this engine. If the
		// default semaphore is in use then ownership will always be granted.
		ttl := influxdb.DefaultLeaseTTL
		lastCompaction := time.Duration(atomic.LoadInt64(&e.lastFullCompactionDuration))
		if lastCompaction > ttl {
			ttl = lastCompaction // If the last full compaction took > default ttl then set a new TTL
		}

		lease, err := e.fullCompactionSemaphore.TryAcquire(ctx, ttl)
		if err == influxdb.ErrNoAcquire {
			e.logger.Info("Cannot acquire semaphore ownership to carry out full compaction", zap.Duration("semaphore_requested_ttl", ttl))
			e.compactionLimiter.Release()
			return false
		} else if err != nil {
			e.logger.Warn("Failed to execute full compaction", zap.Error(err), zap.Duration("semaphore_requested_ttl", ttl))
			e.compactionLimiter.Release()
			return false
		} else if e.fullCompactionSemaphore != influxdb.NopSemaphore {
			e.logger.Info("Acquired semaphore ownership for full compaction", zap.Duration("semaphore_requested_ttl", ttl))
		}

		ctx, cancel := context.WithCancel(ctx)
		go e.keepLeaseAlive(ctx, lease) // context cancelled when compaction finished.

		e.compactionTracker.IncFullActive()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer e.compactionTracker.DecFullActive()
			defer e.compactionLimiter.Release()

			now := time.Now() // Track how long compaction takes
			s.Apply(ctx)
			atomic.StoreInt64(&e.lastFullCompactionDuration, int64(time.Since(now)))

			// Release the files in the compaction plan
			e.CompactionPlan.Release([]CompactionGroup{s.group})
			cancel()
		}()
		return true
	}
	return false
}

// keepLeaseAlive blocks, keeping a lease alive until the context is cancelled.
func (e *Engine) keepLeaseAlive(ctx context.Context, lease influxdb.Lease) {
	ttl, err := lease.TTL(ctx)
	if err != nil {
		e.logger.Warn("Unable to get TTL for lease on semaphore", zap.Error(err))
		ttl = influxdb.DefaultLeaseTTL // This is probably a reasonable fallback.
	}

	// Renew the lease when ttl is halved
	ticker := time.NewTicker(ttl / 2)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			if err := lease.Release(ctx); err != nil {
				e.logger.Warn("Lease on sempahore was not released", zap.Error(err))
			}
			return
		case <-ticker.C:
			if err := lease.KeepAlive(ctx); err != nil {
				e.logger.Warn("Unable to extend lease", zap.Error(err))
			} else {
				e.logger.Info("Extended lease on semaphore")
			}
		}
	}
}

// compactionStrategy holds the details of what to do in a compaction.
type compactionStrategy struct {
	group CompactionGroup

	fast  bool
	level compactionLevel

	tracker *compactionTracker

	logger    *zap.Logger
	compactor *Compactor
	fileStore *FileStore

	engine *Engine
}

// Apply concurrently compacts all the groups in a compaction strategy.
func (s *compactionStrategy) Apply(ctx context.Context) {
	s.compactGroup(ctx)
}

// compactGroup executes the compaction strategy against a single CompactionGroup.
func (s *compactionStrategy) compactGroup(ctx context.Context) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	now := time.Now()
	group := s.group
	log, logEnd := logger.NewOperation(ctx, s.logger, "TSM compaction", "tsm1_compact_group")
	defer logEnd()

	log.Info("Beginning compaction", zap.Int("tsm1_files_n", len(group)))
	span.LogKV("file qty", len(group), "fast", s.fast)
	for i, f := range group {
		log.Info("Compacting file", zap.Int("tsm1_index", i), zap.String("tsm1_file", f))
		span.LogKV("compact file", "start", "tsm1_index", i, "tsm1_file", f)
	}

	var (
		err   error
		files []string
	)

	if s.fast {
		files, err = s.compactor.CompactFast(group)
	} else {
		files, err = s.compactor.CompactFull(group)
	}

	if err != nil {
		tracing.LogError(span, err)
		_, inProgress := err.(errCompactionInProgress)
		if err == errCompactionsDisabled || inProgress {
			log.Info("Aborted compaction", zap.Error(err))

			if _, ok := err.(errCompactionInProgress); ok {
				time.Sleep(time.Second)
			}
			return
		}

		log.Info("Error compacting TSM files", zap.Error(err))
		s.tracker.Attempted(s.level, false, "", 0)
		time.Sleep(time.Second)
		return
	}

	if err := s.fileStore.ReplaceWithCallback(group, files, nil); err != nil {
		tracing.LogError(span, err)
		log.Info("Error replacing new TSM files", zap.Error(err))
		s.tracker.Attempted(s.level, false, "", 0)
		time.Sleep(time.Second)

		// Remove the new snapshot files. We will try again.
		for _, file := range files {
			if err := os.Remove(file); err != nil {
				log.Error("Unable to remove file", zap.String("path", file), zap.Error(err))
			}
		}

		return
	}

	for i, f := range files {
		log.Info("Compacted file", zap.Int("tsm1_index", i), zap.String("tsm1_file", f))
		span.LogKV("compact file", "end", "tsm1_index", i, "tsm1_file", f)
	}
	log.Info("Finished compacting files", zap.Int("tsm1_files_n", len(files)))
	s.tracker.Attempted(s.level, true, "", time.Since(now))
}

// levelCompactionStrategy returns a compactionStrategy for the given level.
// It returns nil if there are no TSM files to compact.
func (e *Engine) levelCompactionStrategy(group CompactionGroup, fast bool, level compactionLevel) *compactionStrategy {
	return &compactionStrategy{
		group:     group,
		logger:    e.logger.With(zap.Int("tsm1_level", int(level)), zap.String("tsm1_strategy", "level")),
		fileStore: e.FileStore,
		compactor: e.Compactor,
		fast:      fast,
		engine:    e,
		level:     level,
		tracker:   e.compactionTracker,
	}
}

// fullCompactionStrategy returns a compactionStrategy for higher level generations of TSM files.
// It returns nil if there are no TSM files to compact.
func (e *Engine) fullCompactionStrategy(group CompactionGroup, optimize bool) *compactionStrategy {
	s := &compactionStrategy{
		group:     group,
		logger:    e.logger.With(zap.String("tsm1_strategy", "full"), zap.Bool("tsm1_optimize", optimize)),
		fileStore: e.FileStore,
		compactor: e.Compactor,
		fast:      optimize,
		engine:    e,
		level:     5,
		tracker:   e.compactionTracker,
	}

	if optimize {
		s.level = 4
	}
	return s
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

	ext := fmt.Sprintf(".%s", TmpTSMFileExtension)
	for _, f := range allfiles {
		// Check to see if there are any `.tmp` directories that were left over from failed shard snapshots
		if f.IsDir() && strings.HasSuffix(f.Name(), ext) {
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
func (e *Engine) KeyCursor(ctx context.Context, key []byte, t int64, ascending bool) *KeyCursor {
	return e.FileStore.KeyCursor(ctx, key, t, ascending)
}

// IteratorCost produces the cost of an iterator.
func (e *Engine) IteratorCost(measurement string, opt query.IteratorOptions) (query.IteratorCost, error) {
	// Determine if this measurement exists. If it does not, then no shards are
	// accessed to begin with.
	if exists, err := e.index.MeasurementExists([]byte(measurement)); err != nil {
		return query.IteratorCost{}, err
	} else if !exists {
		return query.IteratorCost{}, nil
	}

	tagSets, err := e.index.TagSets([]byte(measurement), opt)
	if err != nil {
		return query.IteratorCost{}, err
	}

	// Attempt to retrieve the ref from the main expression (if it exists).
	var ref *influxql.VarRef
	if opt.Expr != nil {
		if v, ok := opt.Expr.(*influxql.VarRef); ok {
			ref = v
		} else if call, ok := opt.Expr.(*influxql.Call); ok {
			if len(call.Args) > 0 {
				ref, _ = call.Args[0].(*influxql.VarRef)
			}
		}
	}

	// Count the number of series concatenated from the tag set.
	cost := query.IteratorCost{NumShards: 1}
	for _, t := range tagSets {
		cost.NumSeries += int64(len(t.SeriesKeys))
		for i, key := range t.SeriesKeys {
			// Retrieve the cost for the main expression (if it exists).
			if ref != nil {
				c := e.seriesCost(key, ref.Val, opt.StartTime, opt.EndTime)
				cost = cost.Combine(c)
			}

			// Retrieve the cost for every auxiliary field since these are also
			// iterators that we may have to look through.
			// We may want to separate these though as we are unlikely to incur
			// anywhere close to the full costs of the auxiliary iterators because
			// many of the selected values are usually skipped.
			for _, ref := range opt.Aux {
				c := e.seriesCost(key, ref.Val, opt.StartTime, opt.EndTime)
				cost = cost.Combine(c)
			}

			// Retrieve the expression names in the condition (if there is a condition).
			// We will also create cursors for these too.
			if t.Filters[i] != nil {
				refs := influxql.ExprNames(t.Filters[i])
				for _, ref := range refs {
					c := e.seriesCost(key, ref.Val, opt.StartTime, opt.EndTime)
					cost = cost.Combine(c)
				}
			}
		}
	}
	return cost, nil
}

func (e *Engine) seriesCost(seriesKey, field string, tmin, tmax int64) query.IteratorCost {
	key := SeriesFieldKeyBytes(seriesKey, field)
	c := e.FileStore.Cost(key, tmin, tmax)

	// Retrieve the range of values within the cache.
	cacheValues := e.Cache.Values(key)
	c.CachedValues = int64(len(cacheValues.Include(tmin, tmax)))
	return c
}

// SeriesFieldKey combine a series key and field name for a unique string to be hashed to a numeric ID.
func SeriesFieldKey(seriesKey, field string) string {
	return seriesKey + keyFieldSeparator + field
}

func SeriesFieldKeyBytes(seriesKey, field string) []byte {
	b := make([]byte, len(seriesKey)+len(keyFieldSeparator)+len(field))
	i := copy(b[:], seriesKey)
	i += copy(b[i:], KeyFieldSeparatorBytes)
	copy(b[i:], field)
	return b
}

// AppendSeriesFieldKeyBytes combines seriesKey and field such
// that can be used to search a TSM index. The value is appended to dst and
// the extended buffer returned.
func AppendSeriesFieldKeyBytes(dst, seriesKey, field []byte) []byte {
	dst = append(dst, seriesKey...)
	dst = append(dst, KeyFieldSeparatorBytes...)
	return append(dst, field...)
}

var (
	blockToFieldType = [8]influxql.DataType{
		BlockFloat64:   influxql.Float,
		BlockInteger:   influxql.Integer,
		BlockBoolean:   influxql.Boolean,
		BlockString:    influxql.String,
		BlockUnsigned:  influxql.Unsigned,
		BlockUndefined: influxql.Unknown,
		6:              influxql.Unknown,
		7:              influxql.Unknown,
	}
)

func BlockTypeToInfluxQLDataType(typ byte) influxql.DataType { return blockToFieldType[typ&7] }

var (
	blockTypeFieldType = [8]cursors.FieldType{
		BlockFloat64:   cursors.Float,
		BlockInteger:   cursors.Integer,
		BlockBoolean:   cursors.Boolean,
		BlockString:    cursors.String,
		BlockUnsigned:  cursors.Unsigned,
		BlockUndefined: cursors.Undefined,
		6:              cursors.Undefined,
		7:              cursors.Undefined,
	}
)

func BlockTypeToFieldType(typ byte) cursors.FieldType { return blockTypeFieldType[typ&7] }

// SeriesAndFieldFromCompositeKey returns the series key and the field key extracted from the composite key.
func SeriesAndFieldFromCompositeKey(key []byte) ([]byte, []byte) {
	sep := bytes.Index(key, KeyFieldSeparatorBytes)
	if sep == -1 {
		// No field???
		return key, nil
	}
	return key[:sep], key[sep+len(keyFieldSeparator):]
}

// readTracker tracks reads from the engine.
type readTracker struct {
	metrics *readMetrics
	labels  prometheus.Labels
	cursors uint64
	seeks   uint64
}

func newReadTracker(metrics *readMetrics, defaultLabels prometheus.Labels) *readTracker {
	t := &readTracker{metrics: metrics, labels: defaultLabels}
	t.AddCursors(0)
	t.AddSeeks(0)
	return t
}

// Labels returns a copy of the default labels used by the tracker's metrics.
// The returned map is safe for modification.
func (t *readTracker) Labels() prometheus.Labels {
	labels := make(prometheus.Labels, len(t.labels))
	for k, v := range t.labels {
		labels[k] = v
	}
	return labels
}

// AddCursors increases the number of cursors.
func (t *readTracker) AddCursors(n uint64) {
	atomic.AddUint64(&t.cursors, n)
	t.metrics.Cursors.With(t.labels).Add(float64(n))
}

// AddSeeks increases the number of location seeks.
func (t *readTracker) AddSeeks(n uint64) {
	atomic.AddUint64(&t.seeks, n)
	t.metrics.Seeks.With(t.labels).Add(float64(n))
}
