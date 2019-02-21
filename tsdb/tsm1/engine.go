// Package tsm1 provides a TSDB in the Time Structured Merge tree format.
package tsm1 // import "github.com/influxdata/influxdb/tsdb/tsm1"

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/lifecycle"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/pkg/metrics"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/tsi1"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@array_cursor.gen.go.tmpldata array_cursor.gen.go.tmpl array_cursor_iterator.gen.go.tmpl
//go:generate env GO111MODULE=on go run github.com/influxdata/influxdb/tools/tmpl -i -data=file_store.gen.go.tmpldata file_store.gen.go.tmpl=file_store.gen.go
//go:generate env GO111MODULE=on go run github.com/influxdata/influxdb/tools/tmpl -i -d isArray=y -data=file_store.gen.go.tmpldata file_store.gen.go.tmpl=file_store_array.gen.go
//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@encoding.gen.go.tmpldata encoding.gen.go.tmpl
//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@compact.gen.go.tmpldata compact.gen.go.tmpl
//go:generate env GO111MODULE=on go run github.com/benbjohnson/tmpl -data=@reader.gen.go.tmpldata reader.gen.go.tmpl

var (
	// Static objects to prevent small allocs.
	keyFieldSeparatorBytes = []byte(keyFieldSeparator)
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

// WithTraceLogging sets if trace logging is enabled for the engine.
func WithTraceLogging(logging bool) EngineOption {
	return func(e *Engine) {
		e.fileStore.enableTraceLogging(logging)
	}
}

// WithCompactionPlanner sets the compaction planner for the engine.
func WithCompactionPlanner(planner CompactionPlanner) EngineOption {
	return func(e *Engine) {
		planner.SetFileStore(e.fileStore)
		e.compactionPlanner = planner
	}
}

// Snapshotter allows upward signaling of the tsm1 engine to the storage engine. Hopefully
// it can be removed one day. The weird interface is due to the weird inversion of locking
// that has to happen.
type Snapshotter interface {
	AcquireSegments(func(segments []string) error) error
	CommitSegments(segments []string, fn func() error) error
}

type noSnapshotter struct{}

func (noSnapshotter) AcquireSegments(fn func([]string) error) error    { return fn(nil) }
func (noSnapshotter) CommitSegments(_ []string, fn func() error) error { return fn() }

// WithSnapshotter sets the callbacks for the engine to use when creating snapshots.
func WithSnapshotter(snapshotter Snapshotter) EngineOption {
	return func(e *Engine) {
		e.snapshotter = snapshotter
	}
}

// Engine represents a storage engine with compressed blocks.
type Engine struct {
	mu      sync.RWMutex
	tracker lifecycle.Tracker

	path   string
	index  *tsi1.Index
	config Config

	sfile        *tsdb.SeriesFile
	logger       *zap.Logger // Logger to be used for important messages
	traceLogger  *zap.Logger // Logger to be used when trace-logging is on.
	traceLogging bool

	cache             *Cache
	compactor         *Compactor
	compactionPlanner CompactionPlanner
	fileStore         *FileStore

	// Invoked when creating a backup file "as new".
	formatFileName FormatFileNameFunc

	compactionTracker   *compactionTracker // Used to track state of compactions.
	defaultMetricLabels prometheus.Labels  // N.B this must not be mutated after Open is called.

	// Limiter for concurrent compactions.
	compactionLimiter limiter.Fixed

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
		config: config,

		sfile:       idx.SeriesFile(),
		logger:      logger,
		traceLogger: logger,

		cache:     cache,
		fileStore: fs,
		compactor: c,
		compactionPlanner: NewDefaultPlanner(fs,
			time.Duration(config.Compaction.FullWriteColdDuration)),

		formatFileName:    DefaultFormatFileName,
		compactionLimiter: limiter.NewFixed(maxCompactions),
		scheduler:         newScheduler(maxCompactions),
		snapshotter:       new(noSnapshotter),
	}

	for _, option := range options {
		option(e)
	}

	return e
}

// WithFormatFileNameFunc sets the engine's function to format file names.
func (e *Engine) WithFormatFileNameFunc(formatFileNameFunc FormatFileNameFunc) {
	e.compactor.WithFormatFileNameFunc(formatFileNameFunc)
	e.formatFileName = formatFileNameFunc
}

// WithParseFileNameFunc sets the engine's function to parse file names.
func (e *Engine) WithParseFileNameFunc(parseFileNameFunc ParseFileNameFunc) {
	e.fileStore.WithParseFileNameFunc(parseFileNameFunc)
	e.compactor.WithParseFileNameFunc(parseFileNameFunc)
}

// WithFileStoreObserver sets an observer for files in the file store.
func (e *Engine) WithFileStoreObserver(obs FileStoreObserver) {
	e.fileStore.WithObserver(obs)
}

// WithCompactionPlanner sets the CompactionPlanner the engine will use.
func (e *Engine) WithCompactionPlanner(planner CompactionPlanner) {
	planner.SetFileStore(e.fileStore)
	e.compactionPlanner = planner
}

// SetDefaultMetricLabels sets the default labels for metrics on the engine.
// It must be called before the Engine is opened.
func (e *Engine) SetDefaultMetricLabels(labels prometheus.Labels) {
	e.defaultMetricLabels = labels
}

// Path returns the path the engine was opened with.
func (e *Engine) Path() string { return e.path }

// MeasurementStats returns the current measurement stats for the engine.
func (e *Engine) MeasurementStats() (MeasurementStats, error) {
	return e.fileStore.MeasurementStats()
}

// initTrackers sets up the metrics trackers for the engine.
func (e *Engine) initTrackers() {
	mmu.Lock()
	defer mmu.Unlock()

	if bms == nil {
		// Initialise metrics if an engine has not done so already.
		bms = newBlockMetrics(e.defaultMetricLabels)
	}

	// Propagate prometheus metrics down into trackers.
	e.compactionTracker = newCompactionTracker(bms.compactionMetrics, e.defaultMetricLabels)
	e.fileStore.tracker = newFileTracker(bms.fileMetrics, e.defaultMetricLabels)
	e.cache.tracker = newCacheTracker(bms.cacheMetrics, e.defaultMetricLabels)

	e.scheduler.setCompactionTracker(e.compactionTracker)
}

// Open opens and initializes the engine.
func (e *Engine) Open() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.initTrackers()

	if err := os.MkdirAll(e.path, 0777); err != nil {
		return err
	}

	if err := e.cleanup(); err != nil {
		return err
	}

	intC := e.tracker.Open()
	if err := e.fileStore.Open(); err != nil {
		return err
	}
	e.compactor.Open()

	// Start the two tasks. We can't call start from within the
	// goroutine in the same way that you shouldn't for Add on
	// a wait group.
	e.tracker.Start()
	go e.compactCache(intC)

	e.tracker.Start()
	go e.compactLevels(intC)

	return nil
}

// Close closes the engine. Subsequent calls to Close are a nop.
func (e *Engine) Close() error {
	// Lock now and close everything else down.
	e.mu.Lock()
	defer e.mu.Unlock()

	e.compactor.Close()
	if err := e.fileStore.Close(); err != nil {
		return err
	}
	e.tracker.Close()

	return nil
}

// WithLogger sets the logger for the engine.
func (e *Engine) WithLogger(log *zap.Logger) {
	e.logger = log.With(zap.String("engine", "tsm1"))

	if e.traceLogging {
		e.traceLogger = e.logger
	}

	e.fileStore.WithLogger(e.logger)
}

// IsIdle returns true if the cache is empty, there are no running compactions and the
// shard is fully compacted.
func (e *Engine) IsIdle() bool {
	cacheEmpty := e.cache.Size() == 0
	return cacheEmpty &&
		e.compactionTracker.AllActive() == 0 &&
		e.compactionPlanner.FullyCompacted()
}

// WritePoints saves the set of points in the engine.
func (e *Engine) WritePoints(points []models.Point) error {
	values, err := PointsToValues(points)
	if err != nil {
		return err
	}

	return e.WriteValues(values)
}

// WriteValues saves the set of values in the engine.
func (e *Engine) WriteValues(values map[string][]Value) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if err := e.cache.WriteMulti(values); err != nil {
		return err
	}

	return nil
}

// DisableMaxSize disables the cache's maximum size limit until the returned function
// is called. It is not safe to call this while there is a closure already returned.
func (e *Engine) DisableMaxSize() func() {
	limit := e.cache.MaxSize()
	e.cache.SetMaxSize(0)
	return func() { e.cache.SetMaxSize(limit) }
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
func (t *compactionTracker) Attempted(level compactionLevel, success bool, duration time.Duration) {
	if success {
		atomic.AddUint64(&t.ok[level], 1)

		labels := t.Labels(level)

		t.metrics.CompactionDuration.With(labels).Observe(duration.Seconds())

		labels["status"] = "ok"
		t.metrics.Compactions.With(labels).Inc()

		return
	}

	atomic.AddUint64(&t.errors[level], 1)

	labels := t.Labels(level)
	labels["status"] = "error"
	t.metrics.Compactions.With(labels).Inc()
}

// SnapshotAttempted updates the number of snapshots attempted.
func (t *compactionTracker) SnapshotAttempted(success bool, duration time.Duration) {
	t.Attempted(0, success, duration)
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

// WriteSnapshot will snapshot the cache and write a new TSM file with its contents, releasing the snapshot when done.
func (e *Engine) WriteSnapshot() error {
	// Lock and grab the cache snapshot along with all the closed WAL
	// filenames associated with the snapshot
	started := time.Now()

	log, logEnd := logger.NewOperation(e.logger, "Cache snapshot", "tsm1_cache_snapshot")
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
	if err := e.snapshotter.AcquireSegments(func(segs []string) (err error) {
		segments = segs

		e.mu.Lock()
		snapshot, err = e.cache.Snapshot()
		e.mu.Unlock()
		return err
	}); err != nil {
		return err
	}

	if snapshot.Size() == 0 {
		e.cache.ClearSnapshot(true)
		return nil
	}

	// The snapshotted cache may have duplicate points and unsorted data.  We need to deduplicate
	// it before writing the snapshot.  This can be very expensive so it's done while we are not
	// holding the engine write lock.
	dedup := time.Now()
	snapshot.Deduplicate()
	e.traceLogger.Info("Snapshot for path deduplicated",
		zap.String("path", e.path),
		zap.Duration("duration", time.Since(dedup)))

	return e.writeSnapshotAndCommit(log, snapshot, segments)
}

// writeSnapshotAndCommit will write the passed cache to a new TSM file and remove the closed WAL segments.
func (e *Engine) writeSnapshotAndCommit(log *zap.Logger, snapshot *Cache, segments []string) (err error) {
	defer func() {
		if err != nil {
			e.cache.ClearSnapshot(false)
		}
	}()

	// write the new snapshot files
	newFiles, err := e.compactor.WriteSnapshot(snapshot)
	if err != nil {
		log.Info("Error writing snapshot from compactor", zap.Error(err))
		return err
	}

	return e.snapshotter.CommitSegments(segments, func() error {
		e.mu.RLock()
		defer e.mu.RUnlock()

		// update the file store with these new files
		if err := e.fileStore.Replace(nil, newFiles); err != nil {
			log.Info("Error adding new TSM files from snapshot", zap.Error(err))
			return err
		}

		// clear the snapshot from the in-memory cache
		e.cache.ClearSnapshot(true)
		return nil
	})
}

// compactCache continually checks if the WAL cache should be written to disk.
func (e *Engine) compactCache(intC chan struct{}) {
	defer e.tracker.Stop()

	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-intC:
			return

		case <-t.C:
			e.cache.UpdateAge()
			if !e.shouldCompactCache(time.Now()) {
				continue
			}

			start := time.Now()
			e.traceLogger.Info("Compacting cache", zap.String("path", e.path))
			err := e.WriteSnapshot()
			if err != nil {
				e.logger.Info("Error writing snapshot", zap.Error(err))
			}
			e.compactionTracker.SnapshotAttempted(err == nil, time.Since(start))
		}
	}
}

// shouldCompactCache returns true if the Cache is over its flush threshold
// or if the passed in lastWriteTime is older than the write cold threshold.
func (e *Engine) shouldCompactCache(t time.Time) bool {
	sz := e.cache.Size()

	if sz == 0 {
		return false
	}

	if sz > uint64(e.config.Cache.SnapshotMemorySize) {
		return true
	}

	return t.Sub(e.cache.LastWriteTime()) >
		time.Duration(e.config.Cache.SnapshotWriteColdDuration)
}

func (e *Engine) compactLevels(intC chan struct{}) {
	defer e.tracker.Stop()

	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-intC:
			return

		case <-t.C:

			// Find our compaction plans
			level1Groups := e.compactionPlanner.PlanLevel(1)
			level2Groups := e.compactionPlanner.PlanLevel(2)
			level3Groups := e.compactionPlanner.PlanLevel(3)
			level4Groups := e.compactionPlanner.Plan(e.fileStore.LastModified())
			e.compactionTracker.SetOptimiseQueue(uint64(len(level4Groups)))

			// If no full compactions are need, see if an optimize is needed
			if len(level4Groups) == 0 {
				level4Groups = e.compactionPlanner.PlanOptimize()
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
			if level, runnable := e.scheduler.next(); runnable {
				switch level {
				case 1:
					if e.compactHiPriorityLevel(level1Groups[0], 1, false) {
						level1Groups = level1Groups[1:]
					}
				case 2:
					if e.compactHiPriorityLevel(level2Groups[0], 2, false) {
						level2Groups = level2Groups[1:]
					}
				case 3:
					if e.compactLoPriorityLevel(level3Groups[0], 3, true) {
						level3Groups = level3Groups[1:]
					}
				case 4:
					if e.compactFull(level4Groups[0]) {
						level4Groups = level4Groups[1:]
					}
				}
			}

			// Release all the plans we didn't start.
			e.compactionPlanner.Release(level1Groups)
			e.compactionPlanner.Release(level2Groups)
			e.compactionPlanner.Release(level3Groups)
			e.compactionPlanner.Release(level4Groups)
		}
	}
}

// compactHiPriorityLevel kicks off compactions using the high priority policy. It returns
// true if the compaction was started
func (e *Engine) compactHiPriorityLevel(grp CompactionGroup, level compactionLevel, fast bool) bool {
	s := e.levelCompactionStrategy(grp, fast, level)
	if s == nil {
		return false
	}

	// Try hi priority limiter, otherwise steal a little from the low priority if we can.
	if e.compactionLimiter.TryTake() {
		if _, ok := e.tracker.Start(); ok {
			e.compactionTracker.IncActive(level)
			go func() {
				defer e.tracker.Stop()
				defer e.compactionTracker.DecActive(level)
				defer e.compactionLimiter.Release()
				s.Apply()
				// Release the files in the compaction plan
				e.compactionPlanner.Release([]CompactionGroup{s.group})
			}()
			return true
		}
	}

	// Return the unused plans
	return false
}

// compactLoPriorityLevel kicks off compactions using the lo priority policy. It returns
// the plans that were not able to be started
func (e *Engine) compactLoPriorityLevel(grp CompactionGroup, level compactionLevel, fast bool) bool {
	s := e.levelCompactionStrategy(grp, fast, level)
	if s == nil {
		return false
	}

	// Try the lo priority limiter, otherwise steal a little from the high priority if we can.
	if e.compactionLimiter.TryTake() {
		if _, ok := e.tracker.Start(); ok {
			e.compactionTracker.IncActive(level)
			go func() {
				defer e.tracker.Stop()
				defer e.compactionTracker.DecActive(level)
				defer e.compactionLimiter.Release()
				s.Apply()
				// Release the files in the compaction plan
				e.compactionPlanner.Release([]CompactionGroup{s.group})
			}()
			return true
		}
	}
	return false
}

// compactFull kicks off full and optimize compactions using the lo priority policy. It returns
// the plans that were not able to be started.
func (e *Engine) compactFull(grp CompactionGroup) bool {
	s := e.fullCompactionStrategy(grp, false)
	if s == nil {
		return false
	}

	// Try the lo priority limiter, otherwise steal a little from the high priority if we can.
	if e.compactionLimiter.TryTake() {
		if _, ok := e.tracker.Start(); ok {
			e.compactionTracker.IncFullActive()
			go func() {
				defer e.tracker.Stop()
				defer e.compactionTracker.DecFullActive()
				defer e.compactionLimiter.Release()
				s.Apply()
				// Release the files in the compaction plan
				e.compactionPlanner.Release([]CompactionGroup{s.group})
			}()
			return true
		}
	}
	return false
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
func (s *compactionStrategy) Apply() {
	s.compactGroup()
}

// compactGroup executes the compaction strategy against a single CompactionGroup.
func (s *compactionStrategy) compactGroup() {
	now := time.Now()
	group := s.group
	log, logEnd := logger.NewOperation(s.logger, "TSM compaction", "tsm1_compact_group")
	defer logEnd()

	log.Info("Beginning compaction", zap.Int("tsm1_files_n", len(group)))
	for i, f := range group {
		log.Info("Compacting file", zap.Int("tsm1_index", i), zap.String("tsm1_file", f))
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
		if _, ok := err.(errCompactionInProgress); ok {
			log.Info("Aborted compaction", zap.Error(err))
			time.Sleep(time.Second)
			return
		}

		log.Info("Error compacting TSM files", zap.Error(err))
		s.tracker.Attempted(s.level, false, 0)
		time.Sleep(time.Second)
		return
	}

	if err := s.fileStore.ReplaceWithCallback(group, files, nil); err != nil {
		log.Info("Error replacing new TSM files", zap.Error(err))
		s.tracker.Attempted(s.level, false, 0)
		time.Sleep(time.Second)
		return
	}

	for i, f := range files {
		log.Info("Compacted file", zap.Int("tsm1_index", i), zap.String("tsm1_file", f))
	}
	log.Info("Finished compacting files", zap.Int("tsm1_files_n", len(files)))
	s.tracker.Attempted(s.level, true, time.Since(now))
}

// levelCompactionStrategy returns a compactionStrategy for the given level.
// It returns nil if there are no TSM files to compact.
func (e *Engine) levelCompactionStrategy(group CompactionGroup, fast bool, level compactionLevel) *compactionStrategy {
	return &compactionStrategy{
		group:     group,
		logger:    e.logger.With(zap.Int("tsm1_level", int(level)), zap.String("tsm1_strategy", "level")),
		fileStore: e.fileStore,
		compactor: e.compactor,
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
		fileStore: e.fileStore,
		compactor: e.compactor,
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

// cleanupTempTSMFiles removes all of the temporary tsm files that may have been left over
// from any previous processes.
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
	return e.fileStore.KeyCursor(ctx, key, t, ascending)
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

// seriesCost returns an estimate of the cost of reading the series.
func (e *Engine) seriesCost(seriesKey, field string, tmin, tmax int64) query.IteratorCost {
	key := SeriesFieldKeyBytes(seriesKey, field)
	c := e.fileStore.Cost(key, tmin, tmax)

	// Retrieve the range of values within the cache.
	cacheValues := e.cache.Values(key)
	c.CachedValues = int64(len(cacheValues.Include(tmin, tmax)))
	return c
}

// SeriesFieldKey combine a series key and field name for a unique string to be hashed to a numeric ID.
func SeriesFieldKey(seriesKey, field string) string {
	return seriesKey + keyFieldSeparator + field
}

// SeriesFieldKeyBytes returns a combined series key and field as a byte slice.
func SeriesFieldKeyBytes(seriesKey, field string) []byte {
	b := make([]byte, len(seriesKey)+len(keyFieldSeparator)+len(field))
	i := copy(b[:], seriesKey)
	i += copy(b[i:], keyFieldSeparatorBytes)
	copy(b[i:], field)
	return b
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
