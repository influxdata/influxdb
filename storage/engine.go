package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/storage/wal"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/tsi1"
	"github.com/influxdata/influxdb/tsdb/tsm1"
	"github.com/influxdata/influxdb/tsdb/value"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// Static objects to prevent small allocs.
var timeBytes = []byte("time")

// ErrEngineClosed is returned when a caller attempts to use the engine while
// it's closed.
var ErrEngineClosed = errors.New("engine is closed")

// runner lets us mock out the retention enforcer in tests
type runner interface{ run() }

// runnable is a function that lets the caller know if they can proceed with their
// task. A runnable returns a function that should be called by the caller to
// signal they finished their task.
type runnable func() (done func())

type Engine struct {
	config   Config
	path     string
	engineID *int // Not used by default.
	nodeID   *int // Not used by default.

	mu      sync.RWMutex
	closing chan struct{} //closing returns the zero value when the engine is shutting down.
	index   *tsi1.Index
	sfile   *tsdb.SeriesFile
	engine  *tsm1.Engine
	wal     *wal.WAL

	retentionEnforcer        runner
	retentionEnforcerLimiter runnable

	defaultMetricLabels prometheus.Labels

	// Tracks all goroutines started by the Engine.
	wg sync.WaitGroup

	logger *zap.Logger
}

// Option provides a set
type Option func(*Engine)

// WithTSMFilenameFormatter sets a function on the underlying tsm1.Engine to specify
// how TSM files are named.
func WithTSMFilenameFormatter(fn tsm1.FormatFileNameFunc) Option {
	return func(e *Engine) {
		e.engine.WithFormatFileNameFunc(fn)
	}
}

// WithCurrentGenerationFunc sets a function for obtaining the current generation.
func WithCurrentGenerationFunc(fn func() int) Option {
	return func(e *Engine) {
		e.engine.WithCurrentGenerationFunc(fn)
	}
}

// WithEngineID sets an engine id, which can be useful for logging when multiple
// engines are in use.
func WithEngineID(id int) Option {
	return func(e *Engine) {
		e.engineID = &id
		e.defaultMetricLabels["engine_id"] = fmt.Sprint(*e.engineID)
	}
}

// WithNodeID sets a node id on the engine, which can be useful for logging
// when a system has engines running on multiple nodes.
func WithNodeID(id int) Option {
	return func(e *Engine) {
		e.nodeID = &id
		e.defaultMetricLabels["node_id"] = fmt.Sprint(*e.nodeID)
	}
}

// WithRetentionEnforcer initialises a retention enforcer on the engine.
// WithRetentionEnforcer must be called after other options to ensure that all
// metrics are labelled correctly.
func WithRetentionEnforcer(finder BucketFinder) Option {
	return func(e *Engine) {
		e.retentionEnforcer = newRetentionEnforcer(e, e.engine, finder)
	}
}

// WithRetentionEnforcerLimiter sets a limiter used to control when the
// retention enforcer can proceed. If this option is not used then the default
// limiter (or the absence of one) is a no-op, and no limitations will be put
// on running the retention enforcer.
func WithRetentionEnforcerLimiter(f runnable) Option {
	return func(e *Engine) {
		e.retentionEnforcerLimiter = f
	}
}

// WithFileStoreObserver makes the engine have the provided file store observer.
func WithFileStoreObserver(obs tsm1.FileStoreObserver) Option {
	return func(e *Engine) {
		e.engine.WithFileStoreObserver(obs)
	}
}

// WithCompactionPlanner makes the engine have the provided compaction planner.
func WithCompactionPlanner(planner tsm1.CompactionPlanner) Option {
	return func(e *Engine) {
		e.engine.WithCompactionPlanner(planner)
	}
}

// WithCompactionLimiter allows the caller to set the limiter that a storage
// engine uses. A typical use-case for this would be if multiple engines should
// share the same limiter.
func WithCompactionLimiter(limiter limiter.Fixed) Option {
	return func(e *Engine) {
		e.engine.WithCompactionLimiter(limiter)
	}
}

// WithCompactionSemaphore sets the semaphore used to coordinate full compactions
// across multiple storage engines.
func WithCompactionSemaphore(s influxdb.Semaphore) Option {
	return func(e *Engine) {
		e.engine.SetSemaphore(s)
	}
}

// NewEngine initialises a new storage engine, including a series file, index and
// TSM engine.
func NewEngine(path string, c Config, options ...Option) *Engine {
	e := &Engine{
		config:              c,
		path:                path,
		defaultMetricLabels: prometheus.Labels{},
		logger:              zap.NewNop(),
	}

	// Initialize series file.
	e.sfile = tsdb.NewSeriesFile(c.GetSeriesFilePath(path))
	e.sfile.LargeWriteThreshold = c.TSDB.LargeSeriesWriteThreshold

	// Initialise index.
	e.index = tsi1.NewIndex(e.sfile, c.Index,
		tsi1.WithPath(c.GetIndexPath(path)))

	// Initialize WAL
	e.wal = wal.NewWAL(c.GetWALPath(path))
	e.wal.WithFsyncDelay(time.Duration(c.WAL.FsyncDelay))
	e.wal.SetEnabled(c.WAL.Enabled)

	// Initialise Engine
	e.engine = tsm1.NewEngine(c.GetEnginePath(path), e.index, c.Engine, tsm1.WithSnapshotter(e))

	// Apply options.
	for _, option := range options {
		option(e)
	}

	// Set default metrics labels.
	e.engine.SetDefaultMetricLabels(e.defaultMetricLabels)
	e.sfile.SetDefaultMetricLabels(e.defaultMetricLabels)
	e.index.SetDefaultMetricLabels(e.defaultMetricLabels)
	e.wal.SetDefaultMetricLabels(e.defaultMetricLabels)
	if r, ok := e.retentionEnforcer.(*retentionEnforcer); ok {
		r.SetDefaultMetricLabels(e.defaultMetricLabels)
	}

	return e
}

// WithLogger sets the logger on the Store. It must be called before Open.
func (e *Engine) WithLogger(log *zap.Logger) {
	fields := []zap.Field{}
	if e.nodeID != nil {
		fields = append(fields, zap.Int("node_id", *e.nodeID))
	}

	if e.engineID != nil {
		fields = append(fields, zap.Int("engine_id", *e.engineID))
	}
	fields = append(fields, zap.String("service", "storage-engine"))

	e.logger = log.With(fields...)
	e.sfile.WithLogger(e.logger)
	e.index.WithLogger(e.logger)
	e.engine.WithLogger(e.logger)
	e.wal.WithLogger(e.logger)
	if r, ok := e.retentionEnforcer.(*retentionEnforcer); ok {
		r.WithLogger(e.logger)
	}
}

// PrometheusCollectors returns all the prometheus collectors associated with
// the engine and its components.
func (e *Engine) PrometheusCollectors() []prometheus.Collector {
	var metrics []prometheus.Collector
	metrics = append(metrics, tsdb.PrometheusCollectors()...)
	metrics = append(metrics, tsi1.PrometheusCollectors()...)
	metrics = append(metrics, tsm1.PrometheusCollectors()...)
	metrics = append(metrics, wal.PrometheusCollectors()...)
	metrics = append(metrics, RetentionPrometheusCollectors()...)
	return metrics
}

// Open opens the store and all underlying resources. It returns an error if
// any of the underlying systems fail to open.
func (e *Engine) Open(ctx context.Context) (err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closing != nil {
		return nil // Already open
	}

	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Open the services in order and clean up if any fail.
	var oh openHelper
	oh.Open(ctx, e.sfile)
	oh.Open(ctx, e.index)
	oh.Open(ctx, e.wal)
	oh.Open(ctx, e.engine)
	if err := oh.Done(); err != nil {
		return err
	}

	if err := e.replayWAL(); err != nil {
		return err
	}

	e.closing = make(chan struct{})

	// TODO(edd) background tasks will be run in priority order via a scheduler.
	// For now we will just run on an interval as we only have the retention
	// policy enforcer.
	if e.retentionEnforcer != nil {
		e.runRetentionEnforcer()
	}

	return nil
}

// replayWAL reads the WAL segment files and replays them.
func (e *Engine) replayWAL() error {
	if !e.config.WAL.Enabled {
		return nil
	}
	now := time.Now()

	walPaths, err := wal.SegmentFileNames(e.wal.Path())
	if err != nil {
		return err
	}

	// TODO(jeff): we should just do snapshots and wait for them so that we don't hit
	// OOM situations when reloading huge WALs.

	// Disable the max size during loading
	limit := e.engine.Cache.MaxSize()
	defer func() { e.engine.Cache.SetMaxSize(limit) }()
	e.engine.Cache.SetMaxSize(0)

	// Execute all the entries in the WAL again
	reader := wal.NewWALReader(walPaths)
	reader.WithLogger(e.logger)
	err = reader.Read(func(entry wal.WALEntry) error {
		switch en := entry.(type) {
		case *wal.WriteWALEntry:
			points := tsm1.ValuesToPoints(en.Values)
			err := e.writePointsLocked(context.Background(), tsdb.NewSeriesCollection(points), en.Values)
			if _, ok := err.(tsdb.PartialWriteError); ok {
				err = nil
			}
			return err

		case *wal.DeleteBucketRangeWALEntry:
			var pred tsm1.Predicate
			if len(en.Predicate) > 0 {
				pred, err = tsm1.UnmarshalPredicate(en.Predicate)
				if err != nil {
					return err
				}
			}

			return e.deleteBucketRangeLocked(context.Background(), en.OrgID, en.BucketID, en.Min, en.Max, pred)
		}

		return nil
	})

	e.logger.Info("Reloaded WAL",
		zap.String("path", e.wal.Path()),
		zap.Duration("duration", time.Since(now)),
		zap.Error(err))

	return err
}

// runRetentionEnforcer runs the retention enforcer in a separate goroutine.
//
// Currently this just runs on an interval, but in the future we will add the
// ability to reschedule the retention enforcement if there are not enough
// resources available.
func (e *Engine) runRetentionEnforcer() {
	interval := time.Duration(e.config.RetentionInterval)

	if interval == 0 {
		e.logger.Info("Retention enforcer disabled")
		return // Enforcer disabled.
	} else if interval < 0 {
		e.logger.Error("Negative retention interval", logger.DurationLiteral("check_interval", interval))
		return
	}

	l := e.logger.With(zap.String("component", "retention_enforcer"), logger.DurationLiteral("check_interval", interval))
	l.Info("Starting")

	ticker := time.NewTicker(interval)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			// It's safe to read closing without a lock because it's never
			// modified if this goroutine is active.
			select {
			case <-e.closing:
				l.Info("Stopping")
				return
			case <-ticker.C:
				// canRun will signal to this goroutine that the enforcer can
				// run. It will also carry from the blocking goroutine a function
				// that needs to be called when the enforcer has finished its work.
				canRun := make(chan func())

				// This goroutine blocks until the retention enforcer has permission
				// to proceed.
				go func() {
					if e.retentionEnforcerLimiter != nil {
						// The limiter will block until the enforcer can proceed.
						// The limiter returns a function that needs to be called
						// when the enforcer has finished its work.
						canRun <- e.retentionEnforcerLimiter()
						return
					}
					canRun <- func() {}
				}()

				// Is it possible to get a slot? We need to be able to close
				// whilst waiting...
				select {
				case <-e.closing:
					l.Info("Stopping")
					return
				case done := <-canRun:
					e.retentionEnforcer.run()
					if done != nil {
						done()
					}
				}
			}
		}
	}()
}

// Close closes the store and all underlying resources. It returns an error if
// any of the underlying systems fail to close.
func (e *Engine) Close() error {
	e.mu.RLock()
	if e.closing == nil {
		e.mu.RUnlock()
		// Unusual if an engine is closed more than once, so note it.
		e.logger.Info("Close() called on already-closed engine")
		return nil // Already closed
	}

	close(e.closing)
	e.mu.RUnlock()

	// Wait for any other goroutines to finish.
	e.wg.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()
	e.closing = nil

	var ch closeHelper
	ch.Close(e.engine)
	ch.Close(e.wal)
	ch.Close(e.index)
	ch.Close(e.sfile)
	return ch.Done()
}

// CreateSeriesCursor creates a SeriesCursor for usage with the read service.
func (e *Engine) CreateSeriesCursor(ctx context.Context, req SeriesCursorRequest, cond influxql.Expr) (SeriesCursor, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return nil, ErrEngineClosed
	}

	return newSeriesCursor(req, e.index, e.sfile, cond)
}

// CreateCursorIterator creates a CursorIterator for usage with the read service.
func (e *Engine) CreateCursorIterator(ctx context.Context) (tsdb.CursorIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return nil, ErrEngineClosed
	}
	return e.engine.CreateCursorIterator(ctx)
}

// WritePoints writes the provided points to the engine.
//
// The Engine expects all points to have been correctly validated by the caller.
// However, WritePoints will determine if any tag key-pairs are missing, or if
// there are any field type conflicts.
//
// Appropriate errors are returned in those cases.
func (e *Engine) WritePoints(ctx context.Context, points []models.Point) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	collection, j := tsdb.NewSeriesCollection(points), 0

	// dropPoint should be called whenever there is reason to drop a point from
	// the batch.
	dropPoint := func(key []byte, reason string) {
		if collection.Reason == "" {
			collection.Reason = reason
		}
		collection.Dropped++
		collection.DroppedKeys = append(collection.DroppedKeys, key)
	}

	for iter := collection.Iterator(); iter.Next(); {
		tags := iter.Tags()

		// Not enough tags present.
		if tags.Len() < 2 {
			dropPoint(iter.Key(), fmt.Sprintf("missing required tags: parsed tags: %q", tags))
			continue
		}

		// First tag key is not measurement tag.
		if !bytes.Equal(tags[0].Key, models.MeasurementTagKeyBytes) {
			dropPoint(iter.Key(), fmt.Sprintf("missing required measurement tag as first tag, got: %q", tags[0].Key))
			continue
		}

		fkey, fval := tags[len(tags)-1].Key, tags[len(tags)-1].Value

		// Last tag key is not field tag.
		if !bytes.Equal(fkey, models.FieldKeyTagKeyBytes) {
			dropPoint(iter.Key(), fmt.Sprintf("missing required field key tag as last tag, got: %q", tags[0].Key))
			continue
		}

		// The value representing the underlying field key is invalid if it's "time".
		if bytes.Equal(fval, timeBytes) {
			dropPoint(iter.Key(), fmt.Sprintf("invalid field key: input field %q is invalid", timeBytes))
			continue
		}

		// Filter out any tags with key equal to "time": they are invalid.
		if tags.Get(timeBytes) != nil {
			dropPoint(iter.Key(), fmt.Sprintf("invalid tag key: input tag %q on measurement %q is invalid", timeBytes, iter.Name()))
			continue
		}

		// Drop any point with invalid unicode characters in any of the tag keys or values.
		// This will also cover validating the value used to represent the field key.
		if !models.ValidTagTokens(tags) {
			dropPoint(iter.Key(), fmt.Sprintf("key contains invalid unicode: %q", iter.Key()))
			continue
		}

		collection.Copy(j, iter.Index())
		j++
	}
	collection.Truncate(j)

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closing == nil {
		return ErrEngineClosed
	}

	// Convert the collection to values for adding to the WAL/Cache.
	values, err := tsm1.CollectionToValues(collection)
	if err != nil {
		return err
	}

	// Add the write to the WAL to be replayed if there is a crash or shutdown.
	if _, err := e.wal.WriteMulti(ctx, values); err != nil {
		return err
	}

	return e.writePointsLocked(ctx, collection, values)
}

// writePointsLocked does the work of writing points and must be called under some sort of lock.
func (e *Engine) writePointsLocked(ctx context.Context, collection *tsdb.SeriesCollection, values map[string][]value.Value) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// TODO(jeff): keep track of the values in the collection so that partial write
	// errors get tracked all the way. Right now, the engine doesn't drop any values
	// but if it ever did, the errors could end up missing some data.

	// Add new series to the index and series file.
	if err := e.index.CreateSeriesListIfNotExists(collection); err != nil {
		return err
	}

	// If there was a PartialWriteError, that means the passed in values may contain
	// more than the points so we need to recreate them.
	if collection.PartialWriteError() != nil {
		var err error
		values, err = tsm1.CollectionToValues(collection)
		if err != nil {
			return err
		}
	}

	// Write the values to the engine.
	if err := e.engine.WriteValues(values); err != nil {
		return err
	}

	return collection.PartialWriteError()
}

// AcquireSegments closes the current WAL segment, gets the set of all the currently closed
// segments, and calls the callback. It does all of this under the lock on the engine.
func (e *Engine) AcquireSegments(ctx context.Context, fn func(segs []string) error) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.CloseSegment(); err != nil {
		return err
	}

	segments, err := e.wal.ClosedSegments()
	if err != nil {
		return err
	}

	return fn(segments)
}

// CommitSegments calls the callback and if that does not return an error, removes the segment
// files from the WAL. It does all of this under the lock on the engine.
func (e *Engine) CommitSegments(ctx context.Context, segs []string, fn func() error) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := fn(); err != nil {
		return err
	}

	return e.wal.Remove(ctx, segs)
}

// DeleteBucket deletes an entire bucket from the storage engine.
func (e *Engine) DeleteBucket(ctx context.Context, orgID, bucketID platform.ID) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	return e.DeleteBucketRange(ctx, orgID, bucketID, math.MinInt64, math.MaxInt64)
}

// DeleteBucketRange deletes an entire bucket from the storage engine.
func (e *Engine) DeleteBucketRange(ctx context.Context, orgID, bucketID platform.ID, min, max int64) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return ErrEngineClosed
	}

	// Add the delete to the WAL to be replayed if there is a crash or shutdown.
	if _, err := e.wal.DeleteBucketRange(orgID, bucketID, min, max, nil); err != nil {
		return err
	}

	return e.deleteBucketRangeLocked(ctx, orgID, bucketID, min, max, nil)
}

// DeleteBucketRangePredicate deletes data within a bucket from the storage engine. Any data
// deleted must be in [min, max], and the key must match the predicate if provided.
func (e *Engine) DeleteBucketRangePredicate(ctx context.Context, orgID, bucketID platform.ID, min, max int64, pred platform.Predicate) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return ErrEngineClosed
	}

	var predData []byte
	var err error
	if pred != nil {
		// Marshal the predicate to add it to the WAL.
		predData, err = pred.Marshal()
		if err != nil {
			return err
		}
	}

	// Add the delete to the WAL to be replayed if there is a crash or shutdown.
	if _, err := e.wal.DeleteBucketRange(orgID, bucketID, min, max, predData); err != nil {
		return err
	}

	return e.deleteBucketRangeLocked(ctx, orgID, bucketID, min, max, pred)
}

// deleteBucketRangeLocked does the work of deleting a bucket range and must be called under
// some sort of lock.
func (e *Engine) deleteBucketRangeLocked(ctx context.Context, orgID, bucketID platform.ID, min, max int64, pred tsm1.Predicate) error {
	// TODO(edd): we need to clean up how we're encoding the prefix so that we
	// don't have to remember to get it right everywhere we need to touch TSM data.
	encoded := tsdb.EncodeName(orgID, bucketID)
	name := models.EscapeMeasurement(encoded[:])

	return e.engine.DeletePrefixRange(ctx, name, min, max, pred)
}

// CreateBackup creates a "snapshot" of all TSM data in the Engine.
//   1) Snapshot the cache to ensure the backup includes all data written before now.
//   2) Create hard links to all TSM files, in a new directory within the engine root directory.
//   3) Return a unique backup ID (invalid after the process terminates) and list of files.
func (e *Engine) CreateBackup(ctx context.Context) (int, []string, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if e.closing == nil {
		return 0, nil, ErrEngineClosed
	}

	if err := e.engine.WriteSnapshot(ctx, tsm1.CacheStatusBackup); err != nil {
		return 0, nil, err
	}

	id, snapshotPath, err := e.engine.FileStore.CreateSnapshot(ctx)
	if err != nil {
		return 0, nil, err
	}

	fileInfos, err := ioutil.ReadDir(snapshotPath)
	if err != nil {
		return 0, nil, err
	}
	filenames := make([]string, len(fileInfos))
	for i, fi := range fileInfos {
		filenames[i] = fi.Name()
	}

	return id, filenames, nil
}

// FetchBackupFile writes a given backup file to the provided writer.
// After a successful write, the internal copy is removed.
func (e *Engine) FetchBackupFile(ctx context.Context, backupID int, backupFile string, w io.Writer) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return ErrEngineClosed
	}

	backupPath := e.engine.FileStore.InternalBackupPath(backupID)
	if fi, err := os.Stat(backupPath); err != nil {
		if os.IsNotExist(err) {
			return errors.Errorf("backup %d not found", backupID)
		}
		return errors.WithMessagef(err, "failed to locate backup %d", backupID)
	} else if !fi.IsDir() {
		return errors.Errorf("error in filesystem path of backup %d", backupID)
	}

	backupFileFullPath := filepath.Join(backupPath, backupFile)
	file, err := os.Open(backupFileFullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.Errorf("backup file %d/%s not found", backupID, backupFile)
		}
		return errors.WithMessagef(err, "failed to open backup file %d/%s", backupID, backupFile)
	}
	defer file.Close()

	if _, err = io.Copy(w, file); err != nil {
		err = multierr.Append(err, file.Close())
		return errors.WithMessagef(err, "failed to copy backup file %d/%s to writer", backupID, backupFile)
	}

	if err = file.Close(); err != nil {
		return errors.WithMessagef(err, "failed to close backup file %d/%s", backupID, backupFile)
	}

	if err = os.Remove(backupFileFullPath); err != nil {
		e.logger.Info("Failed to remove backup file after fetch", zap.Error(err), zap.Int("backup_id", backupID), zap.String("backup_file", backupFile))
	}

	return nil
}

// InternalBackupPath provides the internal, full path directory name of the backup.
// This should not be exposed via API.
func (e *Engine) InternalBackupPath(backupID int) string {
	return e.engine.FileStore.InternalBackupPath(backupID)
}

// SeriesCardinality returns the number of series in the engine.
func (e *Engine) SeriesCardinality() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return 0
	}
	return e.index.SeriesN()
}

// Path returns the path of the engine's base directory.
func (e *Engine) Path() string {
	return e.path
}

// ApplyFnToSeriesIDSet allows the caller to apply fn to the SeriesIDSet held
// within the engine's index.
func (e *Engine) ApplyFnToSeriesIDSet(fn func(*tsdb.SeriesIDSet)) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return
	}
	fn(e.index.SeriesIDSet())
}

// MeasurementCardinalityStats returns cardinality stats for all measurements.
func (e *Engine) MeasurementCardinalityStats() (tsi1.MeasurementCardinalityStats, error) {
	return e.index.MeasurementCardinalityStats()
}

// MeasurementStats returns the current measurement stats for the engine.
func (e *Engine) MeasurementStats() (tsm1.MeasurementStats, error) {
	return e.engine.MeasurementStats()
}
