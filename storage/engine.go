package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/tsi1"
	"github.com/influxdata/influxdb/tsdb/tsm1"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Static objects to prevent small allocs.
var timeBytes = []byte("time")

// ErrEngineClosed is returned when a caller attempts to use the engine while
// it's closed.
var ErrEngineClosed = errors.New("engine is closed")

type Engine struct {
	config   Config
	path     string
	engineID *int // Not used by default.
	nodeID   *int // Not used by default.

	mu                sync.RWMutex
	closing           chan struct{} //closing returns the zero value when the engine is shutting down.
	index             *tsi1.Index
	sfile             *tsdb.SeriesFile
	engine            *tsm1.Engine
	wal               *tsm1.WAL
	retentionEnforcer *retentionEnforcer

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
		e.retentionEnforcer = newRetentionEnforcer(e, finder)
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

	// Initialise index.
	e.index = tsi1.NewIndex(e.sfile, c.Index,
		tsi1.WithPath(c.GetIndexPath(path)))

	// Initialize WAL
	var wal tsm1.Log = new(tsm1.NopWAL)
	if c.WAL.Enabled {
		e.wal = tsm1.NewWAL(c.GetWALPath(path))
		e.wal.WithFsyncDelay(time.Duration(c.WAL.FsyncDelay))
		e.wal.EnableTraceLogging(c.TraceLoggingEnabled)
		wal = e.wal
	}

	// Initialise Engine
	e.engine = tsm1.NewEngine(c.GetEnginePath(path), e.index, c.Engine,
		tsm1.WithWAL(wal),
		tsm1.WithTraceLogging(c.TraceLoggingEnabled))

	// Apply options.
	for _, option := range options {
		option(e)
	}
	// Set default metrics labels.
	e.engine.SetDefaultMetricLabels(e.defaultMetricLabels)
	e.sfile.SetDefaultMetricLabels(e.defaultMetricLabels)
	e.index.SetDefaultMetricLabels(e.defaultMetricLabels)

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
	e.retentionEnforcer.WithLogger(e.logger)
}

// PrometheusCollectors returns all the prometheus collectors associated with
// the engine and its components.
func (e *Engine) PrometheusCollectors() []prometheus.Collector {
	var metrics []prometheus.Collector
	metrics = append(metrics, tsdb.PrometheusCollectors()...)
	metrics = append(metrics, tsi1.PrometheusCollectors()...)
	metrics = append(metrics, tsm1.PrometheusCollectors()...)
	metrics = append(metrics, e.retentionEnforcer.PrometheusCollectors()...)
	return metrics
}

// Open opens the store and all underlying resources. It returns an error if
// any of the underlying systems fail to open.
func (e *Engine) Open() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closing != nil {
		return nil // Already open
	}

	if err := e.sfile.Open(); err != nil {
		return err
	}

	if err := e.index.Open(); err != nil {
		return err
	}

	if err := e.engine.Open(); err != nil {
		return err
	}
	e.engine.SetCompactionsEnabled(true) // TODO(edd):is this needed?

	e.closing = make(chan struct{})

	// TODO(edd) background tasks will be run in priority order via a scheduler.
	// For now we will just run on an interval as we only have the retention
	// policy enforcer.
	e.runRetentionEnforcer()

	return nil
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

	if e.retentionEnforcer != nil {
		// Set default metric labels on retention enforcer.
		e.retentionEnforcer.metrics = newRetentionMetrics(e.defaultMetricLabels)
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
				e.retentionEnforcer.run()
			}
		}
	}()
}

// Close closes the store and all underlying resources. It returns an error if
// any of the underlying systems fail to close.
func (e *Engine) Close() error {
	e.mu.RLock()
	if e.closing == nil {
		return nil // Already closed
	}

	close(e.closing)
	e.mu.RUnlock()

	// Wait for any other goroutines to finish.
	e.wg.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()
	e.closing = nil

	if err := e.sfile.Close(); err != nil {
		return err
	}

	if err := e.index.Close(); err != nil {
		return err
	}

	return e.engine.Close()
}

func (e *Engine) CreateSeriesCursor(ctx context.Context, req SeriesCursorRequest, cond influxql.Expr) (SeriesCursor, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return nil, ErrEngineClosed
	}
	return newSeriesCursor(req, e.index, cond)
}

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
// WritePoints will however determine if there are any field type conflicts, and
// return an appropriate error in that case.
func (e *Engine) WritePoints(points []models.Point) error {
	collection := tsdb.NewSeriesCollection(points)

	j := 0
	for iter := collection.Iterator(); iter.Next(); {
		tags := iter.Tags()

		if tags.Len() > 0 && bytes.Equal(tags[0].Key, tsdb.FieldKeyTagKeyBytes) && bytes.Equal(tags[0].Value, timeBytes) {
			// Field key "time" is invalid
			if collection.Reason == "" {
				collection.Reason = fmt.Sprintf("invalid field key: input field %q is invalid", timeBytes)
			}
			collection.Dropped++
			collection.DroppedKeys = append(collection.DroppedKeys, iter.Key())
			continue
		}

		// Filter out any tags with key equal to "time": they are invalid.
		if tags.Get(timeBytes) != nil {
			if collection.Reason == "" {
				collection.Reason = fmt.Sprintf("invalid tag key: input tag %q on measurement %q is invalid", timeBytes, iter.Name())
			}
			collection.Dropped++
			collection.DroppedKeys = append(collection.DroppedKeys, iter.Key())
			continue
		}

		// Drop any series with invalid unicode characters in the key.
		if e.config.ValidateKeys && !models.ValidKeyTokens(string(iter.Name()), tags) {
			if collection.Reason == "" {
				collection.Reason = fmt.Sprintf("key contains invalid unicode: %q", iter.Key())
			}
			collection.Dropped++
			collection.DroppedKeys = append(collection.DroppedKeys, iter.Key())
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

	// Add new series to the index and series file. Check for partial writes.
	if err := e.index.CreateSeriesListIfNotExists(collection); err != nil {
		// ignore PartialWriteErrors. The collection captures it.
		// TODO(edd/jeff): should we just remove PartialWriteError from the index then?
		if _, ok := err.(tsdb.PartialWriteError); !ok {
			return err
		}
	}

	// Write the points to the cache and WAL.
	if err := e.engine.WritePoints(collection.Points); err != nil {
		return err
	}
	return collection.PartialWriteError()
}

// DeleteBucket deletes an entire bucket from the storage engine.
func (e *Engine) DeleteBucket(orgID, bucketID platform.ID) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return ErrEngineClosed
	}

	// TODO(edd): we need to clean up how we're encoding the prefix so that we
	// don't have to remember to get it right everywhere we need to touch TSM data.
	encoded := tsdb.EncodeName(orgID, bucketID)
	name := models.EscapeMeasurement(encoded[:])

	return e.engine.DeleteBucket(name, math.MinInt64, math.MaxInt64)
}

// DeleteSeriesRangeWithPredicate deletes all series data iterated over if fn returns
// true for that series.
func (e *Engine) DeleteSeriesRangeWithPredicate(itr tsdb.SeriesIterator, fn func([]byte, models.Tags) (int64, int64, bool)) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return ErrEngineClosed
	}
	return e.engine.DeleteSeriesRangeWithPredicate(itr, fn)
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
func (e *Engine) MeasurementCardinalityStats() tsi1.MeasurementCardinalityStats {
	return e.index.MeasurementCardinalityStats()
}

// MeasurementStats returns the current measurement stats for the engine.
func (e *Engine) MeasurementStats() (tsm1.MeasurementStats, error) {
	return e.engine.MeasurementStats()
}
