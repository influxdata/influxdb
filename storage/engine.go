package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/logger"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/tsdb"
	"github.com/influxdata/platform/tsdb/tsi1"
	"github.com/influxdata/platform/tsdb/tsm1"
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
	retentionEnforcer *retentionEnforcer

	// Tracks all goroutines started by the Engine.
	wg sync.WaitGroup

	logger *zap.Logger
}

// Option provides a set
type Option func(*Engine)

// WithTSMFilenameFormatter sets a function on the underlying tsm1.Engine to specify
// how TSM files are named.
var WithTSMFilenameFormatter = func(fn tsm1.FormatFileNameFunc) Option {
	return func(e *Engine) {
		e.engine.WithFormatFileNameFunc(fn)
	}
}

// WithEngineID sets an engine id, which can be useful for logging when multiple
// engines are in use.
var WithEngineID = func(id int) Option {
	return func(e *Engine) {
		e.engineID = &id
	}
}

// WithNodeID sets a node id on the engine, which can be useful for logging
// when a system has engines running on multiple nodes.
var WithNodeID = func(id int) Option {
	return func(e *Engine) {
		e.nodeID = &id
	}
}

// WithRetentionEnforcer initialises a retention enforcer on the engine.
// WithRetentionEnforcer must be called after other options to ensure that all
// metrics are labelled correctly.
var WithRetentionEnforcer = func(finder BucketFinder) Option {
	return func(e *Engine) {
		e.retentionEnforcer = newRetentionEnforcer(e, finder)

		if e.engineID != nil {
			e.retentionEnforcer.defaultMetricLabels["engine_id"] = fmt.Sprint(*e.engineID)
		}

		if e.nodeID != nil {
			e.retentionEnforcer.defaultMetricLabels["node_id"] = fmt.Sprint(*e.nodeID)
		}

		// As new labels may have been set, set the new metrics on the enforcer.
		e.retentionEnforcer.retentionMetrics = newRetentionMetrics(e.retentionEnforcer.defaultMetricLabels)
	}
}

// NewEngine initialises a new storage engine, including a series file, index and
// TSM engine.
func NewEngine(path string, c Config, options ...Option) *Engine {
	e := &Engine{
		config: c,
		path:   path,
		sfile:  tsdb.NewSeriesFile(filepath.Join(path, tsdb.DefaultSeriesFileDirectory)),
		logger: zap.NewNop(),
	}

	// Initialise index.
	index := tsi1.NewIndex(e.sfile, "remove me", c.Index,
		tsi1.WithPath(filepath.Join(path, tsi1.DefaultIndexDirectoryName)),
	)
	e.index = index

	// Initialise Engine
	// TODO(edd): should just be able to use the config values for data/wal.
	engine := tsm1.NewEngine(0, e.index, filepath.Join(path, "data"), filepath.Join(path, "wal"), e.sfile, c.EngineOptions)

	// TODO(edd): Once the tsdb.Engine abstraction is gone, this won't be needed.
	e.engine = engine.(*tsm1.Engine)

	// Apply options.
	for _, option := range options {
		option(e)
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
		fields = append(fields, zap.Int("engine_id", *e.nodeID))
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
	// TODO(edd): Get prom metrics for TSM.
	// TODO(edd): Get prom metrics for index.
	// TODO(edd): Get prom metrics for series file.
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
	if e.config.RetentionInterval == 0 {
		e.logger.Info("Retention enforcer disabled")
		return // Enforcer disabled.
	}

	if e.config.RetentionInterval < 0 {
		e.logger.Error("Negative retention interval", zap.Int64("interval", e.config.RetentionInterval))
		return
	}

	interval := time.Duration(e.config.RetentionInterval) * time.Second
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
