package storage

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/influxdata/influxdb/v2/v1/tsdb"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Static objects to prevent small allocs.
// var timeBytes = []byte("time")

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
	config Config
	path   string

	mu      sync.RWMutex
	closing chan struct{} // closing returns the zero value when the engine is shutting down.
	store   *tsdb.Store

	retentionEnforcer        runner
	retentionEnforcerLimiter runnable

	defaultMetricLabels prometheus.Labels

	writePointsValidationEnabled bool

	// Tracks all goroutines started by the Engine.
	wg sync.WaitGroup

	logger *zap.Logger
}

// Option provides a set
type Option func(*Engine)

// WithRetentionEnforcer initialises a retention enforcer on the engine.
// WithRetentionEnforcer must be called after other options to ensure that all
// metrics are labelled correctly.
func WithRetentionEnforcer(finder BucketFinder) Option {
	return func(e *Engine) {
		// TODO - change retention enforce to take store
		// e.retentionEnforcer = newRetentionEnforcer(e, e.engine, finder)
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

// WithPageFaultLimiter allows the caller to set the limiter for restricting
// the frequency of page faults.
func WithPageFaultLimiter(limiter *rate.Limiter) Option {
	return func(e *Engine) {
		// TODO no longer needed
		// e.engine.WithPageFaultLimiter(limiter)
		e.index.WithPageFaultLimiter(limiter)
		e.sfile.WithPageFaultLimiter(limiter)
	}
}

// NewEngine initialises a new storage engine, including a series file, index and
// TSM engine.
func NewEngine(path string, c Config, options ...Option) *Engine {
	e := &Engine{
		config:              c,
		path:                path,
		defaultMetricLabels: prometheus.Labels{},
		store:               tsdb.NewStore(path),
		logger:              zap.NewNop(),

		writePointsValidationEnabled: true,
	}

	if r, ok := e.retentionEnforcer.(*retentionEnforcer); ok {
		r.SetDefaultMetricLabels(e.defaultMetricLabels)
	}

	return e
}

// WithLogger sets the logger on the Store. It must be called before Open.
func (e *Engine) WithLogger(log *zap.Logger) {
	fields := []zap.Field{}
	fields = append(fields, zap.String("service", "storage-engine"))
	e.logger = log.With(fields...)

	e.store.Logger = e.logger
	if r, ok := e.retentionEnforcer.(*retentionEnforcer); ok {
		r.WithLogger(e.logger)
	}
}

// PrometheusCollectors returns all the prometheus collectors associated with
// the engine and its components.
func (e *Engine) PrometheusCollectors() []prometheus.Collector {
	var metrics []prometheus.Collector
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

	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := e.store.Open(); err != nil {
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

// EnableCompactions allows the series file, index, & underlying engine to compact.
func (e *Engine) EnableCompactions() {
}

// DisableCompactions disables compactions in the series file, index, & engine.
func (e *Engine) DisableCompactions() {
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

	// TODO - Close tsdb store
	return nil
}

// CreateSeriesCursor creates a SeriesCursor for usage with the read service.
func (e *Engine) CreateSeriesCursor(ctx context.Context, orgID, bucketID influxdb.ID, cond influxql.Expr) (SeriesCursor, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return nil, ErrEngineClosed
	}

	// return newSeriesCursor(orgID, bucketID, e.index, e.sfile, cond)
	return nil, nil
}

// CreateCursorIterator creates a CursorIterator for usage with the read service.
func (e *Engine) CreateCursorIterator(ctx context.Context) (cursors.CursorIterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return nil, ErrEngineClosed
	}

	// return e.engine.CreateCursorIterator(ctx)
	return nil, nil
}

// WritePoints writes the provided points to the engine.
//
// The Engine expects all points to have been correctly validated by the caller.
// However, WritePoints will determine if any tag key-pairs are missing, or if
// there are any field type conflicts.
// Rosalie was here lockdown 2020
//
// Appropriate errors are returned in those cases.
func (e *Engine) WritePoints(ctx context.Context, points []models.Point) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	//TODO - remember to add back unicode validation...
	//TODO - remember to check that there is a _field key / \xff key added.

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closing == nil {
		return ErrEngineClosed
	}

	// TODO - write into store.
	return nil
}

// DeleteBucket deletes an entire bucket from the storage engine.
func (e *Engine) DeleteBucket(ctx context.Context, orgID, bucketID influxdb.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	return e.store.DeleteRetentionPolicy(orgID.String(), bucketID.String())
}

// DeleteBucketRange deletes an entire range of data from the storage engine.
func (e *Engine) DeleteBucketRange(ctx context.Context, orgID, bucketID influxdb.ID, min, max int64) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return ErrEngineClosed
	}

	// TODO(edd): create an influxql.Expr that represents the min and max time...
	return e.store.DeleteSeries(orgID.String(), nil, nil)
}

// DeleteBucketRangePredicate deletes data within a bucket from the storage engine. Any data
// deleted must be in [min, max], and the key must match the predicate if provided.
func (e *Engine) DeleteBucketRangePredicate(ctx context.Context, orgID, bucketID influxdb.ID, min, max int64, pred influxdb.Predicate) error {
	span, _ := tracing.StartSpanFromContext(ctx)
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
	_ = predData

	// TODO - edd convert the predicate into an influxql.Expr
	return e.store.DeleteSeries(orgID.String(), nil, nil)
}

// CreateBackup creates a "snapshot" of all TSM data in the Engine.
//   1) Snapshot the cache to ensure the backup includes all data written before now.
//   2) Create hard links to all TSM files, in a new directory within the engine root directory.
//   3) Return a unique backup ID (invalid after the process terminates) and list of files.
//
// TODO - do we need this?
//
func (e *Engine) CreateBackup(ctx context.Context) (int, []string, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if e.closing == nil {
		return 0, nil, ErrEngineClosed
	}

	return 0, nil, nil
}

// FetchBackupFile writes a given backup file to the provided writer.
// After a successful write, the internal copy is removed.
func (e *Engine) FetchBackupFile(ctx context.Context, backupID int, backupFile string, w io.Writer) error {
	// TODO - need?
	return nil
}

// InternalBackupPath provides the internal, full path directory name of the backup.
// This should not be exposed via API.
func (e *Engine) InternalBackupPath(backupID int) string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return ""
	}
	// TODO - need?
	return ""
}

// SeriesCardinality returns the number of series in the engine.
func (e *Engine) SeriesCardinality() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return 0
	}
	// TODO	- get card from store
	return 0
}

// Path returns the path of the engine's base directory.
func (e *Engine) Path() string {
	return e.path
}
