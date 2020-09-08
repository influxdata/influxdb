package storage

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	_ "github.com/influxdata/influxdb/v2/tsdb/engine"
	_ "github.com/influxdata/influxdb/v2/tsdb/index/inmem"
	_ "github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/influxdata/influxdb/v2/v1/coordinator"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxdb/v2/v1/services/retention"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// ErrEngineClosed is returned when a caller attempts to use the engine while
// it's closed.
var ErrEngineClosed = errors.New("engine is closed")

type Engine struct {
	config Config
	path   string

	mu           sync.RWMutex
	closing      chan struct{} // closing returns the zero value when the engine is shutting down.
	tsdbStore    *tsdb.Store
	metaClient   MetaClient
	pointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error
	}

	retentionService *retention.Service

	defaultMetricLabels prometheus.Labels

	writePointsValidationEnabled bool

	logger *zap.Logger
}

// Option provides a set
type Option func(*Engine)

func WithMetaClient(c MetaClient) Option {
	return func(e *Engine) {
		e.metaClient = c
	}
}

type MetaClient interface {
	CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
	CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	Database(name string) (di *meta.DatabaseInfo)
	Databases() []meta.DatabaseInfo
	DeleteShardGroup(database, policy string, id uint64) error
	PruneShardGroups() error
	RetentionPolicy(database, policy string) (*meta.RetentionPolicyInfo, error)
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
}

type TSDBStore interface {
	MeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error)
	ShardGroup(ids []uint64) tsdb.ShardGroup
	Shards(ids []uint64) []*tsdb.Shard
	TagKeys(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValues(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
}

// NewEngine initialises a new storage engine, including a series file, index and
// TSM engine.
func NewEngine(path string, c Config, options ...Option) *Engine {
	c.Data.Dir = filepath.Join(path, "data")
	c.Data.WALDir = filepath.Join(path, "wal")

	e := &Engine{
		config:              c,
		path:                path,
		defaultMetricLabels: prometheus.Labels{},
		tsdbStore:           tsdb.NewStore(c.Data.Dir),
		logger:              zap.NewNop(),

		writePointsValidationEnabled: true,
	}

	for _, opt := range options {
		opt(e)
	}

	e.tsdbStore.EngineOptions.Config = c.Data

	// Copy TSDB configuration.
	e.tsdbStore.EngineOptions.EngineVersion = c.Data.Engine
	e.tsdbStore.EngineOptions.IndexVersion = c.Data.Index

	pw := coordinator.NewPointsWriter()
	pw.TSDBStore = e.tsdbStore
	pw.MetaClient = e.metaClient
	e.pointsWriter = pw

	e.retentionService = retention.NewService(retention.Config{Enabled: true, CheckInterval: c.RetentionInterval})
	e.retentionService.TSDBStore = e.tsdbStore
	e.retentionService.MetaClient = e.metaClient

	return e
}

// WithLogger sets the logger on the Store. It must be called before Open.
func (e *Engine) WithLogger(log *zap.Logger) {
	fields := []zap.Field{}
	fields = append(fields, zap.String("service", "storage-engine"))
	e.logger = log.With(fields...)

	e.tsdbStore.Logger = e.logger
	if pw, ok := e.pointsWriter.(*coordinator.PointsWriter); ok {
		pw.Logger = e.logger
	}

	if e.retentionService != nil {
		e.retentionService.WithLogger(log)
	}
}

// PrometheusCollectors returns all the prometheus collectors associated with
// the engine and its components.
func (e *Engine) PrometheusCollectors() []prometheus.Collector {
	return nil
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

	if err := e.tsdbStore.Open(); err != nil {
		return err
	}

	if err := e.retentionService.Open(); err != nil {
		return err
	}

	e.closing = make(chan struct{})

	return nil
}

// EnableCompactions allows the series file, index, & underlying engine to compact.
func (e *Engine) EnableCompactions() {
}

// DisableCompactions disables compactions in the series file, index, & engine.
func (e *Engine) DisableCompactions() {
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

	e.mu.Lock()
	defer e.mu.Unlock()
	e.closing = nil

	var retErr *multierror.Error

	if err := e.retentionService.Close(); err != nil {
		retErr = multierror.Append(retErr, fmt.Errorf("error closing retention service: %w", err))
	}

	if err := e.tsdbStore.Close(); err != nil {
		retErr = multierror.Append(retErr, fmt.Errorf("error closing TSDB store: %w", err))
	}

	return retErr.ErrorOrNil()
}

// WritePoints writes the provided points to the engine.
//
// The Engine expects all points to have been correctly validated by the caller.
// However, WritePoints will determine if any tag key-pairs are missing, or if
// there are any field type conflicts.
// Rosalie was here lockdown 2020
//
// Appropriate errors are returned in those cases.
func (e *Engine) WritePoints(ctx context.Context, orgID influxdb.ID, bucketID influxdb.ID, points []models.Point) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	//TODO - remember to add back unicode validation...
	//TODO - remember to check that there is a _field key / \xff key added.

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closing == nil {
		return ErrEngineClosed
	}

	return e.pointsWriter.WritePoints(bucketID.String(), meta.DefaultRetentionPolicyName, models.ConsistencyLevelAll, &meta.UserInfo{}, points)
}

func (e *Engine) CreateBucket(ctx context.Context, b *influxdb.Bucket) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	spec := meta.RetentionPolicySpec{
		Name:     meta.DefaultRetentionPolicyName,
		Duration: &b.RetentionPeriod,
	}

	if _, err = e.metaClient.CreateDatabaseWithRetentionPolicy(b.ID.String(), &spec); err != nil {
		return err
	}

	return nil
}

func (e *Engine) UpdateBucketRetentionPeriod(ctx context.Context, bucketID influxdb.ID, d time.Duration) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	rpu := meta.RetentionPolicyUpdate{
		Duration: &d,
	}
	return e.metaClient.UpdateRetentionPolicy(bucketID.String(), meta.DefaultRetentionPolicyName, &rpu, true)
}

// DeleteBucket deletes an entire bucket from the storage engine.
func (e *Engine) DeleteBucket(ctx context.Context, orgID, bucketID influxdb.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	return e.tsdbStore.DeleteRetentionPolicy(bucketID.String(), meta.DefaultRetentionPolicyName)
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
	return e.tsdbStore.DeleteSeries(bucketID.String(), nil, nil)
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
	return e.tsdbStore.DeleteSeries(bucketID.String(), nil, nil)
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
func (e *Engine) SeriesCardinality(orgID, bucketID influxdb.ID) int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return 0
	}

	n, err := e.tsdbStore.SeriesCardinality(bucketID.String())
	if err != nil {
		return 0
	}
	return n
}

// Path returns the path of the engine's base directory.
func (e *Engine) Path() string {
	return e.path
}

func (e *Engine) TSDBStore() TSDBStore {
	return e.tsdbStore
}

func (e *Engine) MetaClient() MetaClient {
	return e.metaClient
}
