package storage

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	_ "github.com/influxdata/influxdb/v2/tsdb/engine"
	_ "github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/influxdata/influxdb/v2/v1/coordinator"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxdb/v2/v1/services/precreator"
	"github.com/influxdata/influxdb/v2/v1/services/retention"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

var (
	// ErrEngineClosed is returned when a caller attempts to use the engine while
	// it's closed.
	ErrEngineClosed = errors.New("engine is closed")

	// ErrNotImplemented is returned for APIs that are temporarily not implemented.
	ErrNotImplemented = errors.New("not implemented")
)

type Engine struct {
	config Config
	path   string

	mu           sync.RWMutex
	closing      chan struct{} // closing returns the zero value when the engine is shutting down.
	tsdbStore    *tsdb.Store
	metaClient   MetaClient
	pointsWriter interface {
		WritePoints(ctx context.Context, database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error
		Close() error
	}

	retentionService  *retention.Service
	precreatorService *precreator.Service

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
	PrecreateShardGroups(now, cutoff time.Time) error
	PruneShardGroups() error
	RetentionPolicy(database, policy string) (*meta.RetentionPolicyInfo, error)
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
	RLock()
	RUnlock()
	Backup(ctx context.Context, w io.Writer) error
	Restore(ctx context.Context, r io.Reader) error
	Data() meta.Data
	SetData(data *meta.Data) error
}

type TSDBStore interface {
	DeleteMeasurement(ctx context.Context, database, name string) error
	DeleteSeries(ctx context.Context, database string, sources []influxql.Source, condition influxql.Expr) error
	MeasurementNames(ctx context.Context, auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error)
	ShardGroup(ids []uint64) tsdb.ShardGroup
	Shards(ids []uint64) []*tsdb.Shard
	TagKeys(ctx context.Context, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValues(ctx context.Context, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
	SeriesCardinality(ctx context.Context, database string) (int64, error)
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

	e.retentionService = retention.NewService(c.RetentionService)
	e.retentionService.TSDBStore = e.tsdbStore
	e.retentionService.MetaClient = e.metaClient

	e.precreatorService = precreator.NewService(c.PrecreatorConfig)
	e.precreatorService.MetaClient = e.metaClient

	return e
}

// WithLogger sets the logger on the Store. It must be called before Open.
func (e *Engine) WithLogger(log *zap.Logger) {
	e.logger = log.With(zap.String("service", "storage-engine"))

	e.tsdbStore.WithLogger(e.logger)
	if pw, ok := e.pointsWriter.(*coordinator.PointsWriter); ok {
		pw.WithLogger(e.logger)
	}

	if e.retentionService != nil {
		e.retentionService.WithLogger(log)
	}

	if e.precreatorService != nil {
		e.precreatorService.WithLogger(log)
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

	if err := e.tsdbStore.Open(ctx); err != nil {
		return err
	}

	if err := e.retentionService.Open(ctx); err != nil {
		return err
	}

	if err := e.precreatorService.Open(ctx); err != nil {
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

	var retErr error
	if err := e.precreatorService.Close(); err != nil {
		retErr = multierr.Append(retErr, fmt.Errorf("error closing shard precreator service: %w", err))
	}

	if err := e.retentionService.Close(); err != nil {
		retErr = multierr.Append(retErr, fmt.Errorf("error closing retention service: %w", err))
	}

	if err := e.tsdbStore.Close(); err != nil {
		retErr = multierr.Append(retErr, fmt.Errorf("error closing TSDB store: %w", err))
	}

	if err := e.pointsWriter.Close(); err != nil {
		retErr = multierr.Append(retErr, fmt.Errorf("error closing points writer: %w", err))
	}
	return retErr
}

// WritePoints writes the provided points to the engine.
//
// The Engine expects all points to have been correctly validated by the caller.
// However, WritePoints will determine if any tag key-pairs are missing, or if
// there are any field type conflicts.
// Rosalie was here lockdown 2020
//
// Appropriate errors are returned in those cases.
func (e *Engine) WritePoints(ctx context.Context, orgID platform.ID, bucketID platform.ID, points []models.Point) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	//TODO - remember to add back unicode validation...

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closing == nil {
		return ErrEngineClosed
	}

	return e.pointsWriter.WritePoints(ctx, bucketID.String(), meta.DefaultRetentionPolicyName, models.ConsistencyLevelAll, &meta.UserInfo{}, points)
}

func (e *Engine) CreateBucket(ctx context.Context, b *influxdb.Bucket) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	spec := meta.RetentionPolicySpec{
		Name:               meta.DefaultRetentionPolicyName,
		Duration:           &b.RetentionPeriod,
		ShardGroupDuration: b.ShardGroupDuration,
	}

	if _, err = e.metaClient.CreateDatabaseWithRetentionPolicy(b.ID.String(), &spec); err != nil {
		return err
	}

	return nil
}

func (e *Engine) UpdateBucketRetentionPolicy(ctx context.Context, bucketID platform.ID, upd *influxdb.BucketUpdate) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	rpu := meta.RetentionPolicyUpdate{
		Duration:           upd.RetentionPeriod,
		ShardGroupDuration: upd.ShardGroupDuration,
	}

	err := e.metaClient.UpdateRetentionPolicy(bucketID.String(), meta.DefaultRetentionPolicyName, &rpu, true)
	if err == meta.ErrIncompatibleDurations {
		err = &errors2.Error{
			Code: errors2.EUnprocessableEntity,
			Msg:  "shard-group duration must also be updated to be smaller than new retention duration",
		}
	}
	return err
}

// DeleteBucket deletes an entire bucket from the storage engine.
func (e *Engine) DeleteBucket(ctx context.Context, orgID, bucketID platform.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	return e.tsdbStore.DeleteDatabase(bucketID.String())
}

// DeleteBucketRangePredicate deletes data within a bucket from the storage engine. Any data
// deleted must be in [min, max], and the key must match the predicate if provided.
func (e *Engine) DeleteBucketRangePredicate(ctx context.Context, orgID, bucketID platform.ID, min, max int64, pred influxdb.Predicate) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return ErrEngineClosed
	}
	return e.tsdbStore.DeleteSeriesWithPredicate(ctx, bucketID.String(), min, max, pred)
}

// RLockKVStore locks the KV store as well as the engine in preparation for doing a backup.
func (e *Engine) RLockKVStore() {
	e.mu.RLock()
	e.metaClient.RLock()
}

// RUnlockKVStore unlocks the KV store & engine, intended to be used after a backup is complete.
func (e *Engine) RUnlockKVStore() {
	e.mu.RUnlock()
	e.metaClient.RUnlock()
}

func (e *Engine) BackupKVStore(ctx context.Context, w io.Writer) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if e.closing == nil {
		return ErrEngineClosed
	}

	return e.metaClient.Backup(ctx, w)
}

func (e *Engine) BackupShard(ctx context.Context, w io.Writer, shardID uint64, since time.Time) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closing == nil {
		return ErrEngineClosed
	}

	return e.tsdbStore.BackupShard(shardID, since, w)
}

func (e *Engine) RestoreKVStore(ctx context.Context, r io.Reader) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closing == nil {
		return ErrEngineClosed
	}

	// Replace KV store data and remove all existing shard data.
	if err := e.metaClient.Restore(ctx, r); err != nil {
		return err
	} else if err := e.tsdbStore.DeleteShards(); err != nil {
		return err
	}

	// Create new shards based on the restored KV data.
	data := e.metaClient.Data()
	for _, dbi := range data.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				if sgi.Deleted() {
					continue
				}

				for _, sh := range sgi.Shards {
					if err := e.tsdbStore.CreateShard(ctx, dbi.Name, rpi.Name, sh.ID, true); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (e *Engine) RestoreBucket(ctx context.Context, id platform.ID, buf []byte) (map[uint64]uint64, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closing == nil {
		return nil, ErrEngineClosed
	}

	var newDBI meta.DatabaseInfo
	if err := newDBI.UnmarshalBinary(buf); err != nil {
		return nil, err
	}

	data := e.metaClient.Data()
	dbi := data.Database(id.String())
	if dbi == nil {
		return nil, fmt.Errorf("bucket dbi for %q not found during restore", newDBI.Name)
	} else if len(newDBI.RetentionPolicies) != 1 {
		return nil, fmt.Errorf("bucket must have 1 retention policy; attempting to restore %d retention policies", len(newDBI.RetentionPolicies))
	}

	dbi.RetentionPolicies = newDBI.RetentionPolicies
	dbi.ContinuousQueries = newDBI.ContinuousQueries

	// Generate shard ID mapping.
	shardIDMap := make(map[uint64]uint64)
	rpi := newDBI.RetentionPolicies[0]
	for j, sgi := range rpi.ShardGroups {
		data.MaxShardGroupID++
		rpi.ShardGroups[j].ID = data.MaxShardGroupID

		for k := range sgi.Shards {
			data.MaxShardID++
			shardIDMap[sgi.Shards[k].ID] = data.MaxShardID
			sgi.Shards[k].ID = data.MaxShardID
			sgi.Shards[k].Owners = []meta.ShardOwner{}
		}
	}

	// Update data.
	if err := e.metaClient.SetData(&data); err != nil {
		return nil, err
	}

	// Create shards.
	for _, sgi := range rpi.ShardGroups {
		if sgi.Deleted() {
			continue
		}

		for _, sh := range sgi.Shards {
			if err := e.tsdbStore.CreateShard(ctx, dbi.Name, rpi.Name, sh.ID, true); err != nil {
				return nil, err
			}
		}
	}

	return shardIDMap, nil
}

func (e *Engine) RestoreShard(ctx context.Context, shardID uint64, r io.Reader) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closing == nil {
		return ErrEngineClosed
	}

	return e.tsdbStore.RestoreShard(ctx, shardID, r)
}

// SeriesCardinality returns the number of series in the engine.
func (e *Engine) SeriesCardinality(ctx context.Context, bucketID platform.ID) int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return 0
	}

	n, err := e.tsdbStore.SeriesCardinality(ctx, bucketID.String())
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
