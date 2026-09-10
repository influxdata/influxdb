package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/fs"
	"github.com/influxdata/influxdb/v2/tsdb"
	_ "github.com/influxdata/influxdb/v2/tsdb/engine"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
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

	// restoreMu serializes RestoreBucket calls.
	restoreMu sync.Mutex
	// Bucket replaces staged by RestoreBucket, waiting for shard uploads.
	stagedMu       sync.Mutex
	stagedReplaces map[platform.ID]*stagedBucketReplace
	stagedShards   map[uint64]*stagedBucketReplace
	// pendingBucketUpdates: updates owed by replaces that committed in a
	// previous run, applied through bucketService.
	pendingBucketUpdates map[platform.ID]influxdb.RestoredBucketUpdate
	bucketService        influxdb.BucketService

	writePointsValidationEnabled bool

	logger          *zap.Logger
	metricsDisabled bool
}

// Option provides a set
type Option func(*Engine)

func WithMetaClient(c MetaClient) Option {
	return func(e *Engine) {
		e.metaClient = c
	}
}

func WithMetricsDisabled(m bool) Option {
	return func(e *Engine) {
		e.metricsDisabled = m
	}
}

type MetaClient interface {
	CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
	DropDatabase(name string) error
	CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	Database(name string) (di *meta.DatabaseInfo)
	DropShard(id uint64) error
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
	UpdateData(update func(data *meta.Data) error) error
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
	SeriesCardinalityFromShards(ctx context.Context, shards []*tsdb.Shard) (*tsdb.SeriesIDSet, error)
	SeriesFile(database string) *tsdb.SeriesFile
}

// NewEngine initialises a new storage engine, including a series file, index and
// TSM engine.
func NewEngine(path string, c Config, options ...Option) *Engine {
	c.Data.Dir = filepath.Join(path, "data")
	c.Data.WALDir = filepath.Join(path, "wal")

	e := &Engine{
		config:    c,
		path:      path,
		tsdbStore: tsdb.NewStore(c.Data.Dir),
		logger:    zap.NewNop(),

		stagedReplaces:       make(map[platform.ID]*stagedBucketReplace),
		stagedShards:         make(map[uint64]*stagedBucketReplace),
		pendingBucketUpdates: make(map[platform.ID]influxdb.RestoredBucketUpdate),

		writePointsValidationEnabled: true,
	}

	for _, opt := range options {
		opt(e)
	}

	e.tsdbStore.EngineOptions.Config = c.Data

	// Copy TSDB configuration.
	e.tsdbStore.EngineOptions.EngineVersion = c.Data.Engine
	e.tsdbStore.EngineOptions.IndexVersion = c.Data.Index
	e.tsdbStore.EngineOptions.MetricsDisabled = e.metricsDisabled

	pw := coordinator.NewPointsWriter(c.WriteTimeout, path)
	pw.TSDBStore = e.tsdbStore
	pw.MetaClient = e.metaClient
	e.pointsWriter = pw

	e.retentionService = retention.NewService(c.RetentionService)
	e.retentionService.TSDBStore = e.tsdbStore
	e.retentionService.SetOSSMetaClient(e.metaClient)
	e.retentionService.DropShardMetaRef = retention.OSSDropShardMetaRef(e.MetaClient())

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

// ShardLoadingProgressMetrics observes shard loading during tsdb.Store
// startup. Implementations record total shards via AddShard (once per shard
// found before loading begins) and report each completed shard via
// CompletedShard.
type ShardLoadingProgressMetrics interface {
	AddShard()
	CompletedShard()
	ShardLoadFailed(shardID uint64, err error)
}

// WithStartupMetrics wires a shard-loading progress observer into the
// underlying tsdb.Store. Callers that want to surface progress (e.g. as a
// /ready check) construct and own the observer; the Engine just forwards
// the hook.
func (e *Engine) WithStartupMetrics(sp ShardLoadingProgressMetrics) {
	e.tsdbStore.WithStartupMetrics(sp)
}

// PrometheusCollectors returns all the prometheus collectors associated with
// the engine and its components.
func (e *Engine) PrometheusCollectors() []prometheus.Collector {
	var metrics []prometheus.Collector
	metrics = append(metrics, tsm1.PrometheusCollectors()...)
	metrics = append(metrics, coordinator.PrometheusCollectors()...)
	metrics = append(metrics, tsdb.ShardCollectors()...)
	metrics = append(metrics, tsdb.BucketCollectors()...)
	metrics = append(metrics, retention.PrometheusCollectors()...)
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

	if err := e.tsdbStore.Open(ctx); err != nil {
		return err
	}

	e.cleanupStagedShards()

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
	// Any staged replace of the bucket goes with it; DeleteDatabase removes
	// its shard files along with the rest. The manifest keeps them until
	// then, so a failure here is retried at the next startup.
	dropped := e.dropStagedReplaces(func(st *stagedBucketReplace) bool { return st.bucketID == bucketID }, false)
	if err := e.tsdbStore.DeleteDatabase(bucketID.String()); err != nil {
		return err
	}
	e.stagedMu.Lock()
	_, pending := e.pendingBucketUpdates[bucketID]
	delete(e.pendingBucketUpdates, bucketID)
	if pending || len(dropped) > 0 {
		if err := e.writeStagedManifestLocked(); err != nil {
			e.logger.Warn("Failed to rewrite staged restore manifest", zap.Error(err))
		}
	}
	e.stagedMu.Unlock()
	return e.metaClient.DropDatabase(bucketID.String())
}

// DeleteBucketRangePredicate deletes data within a bucket from the storage engine. Any data
// deleted must be in [min, max], and the key must match the predicate if provided.
func (e *Engine) DeleteBucketRangePredicate(ctx context.Context, orgID, bucketID platform.ID, min, max int64, pred influxdb.Predicate, measurement influxql.Expr) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closing == nil {
		return ErrEngineClosed
	}
	// The store would also apply the delete to the not-yet-committed
	// restored shards, which are registered under the same database.
	if e.hasUncommittedReplace(bucketID) {
		return &errors2.Error{
			Code: errors2.EConflict,
			Msg:  "bucket has a restore in progress; retry once it completes",
		}
	}
	return e.tsdbStore.DeleteSeriesWithPredicate(ctx, bucketID.String(), min, max, pred, measurement)
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

	e.restoreMu.Lock()
	defer e.restoreMu.Unlock()

	// The restored metadata reallocates shard IDs, so a replace staged
	// against the old metadata must never commit against the new one. The
	// manifest stays until the shard files are gone, in case this fails.
	dropped := e.dropStagedReplaces(func(*stagedBucketReplace) bool { return true }, false)

	// Replace KV store data and remove all existing shard data.
	if err := e.metaClient.Restore(ctx, r); err != nil {
		e.restageReplaces(dropped)
		return err
	} else if err := e.tsdbStore.DeleteShards(); err != nil {
		return err
	}
	e.stagedMu.Lock()
	e.pendingBucketUpdates = make(map[platform.ID]influxdb.RestoredBucketUpdate)
	if err := e.writeStagedManifestLocked(); err != nil {
		e.logger.Warn("Failed to rewrite staged restore manifest", zap.Error(err))
	}
	e.stagedMu.Unlock()

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

// RestoreBucket restores a bucket's shard metadata. When replace is true the
// bucket's previous contents are replaced once the restore commits, which for
// a staged replace only happens once every restored shard has been uploaded;
// update, if non-nil, is then applied to the bucket through the bucket
// service. The update is persisted with the staged replace, so a restart
// before it lands applies it at the next startup.
func (e *Engine) RestoreBucket(ctx context.Context, id platform.ID, buf []byte, replace bool, update *influxdb.RestoredBucketUpdate) (map[uint64]uint64, error) {
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
	if len(newDBI.RetentionPolicies) != 1 {
		return nil, fmt.Errorf("bucket must have 1 retention policy; attempting to restore %d retention policies", len(newDBI.RetentionPolicies))
	}
	rpi := newDBI.RetentionPolicies[0]

	// One restore at a time: a concurrent replace of the same bucket would
	// otherwise delete the shards created below while they are still being
	// created.
	e.restoreMu.Lock()
	defer e.restoreMu.Unlock()

	// Reserve the new shard group and shard IDs in their own commit, so IDs
	// handed out by concurrent shard-group creation cannot collide with the
	// shards created below. A bucket that is not being replaced is empty, so
	// its metadata can be swapped in the same commit; a missing shard file is
	// then recreated on the first write.
	shardIDMap := make(map[uint64]uint64)
	if err := e.metaClient.UpdateData(func(data *meta.Data) error {
		dbi := data.Database(id.String())
		if dbi == nil {
			return fmt.Errorf("bucket dbi for %q not found during restore", newDBI.Name)
		}

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

		if !replace {
			dbi.RetentionPolicies = newDBI.RetentionPolicies
			dbi.ContinuousQueries = newDBI.ContinuousQueries
		}
		return nil
	}); err != nil {
		return nil, err
	}

	var newShardIDs []uint64
	for _, sgi := range rpi.ShardGroups {
		if sgi.Deleted() {
			continue
		}
		for _, sh := range sgi.Shards {
			newShardIDs = append(newShardIDs, sh.ID)
		}
	}

	// createShards returns every shard to clean up on failure, including
	// the one whose creation failed: it may have left files behind.
	createShards := func() ([]uint64, error) {
		for i, sid := range newShardIDs {
			if err := e.tsdbStore.CreateShard(ctx, id.String(), rpi.Name, sid, true); err != nil {
				return newShardIDs[:i+1], err
			}
		}
		return nil, nil
	}

	if !replace {
		if created, err := createShards(); err != nil {
			if err2 := e.tsdbStore.DeleteShardsByID(created); err2 != nil {
				e.logger.Warn("Failed to clean up shards after aborted restore",
					zap.Uint64s("shard_ids", created), zap.Error(err2))
			}
			return nil, err
		}
		return shardIDMap, nil
	}

	// A replace stays staged until every shard upload lands; stage before
	// creating shard files so a crash mid-creation is cleaned up at startup.
	staged, err := e.stageBucketReplace(id, newDBI, newShardIDs, update)
	if err != nil {
		return nil, err
	}
	if created, err := createShards(); err != nil {
		// Nothing can be committing: uploads are still pending.
		toDelete := append(created, staged.replacedShardIDs...)
		if err2 := e.tsdbStore.DeleteShardsByID(toDelete); err2 != nil {
			// Keep the staged entry so a rerun or restart retries the cleanup.
			e.logger.Warn("Failed to clean up shards after aborted restore",
				zap.Uint64s("shard_ids", toDelete), zap.Error(err2))
			return nil, err
		}
		// Unstage only after the files are gone; the manifest records them.
		e.unstageBucketReplace(staged)
		return nil, err
	}

	// A replace with nothing to upload commits immediately.
	if len(newShardIDs) == 0 {
		if err := e.finalizeStagedReplace(ctx, staged); err != nil {
			return nil, err
		}
	}
	return shardIDMap, nil
}

// SetBucketService provides the service through which a committed replace
// updates its bucket's description and retention settings, and applies any
// such updates a previous run left behind.
func (e *Engine) SetBucketService(svc influxdb.BucketService) {
	e.stagedMu.Lock()
	e.bucketService = svc
	e.stagedMu.Unlock()
	e.applyPendingBucketUpdates(context.Background())
}

// stagedBucketReplace tracks a bucket replace whose restored shards exist but
// whose metadata swap is deferred until every shard upload completes.
type stagedBucketReplace struct {
	bucketID platform.ID
	newDBI   meta.DatabaseInfo
	shardIDs []uint64

	// mu serializes commit attempts and guards the fields below.
	mu sync.Mutex
	// pending: staged shards still awaiting upload.
	pending map[uint64]struct{}
	// replacedShardIDs: pre-swap shards still to be deleted, recorded before
	// the swap commits. Written under both mu and e.stagedMu, so the
	// manifest writer can read it under e.stagedMu alone; so is update.
	replacedShardIDs []uint64
	// update: bucket settings still to be applied once committed.
	update *influxdb.RestoredBucketUpdate
	// dropped: superseded or cancelled; must not commit.
	dropped bool
	// committed: the metadata swap is done, so shardIDs are live.
	committed atomic.Bool
	// uploads is read-held for the whole of each shard upload, so dropping
	// the replace can wait for them before its shard files are deleted.
	uploads sync.RWMutex
}

// stageBucketReplace records a bucket replace awaiting its shard uploads,
// dropping any earlier staged replace of the same bucket that never finished.
// e.restoreMu must be held.
func (e *Engine) stageBucketReplace(id platform.ID, newDBI meta.DatabaseInfo, shardIDs []uint64, update *influxdb.RestoredBucketUpdate) (*stagedBucketReplace, error) {
	st := &stagedBucketReplace{
		bucketID: id,
		newDBI:   newDBI,
		shardIDs: shardIDs,
		pending:  make(map[uint64]struct{}, len(shardIDs)),
		update:   update,
	}
	for _, sid := range shardIDs {
		st.pending[sid] = struct{}{}
	}

	// The previous entry stays in the manifest, and so survives a crash
	// here, until this replace's own entry overwrites it below.
	for _, prev := range e.dropStagedReplaces(func(p *stagedBucketReplace) bool { return p.bucketID == id }, false) {
		prev.mu.Lock()
		if prev.committed.Load() {
			// Its shards are the bucket's live data now; only whatever it
			// failed to delete after committing is still owed.
			st.replacedShardIDs = append(st.replacedShardIDs, prev.replacedShardIDs...)
		} else if err := e.tsdbStore.DeleteShardsByID(prev.shardIDs); err != nil {
			e.logger.Warn("Failed to delete shards from an abandoned bucket replace",
				zap.String("bucket_id", id.String()), zap.Uint64s("shard_ids", prev.shardIDs), zap.Error(err))
			st.replacedShardIDs = append(st.replacedShardIDs, prev.shardIDs...)
		}
		prev.mu.Unlock()
	}

	e.stagedMu.Lock()
	defer e.stagedMu.Unlock()
	// This replace's own update supersedes one still owed by an earlier run.
	delete(e.pendingBucketUpdates, id)
	for _, sid := range shardIDs {
		e.stagedShards[sid] = st
	}
	e.stagedReplaces[id] = st
	if err := e.writeStagedManifestLocked(); err != nil {
		delete(e.stagedReplaces, id)
		for _, sid := range shardIDs {
			delete(e.stagedShards, sid)
		}
		return nil, fmt.Errorf("failed to persist staged restore manifest: %w", err)
	}

	e.logger.Info("Staged bucket replace; metadata swap deferred until all shard uploads complete",
		zap.String("bucket_id", id.String()), zap.Int("pending_shards", len(shardIDs)))
	return st, nil
}

// unstageBucketReplace drops a staged replace whose shard files are gone,
// unless a newer replace of the same bucket has superseded it.
func (e *Engine) unstageBucketReplace(st *stagedBucketReplace) {
	e.dropStagedReplaces(func(p *stagedBucketReplace) bool { return p == st }, true)
}

// dropStagedReplaces removes every staged replace match accepts from the
// engine's tracking and marks it so an in-flight commit attempt cannot land.
// It waits for any commit attempt already under way. The dropped entries are
// returned; their shard files are left for the caller, as is the manifest
// unless rewriteManifest is set.
func (e *Engine) dropStagedReplaces(match func(*stagedBucketReplace) bool, rewriteManifest bool) []*stagedBucketReplace {
	e.stagedMu.Lock()
	var dropped []*stagedBucketReplace
	for id, st := range e.stagedReplaces {
		if !match(st) {
			continue
		}
		delete(e.stagedReplaces, id)
		for _, sid := range st.shardIDs {
			delete(e.stagedShards, sid)
		}
		dropped = append(dropped, st)
	}
	if len(dropped) > 0 && rewriteManifest {
		if err := e.writeStagedManifestLocked(); err != nil {
			e.logger.Warn("Failed to rewrite staged restore manifest", zap.Error(err))
		}
	}
	e.stagedMu.Unlock()

	// Taken after stagedMu: a commit holds st.mu while briefly taking stagedMu.
	for _, st := range dropped {
		st.mu.Lock()
		st.dropped = true
		st.mu.Unlock()
		// Uploads already under way must finish before the caller deletes
		// the shard files; later ones see dropped and abort.
		st.uploads.Lock()
		st.uploads.Unlock()
	}
	return dropped
}

// restageReplaces puts entries dropStagedReplaces removed back, for a caller
// whose operation failed before it touched their shards.
func (e *Engine) restageReplaces(dropped []*stagedBucketReplace) {
	for _, st := range dropped {
		st.mu.Lock()
		st.dropped = false
		st.mu.Unlock()
	}
	e.stagedMu.Lock()
	defer e.stagedMu.Unlock()
	for _, st := range dropped {
		e.stagedReplaces[st.bucketID] = st
		for _, sid := range st.shardIDs {
			e.stagedShards[sid] = st
		}
	}
}

// hasUncommittedReplace reports whether a staged replace of the bucket has
// shard files registered under it that its metadata does not yet own.
func (e *Engine) hasUncommittedReplace(id platform.ID) bool {
	e.stagedMu.Lock()
	st, ok := e.stagedReplaces[id]
	e.stagedMu.Unlock()
	return ok && !st.committed.Load()
}

// stagedManifestPath returns the file recording staged replaces, so a restart
// can clean up staged shards and finish updates the in-memory maps no longer
// track.
func (e *Engine) stagedManifestPath() string {
	return filepath.Join(e.path, "staged-restores.json")
}

// stagedManifestEntry is one bucket's record in the staged restore manifest.
type stagedManifestEntry struct {
	// ShardIDs: the restored shards; live once Committed.
	ShardIDs []uint64 `json:"shard_ids,omitempty"`
	// ReplacedShardIDs: pre-swap shards still to be deleted.
	ReplacedShardIDs []uint64 `json:"replaced_shard_ids,omitempty"`
	// Committed: the metadata swap has landed.
	Committed bool `json:"committed,omitempty"`
	// Update: bucket settings to apply once committed.
	Update *influxdb.RestoredBucketUpdate `json:"update,omitempty"`
}

// writeStagedManifestLocked persists every staged replace and pending bucket
// update. e.stagedMu must be held.
func (e *Engine) writeStagedManifestLocked() error {
	manifest := make(map[string]stagedManifestEntry, len(e.stagedReplaces)+len(e.pendingBucketUpdates))
	for id, st := range e.stagedReplaces {
		manifest[id.String()] = stagedManifestEntry{
			ShardIDs:         st.shardIDs,
			ReplacedShardIDs: st.replacedShardIDs,
			Committed:        st.committed.Load(),
			Update:           st.update,
		}
	}
	for id, upd := range e.pendingBucketUpdates {
		if _, ok := manifest[id.String()]; ok {
			continue
		}
		upd := upd
		manifest[id.String()] = stagedManifestEntry{Committed: true, Update: &upd}
	}
	return e.writeManifestFile(manifest)
}

// writeManifestFile durably writes the staged manifest, or removes it when empty.
func (e *Engine) writeManifestFile(manifest map[string]stagedManifestEntry) error {
	path := e.stagedManifestPath()
	if len(manifest) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return fs.SyncDir(filepath.Dir(path))
	}

	buf, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := fs.RenameFileWithReplacement(tmp, path); err != nil {
		return err
	}
	return fs.SyncDir(filepath.Dir(path))
}

// cleanupStagedShards deletes manifest-listed shards from replaces that did not
// commit before the process exited; shards the metadata references are kept.
// Bucket updates owed by committed replaces are kept for SetBucketService.
func (e *Engine) cleanupStagedShards() {
	e.stagedMu.Lock()
	defer e.stagedMu.Unlock()

	// Reopening invalidates staged state tracked by a prior open.
	e.stagedReplaces = make(map[platform.ID]*stagedBucketReplace)
	e.stagedShards = make(map[uint64]*stagedBucketReplace)
	e.pendingBucketUpdates = make(map[platform.ID]influxdb.RestoredBucketUpdate)

	path := e.stagedManifestPath()
	buf, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return
	} else if err != nil {
		e.logger.Warn("Failed to read staged restore manifest", zap.Error(err))
		return
	}

	var manifest map[string]stagedManifestEntry
	if err := json.Unmarshal(buf, &manifest); err != nil {
		e.logger.Error("Removing corrupt staged restore manifest; shards from an interrupted restore may remain on disk",
			zap.Error(err))
		if err := os.Remove(path); err != nil {
			e.logger.Warn("Failed to remove staged restore manifest", zap.Error(err))
		}
		return
	}

	inMeta := make(map[uint64]struct{})
	for _, dbi := range e.metaClient.Data().Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				for _, sh := range sgi.Shards {
					inMeta[sh.ID] = struct{}{}
				}
			}
		}
	}

	remaining := make(map[string]stagedManifestEntry)
	for bucketID, entry := range manifest {
		// The swap may have landed before the flag was written; the
		// metadata owning a restored shard proves it. With nothing to
		// restore, the metadata having let go of every replaced shard does.
		committed := entry.Committed
		for _, sid := range entry.ShardIDs {
			if _, ok := inMeta[sid]; ok {
				committed = true
			}
		}
		if !committed && len(entry.ShardIDs) == 0 {
			committed = true
			for _, sid := range entry.ReplacedShardIDs {
				if _, ok := inMeta[sid]; ok {
					committed = false
				}
			}
		}

		var left stagedManifestEntry
		orphaned := make([]uint64, 0, len(entry.ShardIDs)+len(entry.ReplacedShardIDs))
		for _, sid := range append(append([]uint64{}, entry.ShardIDs...), entry.ReplacedShardIDs...) {
			if _, ok := inMeta[sid]; !ok {
				orphaned = append(orphaned, sid)
			}
		}
		if len(orphaned) > 0 {
			e.logger.Info("Deleting shards left behind by a bucket replace interrupted by shutdown",
				zap.String("bucket_id", bucketID), zap.Uint64s("shard_ids", orphaned))
			if err := e.tsdbStore.DeleteShardsByID(orphaned); err != nil {
				e.logger.Warn("Failed to delete shards left behind by an interrupted bucket replace",
					zap.String("bucket_id", bucketID), zap.Uint64s("shard_ids", orphaned), zap.Error(err))
			}
			// Keep entries for shards that still exist (e.g. loaded as bad
			// shards) so the next startup retries.
			badShards := e.tsdbStore.GetBadShardList()
			for _, sid := range orphaned {
				if _, bad := badShards[sid]; bad || e.tsdbStore.Shard(sid) != nil {
					left.ReplacedShardIDs = append(left.ReplacedShardIDs, sid)
				}
			}
			if len(left.ReplacedShardIDs) > 0 {
				e.logger.Warn("Shards left behind by an interrupted bucket replace could not be deleted; will retry next startup",
					zap.String("bucket_id", bucketID), zap.Uint64s("shard_ids", left.ReplacedShardIDs))
			}
		}

		if committed && entry.Update != nil {
			id, err := platform.IDFromString(bucketID)
			if err != nil {
				e.logger.Warn("Ignoring bucket update from staged restore manifest with a bad bucket ID",
					zap.String("bucket_id", bucketID), zap.Error(err))
			} else {
				e.pendingBucketUpdates[*id] = *entry.Update
				left.Committed = true
				left.Update = entry.Update
			}
		}
		if len(left.ReplacedShardIDs) > 0 || left.Update != nil {
			remaining[bucketID] = left
		}
	}

	if err := e.writeManifestFile(remaining); err != nil {
		e.logger.Warn("Failed to rewrite staged restore manifest", zap.Error(err))
	}
}

// applyPendingBucketUpdates applies bucket updates owed by replaces that
// committed in a previous run. One that fails stays owed for the next run.
func (e *Engine) applyPendingBucketUpdates(ctx context.Context) {
	e.stagedMu.Lock()
	svc := e.bucketService
	pending := make(map[platform.ID]influxdb.RestoredBucketUpdate, len(e.pendingBucketUpdates))
	for id, upd := range e.pendingBucketUpdates {
		pending[id] = upd
	}
	e.stagedMu.Unlock()
	if svc == nil {
		return
	}

	for id, upd := range pending {
		if err := applyBucketUpdate(ctx, svc, id, upd); err != nil {
			e.logger.Warn("Failed to apply bucket settings from a restore committed in a previous run; will retry next startup",
				zap.String("bucket_id", id.String()), zap.Error(err))
			continue
		}
		e.logger.Info("Applied bucket settings from a restore committed in a previous run",
			zap.String("bucket_id", id.String()))
		e.stagedMu.Lock()
		delete(e.pendingBucketUpdates, id)
		if err := e.writeStagedManifestLocked(); err != nil {
			e.logger.Warn("Failed to rewrite staged restore manifest", zap.Error(err))
		}
		e.stagedMu.Unlock()
	}
}

// applyBucketUpdate brings the bucket's own settings in line with the backup.
func applyBucketUpdate(ctx context.Context, svc influxdb.BucketService, id platform.ID, upd influxdb.RestoredBucketUpdate) error {
	_, err := svc.UpdateBucket(ctx, id, influxdb.BucketUpdate{
		Description:        &upd.Description,
		RetentionPeriod:    &upd.RetentionPeriod,
		ShardGroupDuration: &upd.ShardGroupDuration,
	})
	return err
}

// completeStagedShard marks a staged shard's upload as done; the last upload
// commits the replace. Once nothing is pending, every further upload of one
// of the replace's shards retries whatever part of the commit failed.
func (e *Engine) completeStagedShard(ctx context.Context, shardID uint64) error {
	e.stagedMu.Lock()
	st, ok := e.stagedShards[shardID]
	if !ok {
		e.stagedMu.Unlock()
		return nil
	}
	e.stagedMu.Unlock()

	st.mu.Lock()
	delete(st.pending, shardID)
	remaining := len(st.pending)
	st.mu.Unlock()
	if remaining > 0 {
		return nil
	}
	return e.finalizeStagedReplace(ctx, st)
}

// finalizeStagedReplace swaps the bucket's metadata to the restored shards,
// deletes the shards it previously owned, and applies the bucket update.
// Each step is retried on the next call if it fails. Nothing holds
// e.stagedMu here, so other uploads proceed during the slow parts.
func (e *Engine) finalizeStagedReplace(ctx context.Context, st *stagedBucketReplace) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.dropped {
		return fmt.Errorf("bucket replace for %q was superseded or cancelled", st.bucketID)
	}

	if !st.committed.Load() {
		if err := e.swapStagedReplace(st); err != nil {
			return err
		}
		st.committed.Store(true)
		e.logger.Info("Bucket replace committed after all shard uploads completed",
			zap.String("bucket_id", st.bucketID.String()))
		// Best effort: startup also infers the commit from the metadata.
		e.stagedMu.Lock()
		if err := e.writeStagedManifestLocked(); err != nil {
			e.logger.Warn("Failed to rewrite staged restore manifest", zap.Error(err))
		}
		e.stagedMu.Unlock()
	}

	if len(st.replacedShardIDs) > 0 {
		if err := e.tsdbStore.DeleteShardsByID(st.replacedShardIDs); err != nil {
			return fmt.Errorf("bucket replace committed but deleting the replaced shards failed; re-upload a shard to retry: %w", err)
		}
		e.setReplacedShardIDs(st, nil)
	}

	if st.update != nil {
		e.stagedMu.Lock()
		svc := e.bucketService
		e.stagedMu.Unlock()
		if svc == nil {
			return fmt.Errorf("bucket replace committed but no bucket service is configured to apply the bucket's settings")
		}
		if err := applyBucketUpdate(ctx, svc, st.bucketID, *st.update); err != nil {
			return fmt.Errorf("bucket replace committed but updating the bucket's settings failed; re-upload a shard to retry: %w", err)
		}
		e.stagedMu.Lock()
		st.update = nil
		e.stagedMu.Unlock()
	}

	e.stagedMu.Lock()
	defer e.stagedMu.Unlock()
	if e.stagedReplaces[st.bucketID] != st {
		return nil
	}
	delete(e.stagedReplaces, st.bucketID)
	for _, sid := range st.shardIDs {
		delete(e.stagedShards, sid)
	}
	// A stale manifest is harmless: startup keeps metadata-referenced shards.
	if err := e.writeStagedManifestLocked(); err != nil {
		e.logger.Warn("Failed to rewrite staged restore manifest", zap.Error(err))
	}
	return nil
}

// swapStagedReplace commits the metadata swap for st, recording the bucket's
// pre-swap shards in the manifest first so a crash after the commit still
// reclaims them. st.mu must be held.
func (e *Engine) swapStagedReplace(st *stagedBucketReplace) error {
	owed := st.replacedShardIDs
	// Shard groups can be created concurrently, so the set persisted before
	// the commit is checked inside it and the commit retried if it moved.
	for attempt := 0; attempt < 5; attempt++ {
		data := e.metaClient.Data()
		dbi := data.Database(st.bucketID.String())
		if dbi == nil {
			return fmt.Errorf("bucket dbi for %q not found during restore", st.newDBI.Name)
		}
		current := bucketShardIDs(dbi)

		e.stagedMu.Lock()
		st.replacedShardIDs = append(append([]uint64{}, owed...), current...)
		err := e.writeStagedManifestLocked()
		e.stagedMu.Unlock()
		if err != nil {
			e.setReplacedShardIDs(st, owed)
			return fmt.Errorf("failed to persist staged restore manifest: %w", err)
		}

		moved := false
		err = e.metaClient.UpdateData(func(data *meta.Data) error {
			dbi := data.Database(st.bucketID.String())
			if dbi == nil {
				return fmt.Errorf("bucket dbi for %q not found during restore", st.newDBI.Name)
			}
			if !sameShardIDs(bucketShardIDs(dbi), current) {
				moved = true
				return nil
			}
			dbi.RetentionPolicies = st.newDBI.RetentionPolicies
			dbi.ContinuousQueries = st.newDBI.ContinuousQueries
			return nil
		})
		if err != nil {
			e.setReplacedShardIDs(st, owed)
			return err
		}
		if !moved {
			return nil
		}
	}
	e.setReplacedShardIDs(st, owed)
	return fmt.Errorf("bucket %q kept gaining shards while its restore was committing", st.bucketID)
}

// setReplacedShardIDs updates the field under e.stagedMu. st.mu must be held.
func (e *Engine) setReplacedShardIDs(st *stagedBucketReplace, ids []uint64) {
	e.stagedMu.Lock()
	st.replacedShardIDs = ids
	e.stagedMu.Unlock()
}

// bucketShardIDs lists every shard the database's metadata currently owns.
func bucketShardIDs(dbi *meta.DatabaseInfo) []uint64 {
	var ids []uint64
	for _, rpi := range dbi.RetentionPolicies {
		for _, sgi := range rpi.ShardGroups {
			for _, sh := range sgi.Shards {
				ids = append(ids, sh.ID)
			}
		}
	}
	return ids
}

func sameShardIDs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[uint64]struct{}, len(a))
	for _, id := range a {
		set[id] = struct{}{}
	}
	for _, id := range b {
		if _, ok := set[id]; !ok {
			return false
		}
	}
	return true
}

func (e *Engine) RestoreShard(ctx context.Context, shardID uint64, r io.Reader) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closing == nil {
		return ErrEngineClosed
	}

	// Hold off any drop of the staged replace this shard belongs to until
	// the upload is done, so its files are not deleted mid-restore.
	e.stagedMu.Lock()
	st := e.stagedShards[shardID]
	e.stagedMu.Unlock()
	if st != nil {
		st.uploads.RLock()
		defer st.uploads.RUnlock()
		st.mu.Lock()
		dropped := st.dropped
		st.mu.Unlock()
		if dropped {
			return fmt.Errorf("bucket replace for %q was superseded or cancelled", st.bucketID)
		}
	}

	if err := e.tsdbStore.RestoreShard(ctx, shardID, r); err != nil {
		return err
	}
	return e.completeStagedShard(ctx, shardID)
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
