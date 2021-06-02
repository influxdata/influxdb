package launcher

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var _ Engine = (*storage.Engine)(nil)

// Engine defines the time-series storage engine.  Wraps *storage.Engine
// to facilitate testing.
type Engine interface {
	influxdb.DeleteService
	storage.PointsWriter
	storage.EngineSchema
	prom.PrometheusCollector
	influxdb.BackupService
	influxdb.RestoreService

	SeriesCardinality(ctx context.Context, bucketID platform.ID) int64

	TSDBStore() storage.TSDBStore
	MetaClient() storage.MetaClient

	WithLogger(log *zap.Logger)
	Open(context.Context) error
	Close() error
}

var _ Engine = (*TemporaryEngine)(nil)
var _ http.Flusher = (*TemporaryEngine)(nil)

// TemporaryEngine creates a time-series storage engine backed
// by a temporary directory that is removed on Close.
type TemporaryEngine struct {
	path    string
	config  storage.Config
	options []storage.Option

	mu     sync.Mutex
	opened bool

	engine    *storage.Engine
	tsdbStore temporaryTSDBStore

	log *zap.Logger
}

// NewTemporaryEngine creates a new engine that places the storage engine files into
// a temporary directory; used for testing.
func NewTemporaryEngine(c storage.Config, options ...storage.Option) *TemporaryEngine {
	return &TemporaryEngine{
		config:  c,
		options: options,
		log:     zap.NewNop(),
	}
}

// Open creates a temporary directory and opens the engine.
func (t *TemporaryEngine) Open(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.opened {
		return nil
	}

	path, err := ioutil.TempDir("", "e2e")
	if err != nil {
		return err
	}

	t.path = path
	t.engine = storage.NewEngine(path, t.config, t.options...)
	t.engine.WithLogger(t.log)

	if err := t.engine.Open(ctx); err != nil {
		_ = os.RemoveAll(path)
		return err
	}

	t.tsdbStore.TSDBStore = t.engine.TSDBStore()

	t.opened = true
	return nil
}

// Close will remove the directory containing the time-series files.
func (t *TemporaryEngine) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.opened = false
	err := t.engine.Close()
	_ = os.RemoveAll(t.path)
	return err
}

// WritePoints stores points into the storage engine.
func (t *TemporaryEngine) WritePoints(ctx context.Context, orgID platform.ID, bucketID platform.ID, points []models.Point) error {
	return t.engine.WritePoints(ctx, orgID, bucketID, points)
}

// SeriesCardinality returns the number of series in the engine.
func (t *TemporaryEngine) SeriesCardinality(ctx context.Context, bucketID platform.ID) int64 {
	return t.engine.SeriesCardinality(ctx, bucketID)
}

// DeleteBucketRangePredicate will delete a bucket from the range and predicate.
func (t *TemporaryEngine) DeleteBucketRangePredicate(ctx context.Context, orgID, bucketID platform.ID, min, max int64, pred influxdb.Predicate) error {
	return t.engine.DeleteBucketRangePredicate(ctx, orgID, bucketID, min, max, pred)
}

func (t *TemporaryEngine) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	return t.engine.CreateBucket(ctx, b)
}

func (t *TemporaryEngine) UpdateBucketRetentionPolicy(ctx context.Context, bucketID platform.ID, upd *influxdb.BucketUpdate) error {
	return t.engine.UpdateBucketRetentionPolicy(ctx, bucketID, upd)
}

// DeleteBucket deletes a bucket from the time-series data.
func (t *TemporaryEngine) DeleteBucket(ctx context.Context, orgID, bucketID platform.ID) error {
	return t.engine.DeleteBucket(ctx, orgID, bucketID)
}

// WithLogger sets the logger on the engine. It must be called before Open.
func (t *TemporaryEngine) WithLogger(log *zap.Logger) {
	t.log = log.With(zap.String("service", "temporary_engine"))
}

// PrometheusCollectors returns all the prometheus collectors associated with
// the engine and its components.
func (t *TemporaryEngine) PrometheusCollectors() []prometheus.Collector {
	return t.engine.PrometheusCollectors()
}

// Flush will remove the time-series files and re-open the engine.
func (t *TemporaryEngine) Flush(ctx context.Context) {
	if err := t.Close(); err != nil {
		t.log.Fatal("unable to close engine", zap.Error(err))
	}

	if err := t.Open(ctx); err != nil {
		t.log.Fatal("unable to open engine", zap.Error(err))
	}
}

func (t *TemporaryEngine) BackupKVStore(ctx context.Context, w io.Writer) error {
	return t.engine.BackupKVStore(ctx, w)
}

func (t *TemporaryEngine) LockKVStore() {
	t.engine.LockKVStore()
}

func (t *TemporaryEngine) UnlockKVStore() {
	t.engine.UnlockKVStore()
}

func (t *TemporaryEngine) RestoreKVStore(ctx context.Context, r io.Reader) error {
	return t.engine.RestoreKVStore(ctx, r)
}

func (t *TemporaryEngine) RestoreBucket(ctx context.Context, id platform.ID, dbi []byte) (map[uint64]uint64, error) {
	return t.engine.RestoreBucket(ctx, id, dbi)
}

func (t *TemporaryEngine) BackupShard(ctx context.Context, w io.Writer, shardID uint64, since time.Time) error {
	return t.engine.BackupShard(ctx, w, shardID, since)
}

func (t *TemporaryEngine) RestoreShard(ctx context.Context, shardID uint64, r io.Reader) error {
	return t.engine.RestoreShard(ctx, shardID, r)
}

func (t *TemporaryEngine) TSDBStore() storage.TSDBStore {
	return &t.tsdbStore
}

func (t *TemporaryEngine) MetaClient() storage.MetaClient {
	return t.engine.MetaClient()
}

type temporaryTSDBStore struct {
	storage.TSDBStore
}
