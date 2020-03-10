package launcher

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var _ Engine = (*storage.Engine)(nil)

// Engine defines the time-series storage engine.  Wraps *storage.Engine
// to facilitate testing.
type Engine interface {
	influxdb.DeleteService
	reads.Viewer
	storage.PointsWriter
	storage.BucketDeleter
	prom.PrometheusCollector
	influxdb.BackupService

	SeriesCardinality() int64

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

	engine *storage.Engine

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
func (t *TemporaryEngine) WritePoints(ctx context.Context, points []models.Point) error {
	return t.engine.WritePoints(ctx, points)
}

// SeriesCardinality returns the number of series in the engine.
func (t *TemporaryEngine) SeriesCardinality() int64 {
	return t.engine.SeriesCardinality()
}

// DeleteBucketRangePredicate will delete a bucket from the range and predicate.
func (t *TemporaryEngine) DeleteBucketRangePredicate(ctx context.Context, orgID, bucketID influxdb.ID, min, max int64, pred influxdb.Predicate) error {
	return t.engine.DeleteBucketRangePredicate(ctx, orgID, bucketID, min, max, pred)

}

// DeleteBucket deletes a bucket from the time-series data.
func (t *TemporaryEngine) DeleteBucket(ctx context.Context, orgID, bucketID influxdb.ID) error {
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

// CreateCursorIterator calls into the underlying engines CreateCurorIterator.
func (t *TemporaryEngine) CreateCursorIterator(ctx context.Context) (tsdb.CursorIterator, error) {
	return t.engine.CreateCursorIterator(ctx)
}

// CreateSeriesCursor calls into the underlying engines CreateSeriesCursor.
func (t *TemporaryEngine) CreateSeriesCursor(ctx context.Context, orgID, bucketID influxdb.ID, cond influxql.Expr) (storage.SeriesCursor, error) {
	return t.engine.CreateSeriesCursor(ctx, orgID, bucketID, cond)
}

// TagKeys calls into the underlying engines TagKeys.
func (t *TemporaryEngine) TagKeys(ctx context.Context, orgID, bucketID influxdb.ID, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error) {
	return t.engine.TagKeys(ctx, orgID, bucketID, start, end, predicate)
}

// TagValues calls into the underlying engines TagValues.
func (t *TemporaryEngine) TagValues(ctx context.Context, orgID, bucketID influxdb.ID, tagKey string, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error) {
	return t.engine.TagValues(ctx, orgID, bucketID, tagKey, start, end, predicate)
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

func (t *TemporaryEngine) CreateBackup(ctx context.Context) (int, []string, error) {
	return t.engine.CreateBackup(ctx)
}

func (t *TemporaryEngine) FetchBackupFile(ctx context.Context, backupID int, backupFile string, w io.Writer) error {
	return t.engine.FetchBackupFile(ctx, backupID, backupFile, w)
}

func (t *TemporaryEngine) InternalBackupPath(backupID int) string {
	return t.engine.InternalBackupPath(backupID)
}
