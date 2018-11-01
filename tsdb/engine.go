package tsdb

import (
	"context"
	"errors"
	"regexp"
	"runtime"
	"time"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/pkg/limiter"
	"go.uber.org/zap"
)

var (
	// ErrUnknownEngineFormat is returned when the engine format is
	// unknown. ErrUnknownEngineFormat is currently returned if a format
	// other than tsm1 is encountered.
	ErrUnknownEngineFormat = errors.New("unknown engine format")
)

// Engine represents a swappable storage engine for the shard.
type Engine interface {
	Open() error
	Close() error
	SetEnabled(enabled bool)
	SetCompactionsEnabled(enabled bool)
	ScheduleFullCompaction() error

	WithLogger(*zap.Logger)

	CreateCursorIterator(ctx context.Context) (CursorIterator, error)
	WritePoints(points []models.Point) error

	CreateSeriesIfNotExists(key, name []byte, tags models.Tags, typ models.FieldType) error
	CreateSeriesListIfNotExists(collection *SeriesCollection) error
	DeleteSeriesRange(itr SeriesIterator, min, max int64) error
	DeleteSeriesRangeWithPredicate(itr SeriesIterator, predicate func(name []byte, tags models.Tags) (int64, int64, bool)) error

	SeriesN() int64

	MeasurementExists(name []byte) (bool, error)

	MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error)
	ForEachMeasurementName(fn func(name []byte) error) error
	DeleteMeasurement(name []byte) error

	HasTagKey(name, key []byte) (bool, error)
	MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error)
	TagKeyCardinality(name, key []byte) int

	// Statistics will return statistics relevant to this engine.
	Statistics(tags map[string]string) []models.Statistic
	LastModified() time.Time
	DiskSize() int64
	IsIdle() bool
	Free() error
}

// SeriesIDSets provides access to the total set of series IDs
type SeriesIDSets interface {
	ForEach(f func(ids *SeriesIDSet)) error
}

// EngineOptions represents the options used to initialize the engine.
type EngineOptions struct {
	EngineVersion string
	ShardID       uint64

	// Limits the concurrent number of TSM files that can be loaded at once.
	OpenLimiter limiter.Fixed

	// CompactionDisabled specifies shards should not schedule compactions.
	// This option is intended for offline tooling.
	CompactionDisabled          bool
	CompactionPlannerCreator    CompactionPlannerCreator
	CompactionLimiter           limiter.Fixed
	CompactionThroughputLimiter limiter.Rate
	WALEnabled                  bool
	MonitorDisabled             bool

	// DatabaseFilter is a predicate controlling which databases may be opened.
	// If no function is set, all databases will be opened.
	DatabaseFilter func(database string) bool

	// RetentionPolicyFilter is a predicate controlling which combination of database and retention policy may be opened.
	// nil will allow all combinations to pass.
	RetentionPolicyFilter func(database, rp string) bool

	// ShardFilter is a predicate controlling which combination of database, retention policy and shard group may be opened.
	// nil will allow all combinations to pass.
	ShardFilter func(database, rp string, id uint64) bool

	Config       Config
	SeriesIDSets SeriesIDSets

	OnNewEngine func(Engine)

	FileStoreObserver FileStoreObserver
}

// NewEngineOptions constructs an EngineOptions object with safe default values.
// This should only be used in tests; production environments should read from a config file.
func NewEngineOptions() EngineOptions {
	return EngineOptions{
		EngineVersion:     DefaultEngine,
		Config:            NewConfig(),
		OpenLimiter:       limiter.NewFixed(runtime.GOMAXPROCS(0)),
		CompactionLimiter: limiter.NewFixed(1),
		WALEnabled:        false,
	}
}

// NewInmemIndex returns a new "inmem" index type.
var NewInmemIndex func(name string, sfile *SeriesFile) (interface{}, error)

type CompactionPlannerCreator func(cfg Config) interface{}

// FileStoreObserver is passed notifications before the file store adds or deletes files. In this way, it can
// be sure to observe every file that is added or removed even in the presence of process death.
type FileStoreObserver interface {
	// FileFinishing is called before a file is renamed to it's final name.
	FileFinishing(path string) error

	// FileUnlinking is called before a file is unlinked.
	FileUnlinking(path string) error
}
