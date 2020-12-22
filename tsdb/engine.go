package tsdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"sort"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

var (
	// ErrFormatNotFound is returned when no format can be determined from a path.
	ErrFormatNotFound = errors.New("format not found")

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

	LoadMetadataIndex(shardID uint64, index Index) error

	CreateSnapshot(skipCacheOk bool) (string, error)
	Backup(w io.Writer, basePath string, since time.Time) error
	Export(w io.Writer, basePath string, start time.Time, end time.Time) error
	Restore(r io.Reader, basePath string) error
	Import(r io.Reader, basePath string) error
	Digest() (io.ReadCloser, int64, error)

	CreateIterator(ctx context.Context, measurement string, opt query.IteratorOptions) (query.Iterator, error)
	CreateCursorIterator(ctx context.Context) (CursorIterator, error)
	IteratorCost(measurement string, opt query.IteratorOptions) (query.IteratorCost, error)
	WritePoints(points []models.Point) error

	CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error
	CreateSeriesListIfNotExists(keys, names [][]byte, tags []models.Tags) error
	DeleteSeriesRange(itr SeriesIterator, min, max int64) error
	DeleteSeriesRangeWithPredicate(itr SeriesIterator, predicate func(name []byte, tags models.Tags) (int64, int64, bool)) error

	MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error)
	SeriesSketches() (estimator.Sketch, estimator.Sketch, error)
	SeriesN() int64

	MeasurementExists(name []byte) (bool, error)

	MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error)
	MeasurementFieldSet() *MeasurementFieldSet
	MeasurementFields(measurement []byte) *MeasurementFields
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

	io.WriterTo
}

// SeriesIDSets provides access to the total set of series IDs
type SeriesIDSets interface {
	ForEach(f func(ids *SeriesIDSet)) error
}

// EngineFormat represents the format for an engine.
type EngineFormat int

const (
	// TSM1Format is the format used by the tsm1 engine.
	TSM1Format EngineFormat = 2
)

// NewEngineFunc creates a new engine.
type NewEngineFunc func(id uint64, i Index, path string, walPath string, sfile *SeriesFile, options EngineOptions) Engine

// newEngineFuncs is a lookup of engine constructors by name.
var newEngineFuncs = make(map[string]NewEngineFunc)

// RegisterEngine registers a storage engine initializer by name.
func RegisterEngine(name string, fn NewEngineFunc) {
	if _, ok := newEngineFuncs[name]; ok {
		panic("engine already registered: " + name)
	}
	newEngineFuncs[name] = fn
}

// RegisteredEngines returns the slice of currently registered engines.
func RegisteredEngines() []string {
	a := make([]string, 0, len(newEngineFuncs))
	for k := range newEngineFuncs {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// NewEngine returns an instance of an engine based on its format.
// If the path does not exist then the DefaultFormat is used.
func NewEngine(id uint64, i Index, path string, walPath string, sfile *SeriesFile, options EngineOptions) (Engine, error) {
	// Create a new engine
	if _, err := os.Stat(path); os.IsNotExist(err) {
		engine := newEngineFuncs[options.EngineVersion](id, i, path, walPath, sfile, options)
		if options.OnNewEngine != nil {
			options.OnNewEngine(engine)
		}
		return engine, nil
	}

	// If it's a dir then it's a tsm1 engine
	format := DefaultEngine
	if fi, err := os.Stat(path); err != nil {
		return nil, err
	} else if !fi.Mode().IsDir() {
		return nil, ErrUnknownEngineFormat
	} else {
		format = "tsm1"
	}

	// Lookup engine by format.
	fn := newEngineFuncs[format]
	if fn == nil {
		return nil, fmt.Errorf("invalid engine format: %q", format)
	}

	engine := fn(id, i, path, walPath, sfile, options)
	if options.OnNewEngine != nil {
		options.OnNewEngine(engine)
	}
	return engine, nil
}

// EngineOptions represents the options used to initialize the engine.
type EngineOptions struct {
	EngineVersion string
	IndexVersion  string
	ShardID       uint64
	InmemIndex    interface{} // shared in-memory index

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

	Config         Config
	SeriesIDSets   SeriesIDSets
	FieldValidator FieldValidator

	OnNewEngine func(Engine)

	FileStoreObserver FileStoreObserver
}

// NewEngineOptions constructs an EngineOptions object with safe default values.
// This should only be used in tests; production environments should read from a config file.
func NewEngineOptions() EngineOptions {
	return EngineOptions{
		EngineVersion: DefaultEngine,
		IndexVersion:  DefaultIndex,
		Config:        NewConfig(),
		WALEnabled:    true,
		OpenLimiter:   limiter.NewFixed(runtime.GOMAXPROCS(0)),
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
