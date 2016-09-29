package tsdb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
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

	SetLogOutput(io.Writer)

	LoadMetadataIndex(shardID uint64, index *DatabaseIndex) error

	CreateSnapshot() (string, error)
	Backup(w io.Writer, basePath string, since time.Time) error
	Restore(r io.Reader, basePath string) error

	CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error)
	WritePoints(points []models.Point) error

	CreateSeries(measurment string, series *Series) (*Series, error)
	DeleteSeriesRange(keys [][]byte, min, max int64) error
	Series(key []byte) (*Series, error)

	SeriesN() (uint64, error)
	SeriesSketches() (estimator.Sketch, estimator.Sketch, error)
	MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error)

	CreateMeasurement(name string) (*Measurement, error)
	DeleteMeasurement(name []byte, seriesKeys [][]byte) error
	Measurement(name []byte) (*Measurement, error)
	Measurements() (Measurements, error)
	MeasurementsByExpr(expr influxql.Expr) (Measurements, bool, error)
	MeasurementsByRegex(re *regexp.Regexp) (Measurements, error)
	MeasurementFields(measurement string) *MeasurementFields

	// Statistics will return statistics relevant to this engine.
	Statistics(tags map[string]string) []models.Statistic

	io.WriterTo
}

// EngineFormat represents the format for an engine.
type EngineFormat int

const (
	// TSM1Format is the format used by the tsm1 engine.
	TSM1Format EngineFormat = 2
)

// NewEngineFunc creates a new engine.
type NewEngineFunc func(path string, walPath string, options EngineOptions) Engine

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
func NewEngine(path string, walPath string, options EngineOptions) (Engine, error) {
	// Create a new engine
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return newEngineFuncs[options.EngineVersion](path, walPath, options), nil
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

	return fn(path, walPath, options), nil
}

// EngineOptions represents the options used to initialize the engine.
type EngineOptions struct {
	EngineVersion string

	Config Config
}

// NewEngineOptions returns the default options.
func NewEngineOptions() EngineOptions {
	return EngineOptions{
		EngineVersion: DefaultEngine,
		Config:        NewConfig(),
	}
}
