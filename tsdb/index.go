package tsdb

import (
	"fmt"
	"os"
	"regexp"
	"sort"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"github.com/uber-go/zap"
)

type Index interface {
	Open() error
	Close() error
	WithLogger(zap.Logger)

	MeasurementExists(name []byte) (bool, error)
	MeasurementNamesByExpr(auth query.Authorizer, expr influxql.Expr) ([][]byte, error)
	MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error)
	DropMeasurement(name []byte) error
	ForEachMeasurementName(fn func(name []byte) error) error

	InitializeSeries(key, name []byte, tags models.Tags) error
	CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error
	CreateSeriesListIfNotExists(keys, names [][]byte, tags []models.Tags) error
	DropSeries(key []byte) error

	SeriesSketches() (estimator.Sketch, estimator.Sketch, error)
	MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error)
	SeriesN() int64

	HasTagKey(name, key []byte) (bool, error)
	TagSets(name []byte, options query.IteratorOptions) ([]*query.TagSet, error)
	MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error)
	MeasurementTagKeyValuesByExpr(auth query.Authorizer, name []byte, keys []string, expr influxql.Expr, keysSorted bool) ([][]string, error)
	TagKeyHasAuthorizedSeries(auth query.Authorizer, name []byte, key string) bool

	ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error
	TagKeyCardinality(name, key []byte) int

	// InfluxQL system iterators
	MeasurementSeriesKeysByExpr(name []byte, condition influxql.Expr) ([][]byte, error)
	SeriesPointIterator(opt query.IteratorOptions) (query.Iterator, error)

	// Sets a shared fieldset from the engine.
	SetFieldSet(fs *MeasurementFieldSet)

	// Creates hard links inside path for snapshotting.
	SnapshotTo(path string) error

	// To be removed w/ tsi1.
	SetFieldName(measurement []byte, name string)
	AssignShard(k string, shardID uint64)
	UnassignShard(k string, shardID uint64) error
	RemoveShard(shardID uint64)

	Type() string

	Rebuild()
}

// IndexFormat represents the format for an index.
type IndexFormat int

const (
	// InMemFormat is the format used by the original in-memory shared index.
	InMemFormat IndexFormat = 1

	// TSI1Format is the format used by the tsi1 index.
	TSI1Format IndexFormat = 2
)

// NewIndexFunc creates a new index.
type NewIndexFunc func(id uint64, database, path string, options EngineOptions) Index

// newIndexFuncs is a lookup of index constructors by name.
var newIndexFuncs = make(map[string]NewIndexFunc)

// RegisterIndex registers a storage index initializer by name.
func RegisterIndex(name string, fn NewIndexFunc) {
	if _, ok := newIndexFuncs[name]; ok {
		panic("index already registered: " + name)
	}
	newIndexFuncs[name] = fn
}

// RegisteredIndexs returns the slice of currently registered indexes.
func RegisteredIndexes() []string {
	a := make([]string, 0, len(newIndexFuncs))
	for k := range newIndexFuncs {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// NewIndex returns an instance of an index based on its format.
// If the path does not exist then the DefaultFormat is used.
func NewIndex(id uint64, database, path string, options EngineOptions) (Index, error) {
	format := options.IndexVersion

	// Use default format unless existing directory exists.
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		// nop, use default
	} else if err != nil {
		return nil, err
	} else if err == nil {
		format = "tsi1"
	}

	// Lookup index by format.
	fn := newIndexFuncs[format]
	if fn == nil {
		return nil, fmt.Errorf("invalid index format: %q", format)
	}
	return fn(id, database, path, options), nil
}

func MustOpenIndex(id uint64, database, path string, options EngineOptions) Index {
	idx, err := NewIndex(id, database, path, options)
	if err != nil {
		panic(err)
	} else if err := idx.Open(); err != nil {
		panic(err)
	}
	return idx
}
