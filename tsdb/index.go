package tsdb

import (
	"fmt"
	"os"
	"regexp"
	"sort"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
)

type Index interface {
	Open() error
	Close() error

	Measurement(name []byte) (*Measurement, error)
	MeasurementNamesByExpr(expr influxql.Expr) ([][]byte, error)
	MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error)
	DropMeasurement(name []byte) error
	ForEachMeasurementName(fn func(name []byte) error) error

	CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error
	DropSeries(keys [][]byte) error

	SeriesSketches() (estimator.Sketch, estimator.Sketch, error)
	MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error)
	SeriesN() int64

	Dereference(b []byte)

	TagSets(name []byte, dimensions []string, condition influxql.Expr) ([]*influxql.TagSet, error)
	MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error)
	ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error

	// InfluxQL system iterators
	MeasurementSeriesKeysByExpr(name []byte, condition influxql.Expr) ([][]byte, error)
	ForEachMeasurementSeriesByExpr(name []byte, expr influxql.Expr, fn func(tags models.Tags) error) error
	SeriesPointIterator(opt influxql.IteratorOptions) (influxql.Iterator, error)

	// Sets a shared fieldset from the engine.
	SetFieldSet(fs *MeasurementFieldSet)

	// To be removed w/ tsi1.
	SetFieldName(measurement, name string)
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
type NewIndexFunc func(id uint64, path string, options EngineOptions) Index

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
func NewIndex(id uint64, path string, options EngineOptions) (Index, error) {
	// Create a new index.
	if _, err := os.Stat(path); os.IsNotExist(err) && options.Config.Index != "inmem" {
		return newIndexFuncs[options.IndexVersion](id, path, options), nil
	}

	// Use default format.
	format := options.Config.Index

	// Lookup index by format.
	fn := newIndexFuncs[format]
	if fn == nil {
		return nil, fmt.Errorf("invalid index format: %q", format)
	}

	return fn(id, path, options), nil
}

func MustNewIndex(id uint64, path string, options EngineOptions) Index {
	idx, err := NewIndex(id, path, options)
	if err != nil {
		panic(err)
	}
	return idx
}
