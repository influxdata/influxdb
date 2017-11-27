package tsdb

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

type Index interface {
	Open() error
	Close() error
	WithLogger(*zap.Logger)

	MeasurementExists(name []byte) (bool, error)
	MeasurementNamesByExpr(expr influxql.Expr) ([][]byte, error)
	MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error)
	DropMeasurement(name []byte) error
	ForEachMeasurementName(fn func(name []byte) error) error

	InitializeSeries(key, name []byte, tags models.Tags) error
	CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error
	CreateSeriesListIfNotExists(keys, names [][]byte, tags []models.Tags) error
	DropSeries(key []byte, ts int64) error

	MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error)
	SeriesN() int64

	HasTagKey(name, key []byte) (bool, error)
	TagSets(name []byte, options query.IteratorOptions) ([]*query.TagSet, error)
	MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error)
	MeasurementTagKeyValuesByExpr(auth query.Authorizer, name []byte, keys []string, expr influxql.Expr, keysSorted bool) ([][]string, error)

	ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error
	TagKeyCardinality(name, key []byte) int

	// InfluxQL system iterators
	MeasurementSeriesKeysByExprIterator(name []byte, condition influxql.Expr) (SeriesIDIterator, error)
	MeasurementSeriesKeysByExpr(name []byte, condition influxql.Expr) ([][]byte, error)
	SeriesIDIterator(opt query.IteratorOptions) (SeriesIDIterator, error)

	// Sets a shared fieldset from the engine.
	SetFieldSet(fs *MeasurementFieldSet)

	// Creates hard links inside path for snapshotting.
	SnapshotTo(path string) error

	// To be removed w/ tsi1.
	SetFieldName(measurement []byte, name string)
	AssignShard(k string, shardID uint64)
	UnassignShard(k string, shardID uint64, ts int64) error
	RemoveShard(shardID uint64)

	Type() string

	Rebuild()
}

// SeriesElem represents a generic series element.
type SeriesElem interface {
	Name() []byte
	Tags() models.Tags
	Deleted() bool

	// InfluxQL expression associated with series during filtering.
	Expr() influxql.Expr
}

// SeriesIterator represents a iterator over a list of series.
type SeriesIterator interface {
	Next() (SeriesElem, error)
}

// NewSeriesIteratorAdapter returns an adapter for converting series ids to series.
func NewSeriesIteratorAdapter(sfile *SeriesFile, itr SeriesIDIterator) SeriesIterator {
	return &seriesIteratorAdapter{
		sfile: sfile,
		itr:   itr,
	}
}

type seriesIteratorAdapter struct {
	sfile *SeriesFile
	itr   SeriesIDIterator
}

func (itr *seriesIteratorAdapter) Next() (SeriesElem, error) {
	elem, err := itr.itr.Next()
	if err != nil {
		return nil, err
	} else if elem.SeriesID == 0 {
		return nil, nil
	}

	name, tags := ParseSeriesKey(itr.sfile.SeriesKey(elem.SeriesID))
	deleted := itr.sfile.IsDeleted(elem.SeriesID)

	return &seriesElemAdapter{
		name:    name,
		tags:    tags,
		deleted: deleted,
		expr:    elem.Expr,
	}, nil
}

type seriesElemAdapter struct {
	name    []byte
	tags    models.Tags
	deleted bool
	expr    influxql.Expr
}

func (e *seriesElemAdapter) Name() []byte        { return e.name }
func (e *seriesElemAdapter) Tags() models.Tags   { return e.tags }
func (e *seriesElemAdapter) Deleted() bool       { return e.deleted }
func (e *seriesElemAdapter) Expr() influxql.Expr { return e.expr }

// SeriesIDElem represents a single series and optional expression.
type SeriesIDElem struct {
	SeriesID uint64
	Expr     influxql.Expr
}

// SeriesIDElems represents a list of series id elements.
type SeriesIDElems []SeriesIDElem

func (a SeriesIDElems) Len() int           { return len(a) }
func (a SeriesIDElems) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SeriesIDElems) Less(i, j int) bool { return a[i].SeriesID < a[j].SeriesID }

// SeriesIDIterator represents a iterator over a list of series ids.
type SeriesIDIterator interface {
	Next() (SeriesIDElem, error)
	Close() error
}

// NewSeriesIDSliceIterator returns a SeriesIDIterator that iterates over a slice.
func NewSeriesIDSliceIterator(ids []uint64) *SeriesIDSliceIterator {
	return &SeriesIDSliceIterator{ids: ids}
}

// SeriesIDSliceIterator iterates over a slice of series ids.
type SeriesIDSliceIterator struct {
	ids []uint64
}

// Next returns the next series id in the slice.
func (itr *SeriesIDSliceIterator) Next() (SeriesIDElem, error) {
	if len(itr.ids) == 0 {
		return SeriesIDElem{}, nil
	}
	id := itr.ids[0]
	itr.ids = itr.ids[1:]
	return SeriesIDElem{SeriesID: id}, nil
}

func (itr *SeriesIDSliceIterator) Close() error { return nil }

// seriesQueryAdapterIterator adapts SeriesIDIterator to an influxql.Iterator.
type seriesQueryAdapterIterator struct {
	once     sync.Once
	sfile    *SeriesFile
	itr      SeriesIDIterator
	fieldset *MeasurementFieldSet
	opt      query.IteratorOptions

	point query.FloatPoint // reusable point
}

// NewSeriesQueryAdapterIterator returns a new instance of SeriesQueryAdapterIterator.
func NewSeriesQueryAdapterIterator(sfile *SeriesFile, itr SeriesIDIterator, fieldset *MeasurementFieldSet, opt query.IteratorOptions) query.Iterator {
	return &seriesQueryAdapterIterator{
		sfile:    sfile,
		itr:      itr,
		fieldset: fieldset,
		point: query.FloatPoint{
			Aux: make([]interface{}, len(opt.Aux)),
		},
		opt: opt,
	}
}

// Stats returns stats about the points processed.
func (itr *seriesQueryAdapterIterator) Stats() query.IteratorStats { return query.IteratorStats{} }

// Close closes the iterator.
func (itr *seriesQueryAdapterIterator) Close() error {
	itr.once.Do(func() {
		itr.itr.Close()
	})
	return nil
}

// Next emits the next point in the iterator.
func (itr *seriesQueryAdapterIterator) Next() (*query.FloatPoint, error) {
	for {
		// Read next series element.
		e, err := itr.itr.Next()
		if err != nil {
			return nil, err
		} else if e.SeriesID == 0 {
			return nil, nil
		}

		// Convert to a key.
		name, tags := ParseSeriesKey(itr.sfile.SeriesKey(e.SeriesID))
		key := string(models.MakeKey(name, tags))

		// Write auxiliary fields.
		for i, f := range itr.opt.Aux {
			switch f.Val {
			case "key":
				itr.point.Aux[i] = key
			}
		}
		return &itr.point, nil
	}
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
type NewIndexFunc func(id uint64, database, path string, sfile *SeriesFile, options EngineOptions) Index

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
func NewIndex(id uint64, database, path string, sfile *SeriesFile, options EngineOptions) (Index, error) {
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
	return fn(id, database, path, sfile, options), nil
}

func MustOpenIndex(id uint64, database, path string, sfile *SeriesFile, options EngineOptions) Index {
	idx, err := NewIndex(id, database, path, sfile, options)
	if err != nil {
		panic(err)
	} else if err := idx.Open(); err != nil {
		panic(err)
	}
	return idx
}
