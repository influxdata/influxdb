package tsdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bytesutil"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

type Index interface {
	Open() error
	Close() error
	WithLogger(*zap.Logger)

	Database() string
	MeasurementExists(name []byte) (bool, error)
	MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error)
	DropMeasurement(name []byte) error
	ForEachMeasurementName(fn func(name []byte) error) error

	InitializeSeries(keys, names [][]byte, tags []models.Tags) error
	CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error
	CreateSeriesListIfNotExists(keys, names [][]byte, tags []models.Tags) error
	DropSeries(seriesID uint64, key []byte, cascade bool) error
	DropMeasurementIfSeriesNotExist(name []byte) error

	MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error)
	SeriesN() int64
	SeriesSketches() (estimator.Sketch, estimator.Sketch, error)

	HasTagKey(name, key []byte) (bool, error)
	HasTagValue(name, key, value []byte) (bool, error)

	MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error)

	TagKeyCardinality(name, key []byte) int

	// InfluxQL system iterators
	MeasurementIterator() (MeasurementIterator, error)
	TagKeyIterator(name []byte) (TagKeyIterator, error)
	TagValueIterator(name, key []byte) (TagValueIterator, error)
	MeasurementSeriesIDIterator(name []byte) (SeriesIDIterator, error)
	TagKeySeriesIDIterator(name, key []byte) (SeriesIDIterator, error)
	TagValueSeriesIDIterator(name, key, value []byte) (SeriesIDIterator, error)

	// Sets a shared fieldset from the engine.
	FieldSet() *MeasurementFieldSet
	SetFieldSet(fs *MeasurementFieldSet)

	// Size of the index on disk, if applicable.
	DiskSizeBytes() int64

	// To be removed w/ tsi1.
	SetFieldName(measurement []byte, name string)

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
	Close() error
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

func (itr *seriesIteratorAdapter) Close() error { return itr.itr.Close() }

func (itr *seriesIteratorAdapter) Next() (SeriesElem, error) {
	for {
		elem, err := itr.itr.Next()
		if err != nil {
			return nil, err
		} else if elem.SeriesID == 0 {
			return nil, nil
		}

		// Skip if this key has been tombstoned.
		key := itr.sfile.SeriesKey(elem.SeriesID)
		if len(key) == 0 {
			continue
		}

		name, tags := ParseSeriesKey(key)
		deleted := itr.sfile.IsDeleted(elem.SeriesID)

		return &seriesElemAdapter{
			name:    name,
			tags:    tags,
			deleted: deleted,
			expr:    elem.Expr,
		}, nil
	}
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

// ReadAllSeriesIDIterator returns all ids from the iterator.
func ReadAllSeriesIDIterator(itr SeriesIDIterator) ([]uint64, error) {
	if itr == nil {
		return nil, nil
	}

	var a []uint64
	for {
		e, err := itr.Next()
		if err != nil {
			return nil, err
		} else if e.SeriesID == 0 {
			break
		}
		a = append(a, e.SeriesID)
	}
	return a, nil
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

type SeriesIDIterators []SeriesIDIterator

func (a SeriesIDIterators) Close() (err error) {
	for i := range a {
		if e := a[i].Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

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

		// Skip if key has been tombstoned.
		seriesKey := itr.sfile.SeriesKey(e.SeriesID)
		if len(seriesKey) == 0 {
			continue
		}

		// Convert to a key.
		name, tags := ParseSeriesKey(seriesKey)
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

// filterUndeletedSeriesIDIterator returns all series which are not deleted.
type filterUndeletedSeriesIDIterator struct {
	sfile *SeriesFile
	itr   SeriesIDIterator
}

// FilterUndeletedSeriesIDIterator returns an iterator which filters all deleted series.
func FilterUndeletedSeriesIDIterator(sfile *SeriesFile, itr SeriesIDIterator) SeriesIDIterator {
	if itr == nil {
		return nil
	}
	return &filterUndeletedSeriesIDIterator{sfile: sfile, itr: itr}
}

func (itr *filterUndeletedSeriesIDIterator) Close() error {
	return itr.itr.Close()
}

func (itr *filterUndeletedSeriesIDIterator) Next() (SeriesIDElem, error) {
	for {
		e, err := itr.itr.Next()
		if err != nil {
			return SeriesIDElem{}, err
		} else if e.SeriesID == 0 {
			return SeriesIDElem{}, nil
		} else if itr.sfile.IsDeleted(e.SeriesID) {
			continue
		}
		return e, nil
	}
}

// seriesIDExprIterator is an iterator that attaches an associated expression.
type seriesIDExprIterator struct {
	itr  SeriesIDIterator
	expr influxql.Expr
}

// newSeriesIDExprIterator returns a new instance of seriesIDExprIterator.
func newSeriesIDExprIterator(itr SeriesIDIterator, expr influxql.Expr) SeriesIDIterator {
	if itr == nil {
		return nil
	}

	return &seriesIDExprIterator{
		itr:  itr,
		expr: expr,
	}
}

func (itr *seriesIDExprIterator) Close() error {
	return itr.itr.Close()
}

// Next returns the next element in the iterator.
func (itr *seriesIDExprIterator) Next() (SeriesIDElem, error) {
	elem, err := itr.itr.Next()
	if err != nil {
		return SeriesIDElem{}, err
	} else if elem.SeriesID == 0 {
		return SeriesIDElem{}, nil
	}
	elem.Expr = itr.expr
	return elem, nil
}

// MergeSeriesIDIterators returns an iterator that merges a set of iterators.
// Iterators that are first in the list take precendence and a deletion by those
// early iterators will invalidate elements by later iterators.
func MergeSeriesIDIterators(itrs ...SeriesIDIterator) SeriesIDIterator {
	if n := len(itrs); n == 0 {
		return nil
	} else if n == 1 {
		return itrs[0]
	}

	return &seriesIDMergeIterator{
		buf:  make([]SeriesIDElem, len(itrs)),
		itrs: itrs,
	}
}

// seriesIDMergeIterator is an iterator that merges multiple iterators together.
type seriesIDMergeIterator struct {
	buf  []SeriesIDElem
	itrs []SeriesIDIterator
}

func (itr *seriesIDMergeIterator) Close() error {
	SeriesIDIterators(itr.itrs).Close()
	return nil
}

// Next returns the element with the next lowest name/tags across the iterators.
func (itr *seriesIDMergeIterator) Next() (SeriesIDElem, error) {
	// Find next lowest id amongst the buffers.
	var elem SeriesIDElem
	for i := range itr.buf {
		buf := &itr.buf[i]

		// Fill buffer.
		if buf.SeriesID == 0 {
			elem, err := itr.itrs[i].Next()
			if err != nil {
				return SeriesIDElem{}, nil
			} else if elem.SeriesID == 0 {
				continue
			}
			itr.buf[i] = elem
		}

		if elem.SeriesID == 0 || buf.SeriesID < elem.SeriesID {
			elem = *buf
		}
	}

	// Return EOF if no elements remaining.
	if elem.SeriesID == 0 {
		return SeriesIDElem{}, nil
	}

	// Clear matching buffers.
	for i := range itr.buf {
		if itr.buf[i].SeriesID == elem.SeriesID {
			itr.buf[i].SeriesID = 0
		}
	}
	return elem, nil
}

// IntersectSeriesIDIterators returns an iterator that only returns series which
// occur in both iterators. If both series have associated expressions then
// they are combined together.
func IntersectSeriesIDIterators(itr0, itr1 SeriesIDIterator) SeriesIDIterator {
	if itr0 == nil || itr1 == nil {
		if itr0 != nil {
			itr0.Close()
		}
		if itr1 != nil {
			itr1.Close()
		}
		return nil
	}

	return &seriesIDIntersectIterator{itrs: [2]SeriesIDIterator{itr0, itr1}}
}

// seriesIDIntersectIterator is an iterator that merges two iterators together.
type seriesIDIntersectIterator struct {
	buf  [2]SeriesIDElem
	itrs [2]SeriesIDIterator
}

func (itr *seriesIDIntersectIterator) Close() (err error) {
	if e := itr.itrs[0].Close(); e != nil && err == nil {
		err = e
	}
	if e := itr.itrs[1].Close(); e != nil && err == nil {
		err = e
	}
	return err
}

// Next returns the next element which occurs in both iterators.
func (itr *seriesIDIntersectIterator) Next() (_ SeriesIDElem, err error) {
	for {
		// Fill buffers.
		if itr.buf[0].SeriesID == 0 {
			if itr.buf[0], err = itr.itrs[0].Next(); err != nil {
				return SeriesIDElem{}, err
			}
		}
		if itr.buf[1].SeriesID == 0 {
			if itr.buf[1], err = itr.itrs[1].Next(); err != nil {
				return SeriesIDElem{}, err
			}
		}

		// Exit if either buffer is still empty.
		if itr.buf[0].SeriesID == 0 || itr.buf[1].SeriesID == 0 {
			return SeriesIDElem{}, nil
		}

		// Skip if both series are not equal.
		if a, b := itr.buf[0].SeriesID, itr.buf[1].SeriesID; a < b {
			itr.buf[0].SeriesID = 0
			continue
		} else if a > b {
			itr.buf[1].SeriesID = 0
			continue
		}

		// Merge series together if equal.
		elem := itr.buf[0]

		// Attach expression.
		expr0 := itr.buf[0].Expr
		expr1 := itr.buf[1].Expr
		if expr0 == nil {
			elem.Expr = expr1
		} else if expr1 == nil {
			elem.Expr = expr0
		} else {
			elem.Expr = influxql.Reduce(&influxql.BinaryExpr{
				Op:  influxql.AND,
				LHS: expr0,
				RHS: expr1,
			}, nil)
		}

		itr.buf[0].SeriesID, itr.buf[1].SeriesID = 0, 0
		return elem, nil
	}
}

// UnionSeriesIDIterators returns an iterator that returns series from both
// both iterators. If both series have associated expressions then they are
// combined together.
func UnionSeriesIDIterators(itr0, itr1 SeriesIDIterator) SeriesIDIterator {
	// Return other iterator if either one is nil.
	if itr0 == nil {
		return itr1
	} else if itr1 == nil {
		return itr0
	}

	return &seriesIDUnionIterator{itrs: [2]SeriesIDIterator{itr0, itr1}}
}

// seriesIDUnionIterator is an iterator that unions two iterators together.
type seriesIDUnionIterator struct {
	buf  [2]SeriesIDElem
	itrs [2]SeriesIDIterator
}

func (itr *seriesIDUnionIterator) Close() (err error) {
	if e := itr.itrs[0].Close(); e != nil && err == nil {
		err = e
	}
	if e := itr.itrs[1].Close(); e != nil && err == nil {
		err = e
	}
	return err
}

// Next returns the next element which occurs in both iterators.
func (itr *seriesIDUnionIterator) Next() (_ SeriesIDElem, err error) {
	// Fill buffers.
	if itr.buf[0].SeriesID == 0 {
		if itr.buf[0], err = itr.itrs[0].Next(); err != nil {
			return SeriesIDElem{}, err
		}
	}
	if itr.buf[1].SeriesID == 0 {
		if itr.buf[1], err = itr.itrs[1].Next(); err != nil {
			return SeriesIDElem{}, err
		}
	}

	// Return non-zero or lesser series.
	if a, b := itr.buf[0].SeriesID, itr.buf[1].SeriesID; a == 0 && b == 0 {
		return SeriesIDElem{}, nil
	} else if b == 0 || (a != 0 && a < b) {
		elem := itr.buf[0]
		itr.buf[0].SeriesID = 0
		return elem, nil
	} else if a == 0 || (b != 0 && a > b) {
		elem := itr.buf[1]
		itr.buf[1].SeriesID = 0
		return elem, nil
	}

	// Attach element.
	elem := itr.buf[0]

	// Attach expression.
	expr0 := itr.buf[0].Expr
	expr1 := itr.buf[1].Expr
	if expr0 != nil && expr1 != nil {
		elem.Expr = influxql.Reduce(&influxql.BinaryExpr{
			Op:  influxql.OR,
			LHS: expr0,
			RHS: expr1,
		}, nil)
	} else {
		elem.Expr = nil
	}

	itr.buf[0].SeriesID, itr.buf[1].SeriesID = 0, 0
	return elem, nil
}

// DifferenceSeriesIDIterators returns an iterator that only returns series which
// occur the first iterator but not the second iterator.
func DifferenceSeriesIDIterators(itr0, itr1 SeriesIDIterator) SeriesIDIterator {
	if itr0 == nil && itr1 == nil {
		return nil
	} else if itr1 == nil {
		return itr0
	} else if itr0 == nil {
		itr1.Close()
		return nil
	}
	return &seriesIDDifferenceIterator{itrs: [2]SeriesIDIterator{itr0, itr1}}
}

// seriesIDDifferenceIterator is an iterator that merges two iterators together.
type seriesIDDifferenceIterator struct {
	buf  [2]SeriesIDElem
	itrs [2]SeriesIDIterator
}

func (itr *seriesIDDifferenceIterator) Close() (err error) {
	if e := itr.itrs[0].Close(); e != nil && err == nil {
		err = e
	}
	if e := itr.itrs[1].Close(); e != nil && err == nil {
		err = e
	}
	return err
}

// Next returns the next element which occurs only in the first iterator.
func (itr *seriesIDDifferenceIterator) Next() (_ SeriesIDElem, err error) {
	for {
		// Fill buffers.
		if itr.buf[0].SeriesID == 0 {
			if itr.buf[0], err = itr.itrs[0].Next(); err != nil {
				return SeriesIDElem{}, err
			}
		}
		if itr.buf[1].SeriesID == 0 {
			if itr.buf[1], err = itr.itrs[1].Next(); err != nil {
				return SeriesIDElem{}, err
			}
		}

		// Exit if first buffer is still empty.
		if itr.buf[0].SeriesID == 0 {
			return SeriesIDElem{}, nil
		} else if itr.buf[1].SeriesID == 0 {
			elem := itr.buf[0]
			itr.buf[0].SeriesID = 0
			return elem, nil
		}

		// Return first series if it's less.
		// If second series is less then skip it.
		// If both series are equal then skip both.
		if a, b := itr.buf[0].SeriesID, itr.buf[1].SeriesID; a < b {
			elem := itr.buf[0]
			itr.buf[0].SeriesID = 0
			return elem, nil
		} else if a > b {
			itr.buf[1].SeriesID = 0
			continue
		} else {
			itr.buf[0].SeriesID, itr.buf[1].SeriesID = 0, 0
			continue
		}
	}
}

// seriesPointIterator adapts SeriesIterator to an influxql.Iterator.
type seriesPointIterator struct {
	once     sync.Once
	indexSet IndexSet
	fieldset *MeasurementFieldSet
	mitr     MeasurementIterator
	keys     [][]byte
	opt      query.IteratorOptions

	point query.FloatPoint // reusable point
}

// newSeriesPointIterator returns a new instance of seriesPointIterator.
func NewSeriesPointIterator(indexSet IndexSet, fieldset *MeasurementFieldSet, opt query.IteratorOptions) (_ query.Iterator, err error) {
	// Only equality operators are allowed.
	influxql.WalkFunc(opt.Condition, func(n influxql.Node) {
		switch n := n.(type) {
		case *influxql.BinaryExpr:
			switch n.Op {
			case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX,
				influxql.OR, influxql.AND:
			default:
				err = errors.New("invalid tag comparison operator")
			}
		}
	})
	if err != nil {
		return nil, err
	}

	mitr, err := indexSet.MeasurementIterator()
	if err != nil {
		return nil, err
	}

	return &seriesPointIterator{
		indexSet: indexSet,
		fieldset: fieldset,
		mitr:     mitr,
		point: query.FloatPoint{
			Aux: make([]interface{}, len(opt.Aux)),
		},
		opt: opt,
	}, nil
}

// Stats returns stats about the points processed.
func (itr *seriesPointIterator) Stats() query.IteratorStats { return query.IteratorStats{} }

// Close closes the iterator.
func (itr *seriesPointIterator) Close() (err error) {
	itr.once.Do(func() {
		if itr.mitr != nil {
			err = itr.mitr.Close()
		}
	})
	return err
}

// Next emits the next point in the iterator.
func (itr *seriesPointIterator) Next() (*query.FloatPoint, error) {
	for {
		// Read series keys for next measurement if no more keys remaining.
		// Exit if there are no measurements remaining.
		if len(itr.keys) == 0 {
			m, err := itr.mitr.Next()
			if err != nil {
				return nil, err
			} else if m == nil {
				return nil, nil
			}

			if err := itr.readSeriesKeys(m); err != nil {
				return nil, err
			}
			continue
		}

		name, tags := ParseSeriesKey(itr.keys[0])
		itr.keys = itr.keys[1:]

		// TODO(edd): It seems to me like this authorisation check should be
		// further down in the index. At this point we're going to be filtering
		// series that have already been materialised in the LogFiles and
		// IndexFiles.
		if itr.opt.Authorizer != nil && !itr.opt.Authorizer.AuthorizeSeriesRead(itr.indexSet.Database(), name, tags) {
			continue
		}

		// Convert to a key.
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

func (itr *seriesPointIterator) readSeriesKeys(name []byte) error {
	sitr, err := itr.indexSet.MeasurementSeriesByExprIterator(name, itr.opt.Condition)
	if err != nil {
		return err
	} else if sitr == nil {
		return nil
	}
	defer sitr.Close()

	// Slurp all series keys.
	itr.keys = itr.keys[:0]
	for {
		elem, err := sitr.Next()
		if err != nil {
			return err
		} else if elem.SeriesID == 0 {
			break
		}

		key := itr.indexSet.SeriesFile.SeriesKey(elem.SeriesID)
		if len(key) == 0 {
			continue
		}
		itr.keys = append(itr.keys, key)
	}

	// Sort keys.
	sort.Sort(seriesKeys(itr.keys))
	return nil
}

// MeasurementIterator represents a iterator over a list of measurements.
type MeasurementIterator interface {
	Close() error
	Next() ([]byte, error)
}

type MeasurementIterators []MeasurementIterator

func (a MeasurementIterators) Close() (err error) {
	for i := range a {
		if e := a[i].Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

type measurementSliceIterator struct {
	names [][]byte
}

// NewMeasurementSliceIterator returns an iterator over a slice of in-memory measurement names.
func NewMeasurementSliceIterator(names [][]byte) *measurementSliceIterator {
	return &measurementSliceIterator{names: names}
}

func (itr *measurementSliceIterator) Close() (err error) { return nil }

func (itr *measurementSliceIterator) Next() (name []byte, err error) {
	if len(itr.names) == 0 {
		return nil, nil
	}
	name, itr.names = itr.names[0], itr.names[1:]
	return name, nil
}

// MergeMeasurementIterators returns an iterator that merges a set of iterators.
// Iterators that are first in the list take precendence and a deletion by those
// early iterators will invalidate elements by later iterators.
func MergeMeasurementIterators(itrs ...MeasurementIterator) MeasurementIterator {
	if len(itrs) == 0 {
		return nil
	} else if len(itrs) == 1 {
		return itrs[0]
	}

	return &measurementMergeIterator{
		buf:  make([][]byte, len(itrs)),
		itrs: itrs,
	}
}

type measurementMergeIterator struct {
	buf  [][]byte
	itrs []MeasurementIterator
}

func (itr *measurementMergeIterator) Close() (err error) {
	for i := range itr.itrs {
		if e := itr.itrs[i].Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// Next returns the element with the next lowest name across the iterators.
//
// If multiple iterators contain the same name then the first is returned
// and the remaining ones are skipped.
func (itr *measurementMergeIterator) Next() (_ []byte, err error) {
	// Find next lowest name amongst the buffers.
	var name []byte
	for i, buf := range itr.buf {
		// Fill buffer if empty.
		if buf == nil {
			if buf, err = itr.itrs[i].Next(); err != nil {
				return nil, err
			} else if buf != nil {
				itr.buf[i] = buf
			} else {
				continue
			}
		}

		// Find next lowest name.
		if name == nil || bytes.Compare(itr.buf[i], name) == -1 {
			name = itr.buf[i]
		}
	}

	// Return nil if no elements remaining.
	if name == nil {
		return nil, nil
	}

	// Merge all elements together and clear buffers.
	for i, buf := range itr.buf {
		if buf == nil || !bytes.Equal(buf, name) {
			continue
		}
		itr.buf[i] = nil
	}
	return name, nil
}

// TagKeyIterator represents a iterator over a list of tag keys.
type TagKeyIterator interface {
	Close() error
	Next() ([]byte, error)
}

type TagKeyIterators []TagKeyIterator

func (a TagKeyIterators) Close() (err error) {
	for i := range a {
		if e := a[i].Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// NewTagKeySliceIterator returns a TagKeyIterator that iterates over a slice.
func NewTagKeySliceIterator(keys [][]byte) *tagKeySliceIterator {
	return &tagKeySliceIterator{keys: keys}
}

// tagKeySliceIterator iterates over a slice of tag keys.
type tagKeySliceIterator struct {
	keys [][]byte
}

// Next returns the next tag key in the slice.
func (itr *tagKeySliceIterator) Next() ([]byte, error) {
	if len(itr.keys) == 0 {
		return nil, nil
	}
	key := itr.keys[0]
	itr.keys = itr.keys[1:]
	return key, nil
}

func (itr *tagKeySliceIterator) Close() error { return nil }

// MergeTagKeyIterators returns an iterator that merges a set of iterators.
func MergeTagKeyIterators(itrs ...TagKeyIterator) TagKeyIterator {
	if len(itrs) == 0 {
		return nil
	} else if len(itrs) == 1 {
		return itrs[0]
	}

	return &tagKeyMergeIterator{
		buf:  make([][]byte, len(itrs)),
		itrs: itrs,
	}
}

type tagKeyMergeIterator struct {
	buf  [][]byte
	itrs []TagKeyIterator
}

func (itr *tagKeyMergeIterator) Close() error {
	for i := range itr.itrs {
		itr.itrs[i].Close()
	}
	return nil
}

// Next returns the element with the next lowest key across the iterators.
//
// If multiple iterators contain the same key then the first is returned
// and the remaining ones are skipped.
func (itr *tagKeyMergeIterator) Next() (_ []byte, err error) {
	// Find next lowest key amongst the buffers.
	var key []byte
	for i, buf := range itr.buf {
		// Fill buffer.
		if buf == nil {
			if buf, err = itr.itrs[i].Next(); err != nil {
				return nil, err
			} else if buf != nil {
				itr.buf[i] = buf
			} else {
				continue
			}
		}

		// Find next lowest key.
		if key == nil || bytes.Compare(buf, key) == -1 {
			key = buf
		}
	}

	// Return nil if no elements remaining.
	if key == nil {
		return nil, nil
	}

	// Merge elements and clear buffers.
	for i, buf := range itr.buf {
		if buf == nil || !bytes.Equal(buf, key) {
			continue
		}
		itr.buf[i] = nil
	}
	return key, nil
}

// TagValueIterator represents a iterator over a list of tag values.
type TagValueIterator interface {
	Close() error
	Next() ([]byte, error)
}

type TagValueIterators []TagValueIterator

func (a TagValueIterators) Close() (err error) {
	for i := range a {
		if e := a[i].Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// NewTagValueSliceIterator returns a TagValueIterator that iterates over a slice.
func NewTagValueSliceIterator(values [][]byte) *tagValueSliceIterator {
	return &tagValueSliceIterator{values: values}
}

// tagValueSliceIterator iterates over a slice of tag values.
type tagValueSliceIterator struct {
	values [][]byte
}

// Next returns the next tag value in the slice.
func (itr *tagValueSliceIterator) Next() ([]byte, error) {
	if len(itr.values) == 0 {
		return nil, nil
	}
	value := itr.values[0]
	itr.values = itr.values[1:]
	return value, nil
}

func (itr *tagValueSliceIterator) Close() error { return nil }

// MergeTagValueIterators returns an iterator that merges a set of iterators.
func MergeTagValueIterators(itrs ...TagValueIterator) TagValueIterator {
	if len(itrs) == 0 {
		return nil
	} else if len(itrs) == 1 {
		return itrs[0]
	}

	return &tagValueMergeIterator{
		buf:  make([][]byte, len(itrs)),
		itrs: itrs,
	}
}

type tagValueMergeIterator struct {
	buf  [][]byte
	itrs []TagValueIterator
}

func (itr *tagValueMergeIterator) Close() error {
	for i := range itr.itrs {
		itr.itrs[i].Close()
	}
	return nil
}

// Next returns the element with the next lowest value across the iterators.
//
// If multiple iterators contain the same value then the first is returned
// and the remaining ones are skipped.
func (itr *tagValueMergeIterator) Next() (_ []byte, err error) {
	// Find next lowest value amongst the buffers.
	var value []byte
	for i, buf := range itr.buf {
		// Fill buffer.
		if buf == nil {
			if buf, err = itr.itrs[i].Next(); err != nil {
				return nil, err
			} else if buf != nil {
				itr.buf[i] = buf
			} else {
				continue
			}
		}

		// Find next lowest value.
		if value == nil || bytes.Compare(buf, value) == -1 {
			value = buf
		}
	}

	// Return nil if no elements remaining.
	if value == nil {
		return nil, nil
	}

	// Merge elements and clear buffers.
	for i, buf := range itr.buf {
		if buf == nil || !bytes.Equal(buf, value) {
			continue
		}
		itr.buf[i] = nil
	}
	return value, nil
}

// IndexSet represents a list of indexes.
type IndexSet struct {
	Indexes    []Index
	SeriesFile *SeriesFile
}

// Database returns the database name of the first index.
func (is IndexSet) Database() string {
	if len(is.Indexes) == 0 {
		return ""
	}
	return is.Indexes[0].Database()
}

// FieldSet returns the fieldset of the first index.
func (is IndexSet) FieldSet() *MeasurementFieldSet {
	if len(is.Indexes) == 0 {
		return nil
	}
	return is.Indexes[0].FieldSet()
}

// DedupeInmemIndexes returns an index set which removes duplicate in-memory indexes.
func (is IndexSet) DedupeInmemIndexes() IndexSet {
	other := IndexSet{Indexes: make([]Index, 0, len(is.Indexes)), SeriesFile: is.SeriesFile}

	var hasInmem bool
	for _, idx := range is.Indexes {
		if idx.Type() == "inmem" {
			if !hasInmem {
				other.Indexes = append(other.Indexes, idx)
				hasInmem = true
			}
			continue
		}
		other.Indexes = append(other.Indexes, idx)
	}
	return other
}

// MeasurementNamesByExpr returns a slice of measurement names matching the
// provided condition. If no condition is provided then all names are returned.
func (is IndexSet) MeasurementNamesByExpr(auth query.Authorizer, expr influxql.Expr) ([][]byte, error) {
	release := is.SeriesFile.Retain()
	defer release()

	// Return filtered list if expression exists.
	if expr != nil {
		return is.measurementNamesByExpr(auth, expr)
	}

	itr, err := is.measurementIterator()
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}
	defer itr.Close()

	// Iterate over all measurements if no condition exists.
	var names [][]byte
	for {
		e, err := itr.Next()
		if err != nil {
			return nil, err
		} else if e == nil {
			break
		}

		// Determine if there exists at least one authorised series for the
		// measurement name.
		if is.measurementAuthorizedSeries(auth, e) {
			names = append(names, e)
		}
	}
	return names, nil
}

func (is IndexSet) measurementNamesByExpr(auth query.Authorizer, expr influxql.Expr) ([][]byte, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *influxql.BinaryExpr:
		switch e.Op {
		case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
			tag, ok := e.LHS.(*influxql.VarRef)
			if !ok {
				return nil, fmt.Errorf("left side of '%s' must be a tag key", e.Op.String())
			}

			// Retrieve value or regex expression from RHS.
			var value string
			var regex *regexp.Regexp
			if influxql.IsRegexOp(e.Op) {
				re, ok := e.RHS.(*influxql.RegexLiteral)
				if !ok {
					return nil, fmt.Errorf("right side of '%s' must be a regular expression", e.Op.String())
				}
				regex = re.Val
			} else {
				s, ok := e.RHS.(*influxql.StringLiteral)
				if !ok {
					return nil, fmt.Errorf("right side of '%s' must be a tag value string", e.Op.String())
				}
				value = s.Val
			}

			// Match on name, if specified.
			if tag.Val == "_name" {
				return is.measurementNamesByNameFilter(auth, e.Op, value, regex)
			} else if influxql.IsSystemName(tag.Val) {
				return nil, nil
			}
			return is.measurementNamesByTagFilter(auth, e.Op, tag.Val, value, regex)

		case influxql.OR, influxql.AND:
			lhs, err := is.measurementNamesByExpr(auth, e.LHS)
			if err != nil {
				return nil, err
			}

			rhs, err := is.measurementNamesByExpr(auth, e.RHS)
			if err != nil {
				return nil, err
			}

			if e.Op == influxql.OR {
				return bytesutil.Union(lhs, rhs), nil
			}
			return bytesutil.Intersect(lhs, rhs), nil

		default:
			return nil, fmt.Errorf("invalid tag comparison operator")
		}

	case *influxql.ParenExpr:
		return is.measurementNamesByExpr(auth, e.Expr)
	default:
		return nil, fmt.Errorf("%#v", expr)
	}
}

// measurementNamesByNameFilter returns matching measurement names in sorted order.
func (is IndexSet) measurementNamesByNameFilter(auth query.Authorizer, op influxql.Token, val string, regex *regexp.Regexp) ([][]byte, error) {
	itr, err := is.measurementIterator()
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}
	defer itr.Close()

	var names [][]byte
	for {
		e, err := itr.Next()
		if err != nil {
			return nil, err
		} else if e == nil {
			break
		}

		var matched bool
		switch op {
		case influxql.EQ:
			matched = string(e) == val
		case influxql.NEQ:
			matched = string(e) != val
		case influxql.EQREGEX:
			matched = regex.Match(e)
		case influxql.NEQREGEX:
			matched = !regex.Match(e)
		}

		if matched && is.measurementAuthorizedSeries(auth, e) {
			names = append(names, e)
		}
	}
	bytesutil.Sort(names)
	return names, nil
}

func (is IndexSet) measurementNamesByTagFilter(auth query.Authorizer, op influxql.Token, key, val string, regex *regexp.Regexp) ([][]byte, error) {
	var names [][]byte

	mitr, err := is.measurementIterator()
	if err != nil {
		return nil, err
	} else if mitr == nil {
		return nil, nil
	}
	defer mitr.Close()

	// valEqual determines if the provided []byte is equal to the tag value
	// to be filtered on.
	valEqual := regex.Match
	if op == influxql.EQ || op == influxql.NEQ {
		vb := []byte(val)
		valEqual = func(b []byte) bool { return bytes.Equal(vb, b) }
	}

	var tagMatch bool
	var authorized bool
	for {
		me, err := mitr.Next()
		if err != nil {
			return nil, err
		} else if me == nil {
			break
		}
		// If the measurement doesn't have the tag key, then it won't be considered.
		if ok, err := is.hasTagKey(me, []byte(key)); err != nil {
			return nil, err
		} else if !ok {
			continue
		}
		tagMatch = false
		// Authorization must be explicitly granted when an authorizer is present.
		authorized = query.AuthorizerIsOpen(auth)

		vitr, err := is.tagValueIterator(me, []byte(key))
		if err != nil {
			return nil, err
		}

		if vitr != nil {
			defer vitr.Close()
			for {
				ve, err := vitr.Next()
				if err != nil {
					return nil, err
				} else if ve == nil {
					break
				}
				if !valEqual(ve) {
					continue

				}

				tagMatch = true
				if query.AuthorizerIsOpen(auth) {
					break
				}

				// When an authorizer is present, the measurement should be
				// included only if one of it's series is authorized.
				sitr, err := is.tagValueSeriesIDIterator(me, []byte(key), ve)
				if err != nil {
					return nil, err
				} else if sitr == nil {
					continue
				}
				defer sitr.Close()

				// Locate a series with this matching tag value that's authorized.
				for {
					se, err := sitr.Next()
					if err != nil {
						return nil, err
					}

					if se.SeriesID == 0 {
						break
					}

					name, tags := is.SeriesFile.Series(se.SeriesID)
					if auth.AuthorizeSeriesRead(is.Database(), name, tags) {
						authorized = true
						break
					}
				}

				if err := sitr.Close(); err != nil {
					return nil, err
				}

				if tagMatch && authorized {
					// The measurement can definitely be included or rejected.
					break
				}
			}
			if err := vitr.Close(); err != nil {
				return nil, err
			}
		}

		// For negation operators, to determine if the measurement is authorized,
		// an authorized series belonging to the measurement must be located.
		// Then, the measurement can be added iff !tagMatch && authorized.
		if (op == influxql.NEQ || op == influxql.NEQREGEX) && !tagMatch {
			authorized = is.measurementAuthorizedSeries(auth, me)
		}

		// tags match | operation is EQ | measurement matches
		// --------------------------------------------------
		//     True   |       True      |      True
		//     True   |       False     |      False
		//     False  |       True      |      False
		//     False  |       False     |      True
		if tagMatch == (op == influxql.EQ || op == influxql.EQREGEX) && authorized {
			names = append(names, me)
			continue
		}
	}

	bytesutil.Sort(names)
	return names, nil
}

// measurementAuthorizedSeries determines if the measurement contains a series
// that is authorized to be read.
func (is IndexSet) measurementAuthorizedSeries(auth query.Authorizer, name []byte) bool {
	if query.AuthorizerIsOpen(auth) {
		return true
	}

	sitr, err := is.measurementSeriesIDIterator(name)
	if err != nil || sitr == nil {
		return false
	}
	defer sitr.Close()

	for {
		series, err := sitr.Next()
		if err != nil {
			return false
		}

		if series.SeriesID == 0 {
			return false // End of iterator
		}

		name, tags := is.SeriesFile.Series(series.SeriesID)
		if auth.AuthorizeSeriesRead(is.Database(), name, tags) {
			return true
		}
	}
}

// HasTagKey returns true if the tag key exists in any index for the provided
// measurement.
func (is IndexSet) HasTagKey(name, key []byte) (bool, error) {
	return is.hasTagKey(name, key)
}

// hasTagKey returns true if the tag key exists in any index for the provided
// measurement, and guarantees to never take a lock on the series file.
func (is IndexSet) hasTagKey(name, key []byte) (bool, error) {
	for _, idx := range is.Indexes {
		if ok, err := idx.HasTagKey(name, key); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}
	return false, nil
}

// HasTagValue returns true if the tag value exists in any index for the provided
// measurement and tag key.
func (is IndexSet) HasTagValue(name, key, value []byte) (bool, error) {
	for _, idx := range is.Indexes {
		if ok, err := idx.HasTagValue(name, key, value); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}
	return false, nil
}

// MeasurementIterator returns an iterator over all measurements in the index.
func (is IndexSet) MeasurementIterator() (MeasurementIterator, error) {
	return is.measurementIterator()
}

// measurementIterator returns an iterator over all measurements in the index.
// It guarantees to never take any locks on the underlying series file.
func (is IndexSet) measurementIterator() (MeasurementIterator, error) {
	a := make([]MeasurementIterator, 0, len(is.Indexes))
	for _, idx := range is.Indexes {
		itr, err := idx.MeasurementIterator()
		if err != nil {
			MeasurementIterators(a).Close()
			return nil, err
		} else if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeMeasurementIterators(a...), nil
}

// TagKeyIterator returns a key iterator for a measurement.
func (is IndexSet) TagKeyIterator(name []byte) (TagKeyIterator, error) {
	return is.tagKeyIterator(name)
}

// tagKeyIterator returns a key iterator for a measurement. It guarantees to never
// take any locks on the underlying series file.
func (is IndexSet) tagKeyIterator(name []byte) (TagKeyIterator, error) {
	a := make([]TagKeyIterator, 0, len(is.Indexes))
	for _, idx := range is.Indexes {
		itr, err := idx.TagKeyIterator(name)
		if err != nil {
			TagKeyIterators(a).Close()
			return nil, err
		} else if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeTagKeyIterators(a...), nil
}

// TagValueIterator returns a value iterator for a tag key.
func (is IndexSet) TagValueIterator(name, key []byte) (TagValueIterator, error) {
	return is.tagValueIterator(name, key)
}

// tagValueIterator returns a value iterator for a tag key. It guarantees to never
// take any locks on the underlying series file.
func (is IndexSet) tagValueIterator(name, key []byte) (TagValueIterator, error) {
	a := make([]TagValueIterator, 0, len(is.Indexes))
	for _, idx := range is.Indexes {
		itr, err := idx.TagValueIterator(name, key)
		if err != nil {
			TagValueIterators(a).Close()
			return nil, err
		} else if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeTagValueIterators(a...), nil
}

// TagKeyHasAuthorizedSeries determines if there exists an authorized series for
// the provided measurement name and tag key.
func (is IndexSet) TagKeyHasAuthorizedSeries(auth query.Authorizer, name, tagKey []byte) (bool, error) {
	release := is.SeriesFile.Retain()
	defer release()

	itr, err := is.tagKeySeriesIDIterator(name, tagKey)
	if err != nil {
		return false, err
	} else if itr == nil {
		return false, nil
	}
	defer itr.Close()

	for {
		e, err := itr.Next()
		if err != nil {
			return false, err
		}

		if e.SeriesID == 0 {
			return false, nil
		}

		if query.AuthorizerIsOpen(auth) {
			return true, nil
		}

		name, tags := is.SeriesFile.Series(e.SeriesID)
		if auth.AuthorizeSeriesRead(is.Database(), name, tags) {
			return true, nil
		}
	}
}

// MeasurementSeriesIDIterator returns an iterator over all non-tombstoned series
// for the provided measurement.
func (is IndexSet) MeasurementSeriesIDIterator(name []byte) (SeriesIDIterator, error) {
	release := is.SeriesFile.Retain()
	defer release()
	return is.measurementSeriesIDIterator(name)
}

// measurementSeriesIDIterator does not provide any locking on the Series file.
//
// See  MeasurementSeriesIDIterator for more details.
func (is IndexSet) measurementSeriesIDIterator(name []byte) (SeriesIDIterator, error) {
	a := make([]SeriesIDIterator, 0, len(is.Indexes))
	for _, idx := range is.Indexes {
		itr, err := idx.MeasurementSeriesIDIterator(name)
		if err != nil {
			SeriesIDIterators(a).Close()
			return nil, err
		} else if itr != nil {
			a = append(a, itr)
		}
	}
	return FilterUndeletedSeriesIDIterator(is.SeriesFile, MergeSeriesIDIterators(a...)), nil
}

// ForEachMeasurementTagKey iterates over all tag keys in a measurement and applies
// the provided function.
func (is IndexSet) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	release := is.SeriesFile.Retain()
	defer release()

	itr, err := is.tagKeyIterator(name)
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}
	defer itr.Close()

	for {
		key, err := itr.Next()
		if err != nil {
			return err
		} else if key == nil {
			return nil
		}

		if err := fn(key); err != nil {
			return err
		}
	}
}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (is IndexSet) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	release := is.SeriesFile.Retain()
	defer release()

	keys := make(map[string]struct{})
	for _, idx := range is.Indexes {
		m, err := idx.MeasurementTagKeysByExpr(name, expr)
		if err != nil {
			return nil, err
		}
		for k := range m {
			keys[k] = struct{}{}
		}
	}
	return keys, nil
}

// TagKeySeriesIDIterator returns a series iterator for all values across a single key.
func (is IndexSet) TagKeySeriesIDIterator(name, key []byte) (SeriesIDIterator, error) {
	release := is.SeriesFile.Retain()
	defer release()
	return is.tagKeySeriesIDIterator(name, key)
}

// tagKeySeriesIDIterator returns a series iterator for all values across a
// single key.
//
// It guarantees to never take any locks on the series file.
func (is IndexSet) tagKeySeriesIDIterator(name, key []byte) (SeriesIDIterator, error) {
	a := make([]SeriesIDIterator, 0, len(is.Indexes))
	for _, idx := range is.Indexes {
		itr, err := idx.TagKeySeriesIDIterator(name, key)
		if err != nil {
			SeriesIDIterators(a).Close()
			return nil, err
		} else if itr != nil {
			a = append(a, itr)
		}
	}
	return FilterUndeletedSeriesIDIterator(is.SeriesFile, MergeSeriesIDIterators(a...)), nil
}

// TagValueSeriesIDIterator returns a series iterator for a single tag value.
func (is IndexSet) TagValueSeriesIDIterator(name, key, value []byte) (SeriesIDIterator, error) {
	release := is.SeriesFile.Retain()
	defer release()
	return is.tagValueSeriesIDIterator(name, key, value)
}

// tagValueSeriesIDIterator does not provide any locking on the Series File.
//
// See TagValueSeriesIDIterator for more details.
func (is IndexSet) tagValueSeriesIDIterator(name, key, value []byte) (SeriesIDIterator, error) {
	a := make([]SeriesIDIterator, 0, len(is.Indexes))
	for _, idx := range is.Indexes {
		itr, err := idx.TagValueSeriesIDIterator(name, key, value)
		if err != nil {
			SeriesIDIterators(a).Close()
			return nil, err
		} else if itr != nil {
			a = append(a, itr)
		}
	}
	return FilterUndeletedSeriesIDIterator(is.SeriesFile, MergeSeriesIDIterators(a...)), nil
}

// MeasurementSeriesByExprIterator returns a series iterator for a measurement
// that is filtered by expr. If expr only contains time expressions then this
// call is equivalent to MeasurementSeriesIDIterator().
func (is IndexSet) MeasurementSeriesByExprIterator(name []byte, expr influxql.Expr) (SeriesIDIterator, error) {
	release := is.SeriesFile.Retain()
	defer release()
	return is.measurementSeriesByExprIterator(name, expr)
}

// measurementSeriesByExprIterator returns a series iterator for a measurement
// that is filtered by expr. See MeasurementSeriesByExprIterator for more details.
//
// measurementSeriesByExprIterator guarantees to never take any locks on the
// series file.
func (is IndexSet) measurementSeriesByExprIterator(name []byte, expr influxql.Expr) (SeriesIDIterator, error) {
	// Return all series for the measurement if there are no tag expressions.
	if expr == nil {
		return is.measurementSeriesIDIterator(name)
	}
	fieldset := is.FieldSet()

	itr, err := is.seriesByExprIterator(name, expr, fieldset.CreateFieldsIfNotExists(name))
	if err != nil {
		return nil, err
	}
	return FilterUndeletedSeriesIDIterator(is.SeriesFile, itr), nil
}

// MeasurementSeriesKeysByExpr returns a list of series keys matching expr.
func (is IndexSet) MeasurementSeriesKeysByExpr(name []byte, expr influxql.Expr) ([][]byte, error) {
	release := is.SeriesFile.Retain()
	defer release()

	// Create iterator for all matching series.
	itr, err := is.measurementSeriesByExprIterator(name, expr)
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}
	defer itr.Close()

	// Iterate over all series and generate keys.
	var keys [][]byte
	for {
		e, err := itr.Next()
		if err != nil {
			return nil, err
		} else if e.SeriesID == 0 {
			break
		}

		// Check for unsupported field filters.
		// Any remaining filters means there were fields (e.g., `WHERE value = 1.2`).
		if e.Expr != nil {
			if v, ok := e.Expr.(*influxql.BooleanLiteral); !ok || !v.Val {
				return nil, errors.New("fields not supported in WHERE clause during deletion")
			}
		}

		seriesKey := is.SeriesFile.SeriesKey(e.SeriesID)
		if len(seriesKey) == 0 {
			continue
		}

		name, tags := ParseSeriesKey(seriesKey)
		keys = append(keys, models.MakeKey(name, tags))
	}

	bytesutil.Sort(keys)

	return keys, nil
}

func (is IndexSet) seriesByExprIterator(name []byte, expr influxql.Expr, mf *MeasurementFields) (SeriesIDIterator, error) {
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			// Get the series IDs and filter expressions for the LHS.
			litr, err := is.seriesByExprIterator(name, expr.LHS, mf)
			if err != nil {
				return nil, err
			}

			// Get the series IDs and filter expressions for the RHS.
			ritr, err := is.seriesByExprIterator(name, expr.RHS, mf)
			if err != nil {
				if litr != nil {
					litr.Close()
				}
				return nil, err
			}

			// Intersect iterators if expression is "AND".
			if expr.Op == influxql.AND {
				return IntersectSeriesIDIterators(litr, ritr), nil
			}

			// Union iterators if expression is "OR".
			return UnionSeriesIDIterators(litr, ritr), nil

		default:
			return is.seriesByBinaryExprIterator(name, expr, mf)
		}

	case *influxql.ParenExpr:
		return is.seriesByExprIterator(name, expr.Expr, mf)

	case *influxql.BooleanLiteral:
		if expr.Val {
			return is.measurementSeriesIDIterator(name)
		}
		return nil, nil

	default:
		return nil, nil
	}
}

// seriesByBinaryExprIterator returns a series iterator and a filtering expression.
func (is IndexSet) seriesByBinaryExprIterator(name []byte, n *influxql.BinaryExpr, mf *MeasurementFields) (SeriesIDIterator, error) {
	// If this binary expression has another binary expression, then this
	// is some expression math and we should just pass it to the underlying query.
	if _, ok := n.LHS.(*influxql.BinaryExpr); ok {
		itr, err := is.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}
		return newSeriesIDExprIterator(itr, n), nil
	} else if _, ok := n.RHS.(*influxql.BinaryExpr); ok {
		itr, err := is.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}
		return newSeriesIDExprIterator(itr, n), nil
	}

	// Retrieve the variable reference from the correct side of the expression.
	key, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		key, ok = n.RHS.(*influxql.VarRef)
		if !ok {
			return nil, fmt.Errorf("invalid expression: %s", n.String())
		}
		value = n.LHS
	}

	// For fields, return all series from this measurement.
	if key.Val != "_name" && ((key.Type == influxql.Unknown && mf.HasField(key.Val)) || key.Type == influxql.AnyField || (key.Type != influxql.Tag && key.Type != influxql.Unknown)) {
		itr, err := is.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}
		return newSeriesIDExprIterator(itr, n), nil
	} else if value, ok := value.(*influxql.VarRef); ok {
		// Check if the RHS is a variable and if it is a field.
		if value.Val != "_name" && ((value.Type == influxql.Unknown && mf.HasField(value.Val)) || key.Type == influxql.AnyField || (value.Type != influxql.Tag && value.Type != influxql.Unknown)) {
			itr, err := is.measurementSeriesIDIterator(name)
			if err != nil {
				return nil, err
			}
			return newSeriesIDExprIterator(itr, n), nil
		}
	}

	// Create iterator based on value type.
	switch value := value.(type) {
	case *influxql.StringLiteral:
		return is.seriesByBinaryExprStringIterator(name, []byte(key.Val), []byte(value.Val), n.Op)
	case *influxql.RegexLiteral:
		return is.seriesByBinaryExprRegexIterator(name, []byte(key.Val), value.Val, n.Op)
	case *influxql.VarRef:
		return is.seriesByBinaryExprVarRefIterator(name, []byte(key.Val), value, n.Op)
	default:
		if n.Op == influxql.NEQ || n.Op == influxql.NEQREGEX {
			return is.measurementSeriesIDIterator(name)
		}
		return nil, nil
	}
}

func (is IndexSet) seriesByBinaryExprStringIterator(name, key, value []byte, op influxql.Token) (SeriesIDIterator, error) {
	// Special handling for "_name" to match measurement name.
	if bytes.Equal(key, []byte("_name")) {
		if (op == influxql.EQ && bytes.Equal(value, name)) || (op == influxql.NEQ && !bytes.Equal(value, name)) {
			return is.measurementSeriesIDIterator(name)
		}
		return nil, nil
	}

	if op == influxql.EQ {
		// Match a specific value.
		if len(value) != 0 {
			return is.tagValueSeriesIDIterator(name, key, value)
		}

		mitr, err := is.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}

		kitr, err := is.tagKeySeriesIDIterator(name, key)
		if err != nil {
			if mitr != nil {
				mitr.Close()
			}
			return nil, err
		}

		// Return all measurement series that have no values from this tag key.
		return DifferenceSeriesIDIterators(mitr, kitr), nil
	}

	// Return all measurement series without this tag value.
	if len(value) != 0 {
		mitr, err := is.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}

		vitr, err := is.tagValueSeriesIDIterator(name, key, value)
		if err != nil {
			if mitr != nil {
				mitr.Close()
			}
			return nil, err
		}

		return DifferenceSeriesIDIterators(mitr, vitr), nil
	}

	// Return all series across all values of this tag key.
	return is.tagKeySeriesIDIterator(name, key)
}

func (is IndexSet) seriesByBinaryExprRegexIterator(name, key []byte, value *regexp.Regexp, op influxql.Token) (SeriesIDIterator, error) {
	// Special handling for "_name" to match measurement name.
	if bytes.Equal(key, []byte("_name")) {
		match := value.Match(name)
		if (op == influxql.EQREGEX && match) || (op == influxql.NEQREGEX && !match) {
			mitr, err := is.measurementSeriesIDIterator(name)
			if err != nil {
				return nil, err
			}
			return newSeriesIDExprIterator(mitr, &influxql.BooleanLiteral{Val: true}), nil
		}
		return nil, nil
	}
	return is.matchTagValueSeriesIDIterator(name, key, value, op == influxql.EQREGEX)
}

func (is IndexSet) seriesByBinaryExprVarRefIterator(name, key []byte, value *influxql.VarRef, op influxql.Token) (SeriesIDIterator, error) {
	itr0, err := is.tagKeySeriesIDIterator(name, key)
	if err != nil {
		return nil, err
	}

	itr1, err := is.tagKeySeriesIDIterator(name, []byte(value.Val))
	if err != nil {
		if itr0 != nil {
			itr0.Close()
		}
		return nil, err
	}

	if op == influxql.EQ {
		return IntersectSeriesIDIterators(itr0, itr1), nil
	}
	return DifferenceSeriesIDIterators(itr0, itr1), nil
}

// MatchTagValueSeriesIDIterator returns a series iterator for tags which match value.
// If matches is false, returns iterators which do not match value.
func (is IndexSet) MatchTagValueSeriesIDIterator(name, key []byte, value *regexp.Regexp, matches bool) (SeriesIDIterator, error) {
	release := is.SeriesFile.Retain()
	defer release()
	return is.matchTagValueSeriesIDIterator(name, key, value, matches)
}

// matchTagValueSeriesIDIterator returns a series iterator for tags which match
// value. See MatchTagValueSeriesIDIterator for more details.
//
// It guarantees to never take any locks on the underlying series file.
func (is IndexSet) matchTagValueSeriesIDIterator(name, key []byte, value *regexp.Regexp, matches bool) (SeriesIDIterator, error) {
	matchEmpty := value.MatchString("")
	if matches {
		if matchEmpty {
			return is.matchTagValueEqualEmptySeriesIDIterator(name, key, value)
		}
		return is.matchTagValueEqualNotEmptySeriesIDIterator(name, key, value)
	}

	if matchEmpty {
		return is.matchTagValueNotEqualEmptySeriesIDIterator(name, key, value)
	}
	return is.matchTagValueNotEqualNotEmptySeriesIDIterator(name, key, value)
}

func (is IndexSet) matchTagValueEqualEmptySeriesIDIterator(name, key []byte, value *regexp.Regexp) (SeriesIDIterator, error) {
	vitr, err := is.tagValueIterator(name, key)
	if err != nil {
		return nil, err
	} else if vitr == nil {
		return is.measurementSeriesIDIterator(name)
	}
	defer vitr.Close()

	var itrs []SeriesIDIterator
	if err := func() error {
		for {
			e, err := vitr.Next()
			if err != nil {
				return err
			} else if e == nil {
				break
			}

			if !value.Match(e) {
				itr, err := is.tagValueSeriesIDIterator(name, key, e)
				if err != nil {
					return err
				}
				itrs = append(itrs, itr)
			}
		}
		return nil
	}(); err != nil {
		SeriesIDIterators(itrs).Close()
		return nil, err
	}

	mitr, err := is.measurementSeriesIDIterator(name)
	if err != nil {
		SeriesIDIterators(itrs).Close()
		return nil, err
	}

	return DifferenceSeriesIDIterators(mitr, MergeSeriesIDIterators(itrs...)), nil
}

func (is IndexSet) matchTagValueEqualNotEmptySeriesIDIterator(name, key []byte, value *regexp.Regexp) (SeriesIDIterator, error) {
	vitr, err := is.tagValueIterator(name, key)
	if err != nil {
		return nil, err
	} else if vitr == nil {
		return nil, nil
	}
	defer vitr.Close()

	var itrs []SeriesIDIterator
	for {
		e, err := vitr.Next()
		if err != nil {
			SeriesIDIterators(itrs).Close()
			return nil, err
		} else if e == nil {
			break
		}

		if value.Match(e) {
			itr, err := is.tagValueSeriesIDIterator(name, key, e)
			if err != nil {
				SeriesIDIterators(itrs).Close()
				return nil, err
			}
			itrs = append(itrs, itr)
		}
	}
	return MergeSeriesIDIterators(itrs...), nil
}

func (is IndexSet) matchTagValueNotEqualEmptySeriesIDIterator(name, key []byte, value *regexp.Regexp) (SeriesIDIterator, error) {
	vitr, err := is.tagValueIterator(name, key)
	if err != nil {
		return nil, err
	} else if vitr == nil {
		return nil, nil
	}
	defer vitr.Close()

	var itrs []SeriesIDIterator
	for {
		e, err := vitr.Next()
		if err != nil {
			SeriesIDIterators(itrs).Close()
			return nil, err
		} else if e == nil {
			break
		}

		if !value.Match(e) {
			itr, err := is.tagValueSeriesIDIterator(name, key, e)
			if err != nil {
				SeriesIDIterators(itrs).Close()
				return nil, err
			}
			itrs = append(itrs, itr)
		}
	}
	return MergeSeriesIDIterators(itrs...), nil
}

func (is IndexSet) matchTagValueNotEqualNotEmptySeriesIDIterator(name, key []byte, value *regexp.Regexp) (SeriesIDIterator, error) {
	vitr, err := is.tagValueIterator(name, key)
	if err != nil {
		return nil, err
	} else if vitr == nil {
		return is.measurementSeriesIDIterator(name)
	}
	defer vitr.Close()

	var itrs []SeriesIDIterator
	for {
		e, err := vitr.Next()
		if err != nil {
			SeriesIDIterators(itrs).Close()
			return nil, err
		} else if e == nil {
			break
		}
		if value.Match(e) {
			itr, err := is.tagValueSeriesIDIterator(name, key, e)
			if err != nil {
				SeriesIDIterators(itrs).Close()
				return nil, err
			}
			itrs = append(itrs, itr)
		}
	}

	mitr, err := is.measurementSeriesIDIterator(name)
	if err != nil {
		SeriesIDIterators(itrs).Close()
		return nil, err
	}
	return DifferenceSeriesIDIterators(mitr, MergeSeriesIDIterators(itrs...)), nil
}

// TagValuesByKeyAndExpr retrieves tag values for the provided tag keys.
//
// TagValuesByKeyAndExpr returns sets of values for each key, indexable by the
// position of the tag key in the keys argument.
//
// N.B tagValuesByKeyAndExpr relies on keys being sorted in ascending
// lexicographic order.
func (is IndexSet) TagValuesByKeyAndExpr(auth query.Authorizer, name []byte, keys []string, expr influxql.Expr, fieldset *MeasurementFieldSet) ([]map[string]struct{}, error) {
	release := is.SeriesFile.Retain()
	defer release()
	return is.tagValuesByKeyAndExpr(auth, name, keys, expr, fieldset)
}

// tagValuesByKeyAndExpr retrieves tag values for the provided tag keys. See
// TagValuesByKeyAndExpr for more details.
//
// tagValuesByKeyAndExpr guarantees to never take any locks on the underlying
// series file.
func (is IndexSet) tagValuesByKeyAndExpr(auth query.Authorizer, name []byte, keys []string, expr influxql.Expr, fieldset *MeasurementFieldSet) ([]map[string]struct{}, error) {
	database := is.Database()

	itr, err := is.seriesByExprIterator(name, expr, fieldset.Fields(string(name)))
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}
	itr = FilterUndeletedSeriesIDIterator(is.SeriesFile, itr)
	defer itr.Close()

	keyIdxs := make(map[string]int, len(keys))
	for ki, key := range keys {
		keyIdxs[key] = ki

		// Check that keys are in order.
		if ki > 0 && key < keys[ki-1] {
			return nil, fmt.Errorf("keys %v are not in ascending order", keys)
		}
	}

	resultSet := make([]map[string]struct{}, len(keys))
	for i := 0; i < len(resultSet); i++ {
		resultSet[i] = make(map[string]struct{})
	}

	// Iterate all series to collect tag values.
	for {
		e, err := itr.Next()
		if err != nil {
			return nil, err
		} else if e.SeriesID == 0 {
			break
		}

		buf := is.SeriesFile.SeriesKey(e.SeriesID)
		if len(buf) == 0 {
			continue
		}

		if auth != nil {
			name, tags := ParseSeriesKey(buf)
			if !auth.AuthorizeSeriesRead(database, name, tags) {
				continue
			}
		}

		_, buf = ReadSeriesKeyLen(buf)
		_, buf = ReadSeriesKeyMeasurement(buf)
		tagN, buf := ReadSeriesKeyTagN(buf)
		for i := 0; i < tagN; i++ {
			var key, value []byte
			key, value, buf = ReadSeriesKeyTag(buf)

			if idx, ok := keyIdxs[string(key)]; ok {
				resultSet[idx][string(value)] = struct{}{}
			} else if string(key) > keys[len(keys)-1] {
				// The tag key is > the largest key we're interested in.
				break
			}
		}
	}
	return resultSet, nil
}

// MeasurementTagKeyValuesByExpr returns a set of tag values filtered by an expression.
func (is IndexSet) MeasurementTagKeyValuesByExpr(auth query.Authorizer, name []byte, keys []string, expr influxql.Expr, keysSorted bool) ([][]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	results := make([][]string, len(keys))
	// If the keys are not sorted, then sort them.
	if !keysSorted {
		sort.Strings(keys)
	}

	release := is.SeriesFile.Retain()
	defer release()

	// No expression means that the values shouldn't be filtered; so fetch them
	// all.
	if expr == nil {
		for ki, key := range keys {
			vitr, err := is.tagValueIterator(name, []byte(key))
			if err != nil {
				return nil, err
			} else if vitr == nil {
				break
			}
			defer vitr.Close()

			// If no authorizer present then return all values.
			if query.AuthorizerIsOpen(auth) {
				for {
					val, err := vitr.Next()
					if err != nil {
						return nil, err
					} else if val == nil {
						break
					}
					results[ki] = append(results[ki], string(val))
				}
				continue
			}

			// Authorization is present — check all series with matching tag values
			// and measurements for the presence of an authorized series.
			for {
				val, err := vitr.Next()
				if err != nil {
					return nil, err
				} else if val == nil {
					break
				}

				sitr, err := is.tagValueSeriesIDIterator(name, []byte(key), val)
				if err != nil {
					return nil, err
				} else if sitr == nil {
					continue
				}
				defer sitr.Close()

				for {
					se, err := sitr.Next()
					if err != nil {
						return nil, err
					}

					if se.SeriesID == 0 {
						break
					}

					name, tags := is.SeriesFile.Series(se.SeriesID)
					if auth.AuthorizeSeriesRead(is.Database(), name, tags) {
						results[ki] = append(results[ki], string(val))
						break
					}
				}
				if err := sitr.Close(); err != nil {
					return nil, err
				}
			}
		}
		return results, nil
	}

	// This is the case where we have filtered series by some WHERE condition.
	// We only care about the tag values for the keys given the
	// filtered set of series ids.
	resultSet, err := is.tagValuesByKeyAndExpr(auth, name, keys, expr, is.FieldSet())
	if err != nil {
		return nil, err
	}

	// Convert result sets into []string
	for i, s := range resultSet {
		values := make([]string, 0, len(s))
		for v := range s {
			values = append(values, v)
		}
		sort.Strings(values)
		results[i] = values
	}
	return results, nil
}

// TagSets returns an ordered list of tag sets for a measurement by dimension
// and filtered by an optional conditional expression.
func (is IndexSet) TagSets(sfile *SeriesFile, name []byte, opt query.IteratorOptions) ([]*query.TagSet, error) {
	release := is.SeriesFile.Retain()
	defer release()

	itr, err := is.measurementSeriesByExprIterator(name, opt.Condition)
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}
	defer itr.Close()

	var dims []string
	if len(opt.Dimensions) > 0 {
		dims = make([]string, len(opt.Dimensions))
		copy(dims, opt.Dimensions)
		sort.Strings(dims)
	}

	// For every series, get the tag values for the requested tag keys i.e.
	// dimensions. This is the TagSet for that series. Series with the same
	// TagSet are then grouped together, because for the purpose of GROUP BY
	// they are part of the same composite series.
	tagSets := make(map[string]*query.TagSet, 64)
	var (
		seriesN, maxSeriesN int
		db                  = is.Database()
	)

	if opt.MaxSeriesN > 0 {
		maxSeriesN = opt.MaxSeriesN
	} else {
		maxSeriesN = int(^uint(0) >> 1)
	}

	for {
		se, err := itr.Next()
		if err != nil {
			return nil, err
		} else if se.SeriesID == 0 {
			break
		}

		// Skip if the series has been tombstoned.
		key := sfile.SeriesKey(se.SeriesID)
		if len(key) == 0 {
			continue
		}

		if seriesN&0x3fff == 0x3fff {
			// check every 16384 series if the query has been canceled
			select {
			case <-opt.InterruptCh:
				return nil, query.ErrQueryInterrupted
			default:
			}
		}

		if seriesN > maxSeriesN {
			return nil, fmt.Errorf("max-select-series limit exceeded: (%d/%d)", seriesN, opt.MaxSeriesN)
		}

		_, tags := ParseSeriesKey(key)
		if opt.Authorizer != nil && !opt.Authorizer.AuthorizeSeriesRead(db, name, tags) {
			continue
		}

		var tagsAsKey []byte
		if len(dims) > 0 {
			tagsAsKey = MakeTagsKey(dims, tags)
		}

		tagSet, ok := tagSets[string(tagsAsKey)]
		if !ok {
			// This TagSet is new, create a new entry for it.
			tagSet = &query.TagSet{
				Tags: nil,
				Key:  tagsAsKey,
			}
		}

		// Associate the series and filter with the Tagset.
		tagSet.AddFilter(string(models.MakeKey(name, tags)), se.Expr)

		// Ensure it's back in the map.
		tagSets[string(tagsAsKey)] = tagSet
		seriesN++
	}

	// Sort the series in each tag set.
	for _, t := range tagSets {
		sort.Sort(t)
	}

	// The TagSets have been created, as a map of TagSets. Just send
	// the values back as a slice, sorting for consistency.
	sortedTagsSets := make([]*query.TagSet, 0, len(tagSets))
	for _, v := range tagSets {
		sortedTagsSets = append(sortedTagsSets, v)
	}
	sort.Sort(byTagKey(sortedTagsSets))

	return sortedTagsSets, nil
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
type NewIndexFunc func(id uint64, database, path string, seriesIDSet *SeriesIDSet, sfile *SeriesFile, options EngineOptions) Index

// newIndexFuncs is a lookup of index constructors by name.
var newIndexFuncs = make(map[string]NewIndexFunc)

// RegisterIndex registers a storage index initializer by name.
func RegisterIndex(name string, fn NewIndexFunc) {
	if _, ok := newIndexFuncs[name]; ok {
		panic("index already registered: " + name)
	}
	newIndexFuncs[name] = fn
}

// RegisteredIndexes returns the slice of currently registered indexes.
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
func NewIndex(id uint64, database, path string, seriesIDSet *SeriesIDSet, sfile *SeriesFile, options EngineOptions) (Index, error) {
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
	return fn(id, database, path, seriesIDSet, sfile, options), nil
}

func MustOpenIndex(id uint64, database, path string, seriesIDSet *SeriesIDSet, sfile *SeriesFile, options EngineOptions) Index {
	idx, err := NewIndex(id, database, path, seriesIDSet, sfile, options)
	if err != nil {
		panic(err)
	} else if err := idx.Open(); err != nil {
		panic(err)
	}
	return idx
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}

type byTagKey []*query.TagSet

func (t byTagKey) Len() int           { return len(t) }
func (t byTagKey) Less(i, j int) bool { return bytes.Compare(t[i].Key, t[j].Key) < 0 }
func (t byTagKey) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
