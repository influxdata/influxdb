package tsdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"sync"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/bytesutil"
	"github.com/influxdata/influxdb/v2/pkg/estimator"
	"github.com/influxdata/influxdb/v2/pkg/slices"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

// Available index types.
const (
	TSI1IndexName = "tsi1"
)

// ErrIndexClosing can be returned to from an Index method if the index is currently closing.
var ErrIndexClosing = errors.New("index is closing")

type Index interface {
	Open() error
	Close() error
	WithLogger(*zap.Logger)

	Database() string
	MeasurementExists(name []byte) (bool, error)
	MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error)
	DropMeasurement(name []byte) error
	ForEachMeasurementName(fn func(name []byte) error) error

	CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error
	CreateSeriesListIfNotExists(keys, names [][]byte, tags []models.Tags) error
	DropSeries(seriesID uint64, key []byte, cascade bool) error
	DropMeasurementIfSeriesNotExist(name []byte) (bool, error)

	MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error)
	SeriesN() int64
	SeriesSketches() (estimator.Sketch, estimator.Sketch, error)
	SeriesIDSet() *SeriesIDSet

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

	// Bytes estimates the memory footprint of this Index, in bytes.
	Bytes() int

	Type() string

	// Returns a unique reference ID to the index instance.
	UniqueReferenceID() uintptr
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
		if len(name) == 0 {
			continue
		}
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

var _ SeriesIDIterator = (*PredicateSeriesIDIterator)(nil)

type PredicateSeriesIDIterator struct {
	itr   SeriesIDIterator
	sfile *SeriesFile
	pred  influxdb.Predicate
}

func NewPredicateSeriesIDIterator(itr SeriesIDIterator, sfile *SeriesFile, pred influxdb.Predicate) SeriesIDIterator {
	if pred == nil {
		return itr
	}
	return &PredicateSeriesIDIterator{
		itr:   itr,
		sfile: sfile,
		pred:  pred,
	}
}

func (itr *PredicateSeriesIDIterator) Close() error { return itr.itr.Close() }

func (itr *PredicateSeriesIDIterator) Next() (SeriesIDElem, error) {
	for {
		elem, err := itr.itr.Next()
		if elem.SeriesID == 0 || err != nil {
			return elem, err
		}

		// Skip if this key has been tombstoned.
		seriesKey := itr.sfile.SeriesKey(elem.SeriesID)
		if len(seriesKey) == 0 {
			continue
		}

		name, tags := ParseSeriesKey(seriesKey)
		tags = append(models.Tags{{Key: models.MeasurementTagKeyBytes, Value: name}}, tags...)
		key := models.MakeKey(name, tags)
		if !itr.pred.Matches(key) {
			continue
		}
		return elem, nil
	}
}

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

// SeriesKeyIterator represents an iterator over a list of SeriesKeys
type SeriesKeyIterator interface {
	Next() ([]byte, error)
	Close() error
}

// SeriesIDSetIterator represents an iterator that can produce a SeriesIDSet.
type SeriesIDSetIterator interface {
	SeriesIDIterator
	SeriesIDSet() *SeriesIDSet
}

type seriesIDSetIterator struct {
	ss  *SeriesIDSet
	itr SeriesIDSetIterable
}

func NewSeriesIDSetIterator(ss *SeriesIDSet) SeriesIDSetIterator {
	if ss == nil || ss.bitmap == nil {
		return nil
	}
	return &seriesIDSetIterator{ss: ss, itr: ss.Iterator()}
}

func (itr *seriesIDSetIterator) Next() (SeriesIDElem, error) {
	if !itr.itr.HasNext() {
		return SeriesIDElem{}, nil
	}
	return SeriesIDElem{SeriesID: uint64(itr.itr.Next())}, nil
}

func (itr *seriesIDSetIterator) Close() error { return nil }

func (itr *seriesIDSetIterator) SeriesIDSet() *SeriesIDSet { return itr.ss }

// NewSeriesIDSetIterators returns a slice of SeriesIDSetIterator if all itrs
// can be type casted. Otherwise returns nil.
func NewSeriesIDSetIterators(itrs []SeriesIDIterator) []SeriesIDSetIterator {
	if len(itrs) == 0 {
		return nil
	}

	a := make([]SeriesIDSetIterator, len(itrs))
	for i := range itrs {
		if itr, ok := itrs[i].(SeriesIDSetIterator); ok {
			a[i] = itr
		} else {
			return nil
		}
	}
	return a
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

// SeriesIDSet returns a set of all remaining ids.
func (itr *SeriesIDSliceIterator) SeriesIDSet() *SeriesIDSet {
	s := NewSeriesIDSet()
	for _, id := range itr.ids {
		s.AddNoLock(id)
	}
	return s
}

type SeriesIDIterators []SeriesIDIterator

func (a SeriesIDIterators) Close() (err error) {
	for i := range a {
		if e := a[i].Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (a SeriesIDIterators) filterNonNil() []SeriesIDIterator {
	other := make([]SeriesIDIterator, 0, len(a))
	for _, itr := range a {
		if itr == nil {
			continue
		}
		other = append(other, itr)
	}
	return other
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
		if len(name) == 0 {
			continue
		}
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
// Iterators that are first in the list take precedence and a deletion by those
// early iterators will invalidate elements by later iterators.
func MergeSeriesIDIterators(itrs ...SeriesIDIterator) SeriesIDIterator {
	if n := len(itrs); n == 0 {
		return nil
	} else if n == 1 {
		return itrs[0]
	}
	itrs = SeriesIDIterators(itrs).filterNonNil()

	// Merge as series id sets, if available.
	if a := NewSeriesIDSetIterators(itrs); a != nil {
		sets := make([]*SeriesIDSet, len(a))
		for i := range a {
			sets[i] = a[i].SeriesIDSet()
		}

		ss := NewSeriesIDSet()
		ss.Merge(sets...)
		SeriesIDIterators(itrs).Close()
		return NewSeriesIDSetIterator(ss)
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

	// Create series id set, if available.
	if a := NewSeriesIDSetIterators([]SeriesIDIterator{itr0, itr1}); a != nil {
		ss := a[0].SeriesIDSet().And(a[1].SeriesIDSet())
		// `a` holds references to itr0/itr1 so itr0/itr1 should not be closed when `a` is still in use
		itr0.Close()
		itr1.Close()
		return NewSeriesIDSetIterator(ss)
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

	// Create series id set, if available.
	if a := NewSeriesIDSetIterators([]SeriesIDIterator{itr0, itr1}); a != nil {
		ss := NewSeriesIDSet()
		ss.Merge(a[0].SeriesIDSet(), a[1].SeriesIDSet())
		// `a` holds references to itr0/itr1 so itr0/itr1 should not be closed when `a` is still in use
		itr0.Close()
		itr1.Close()
		return NewSeriesIDSetIterator(ss)
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

	// Create series id set, if available.
	if a := NewSeriesIDSetIterators([]SeriesIDIterator{itr0, itr1}); a != nil {
		ss := a[0].SeriesIDSet().AndNot(a[1].SeriesIDSet())
		// `a` holds references to itr0/itr1 so itr0/itr1 should not be closed when `a` is still in use
		itr0.Close()
		itr1.Close()
		return NewSeriesIDSetIterator(ss)
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
	mitr     MeasurementIterator
	keys     [][]byte
	opt      query.IteratorOptions

	point query.FloatPoint // reusable point
}

// newSeriesPointIterator returns a new instance of seriesPointIterator.
func NewSeriesPointIterator(indexSet IndexSet, opt query.IteratorOptions) (_ query.Iterator, err error) {
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
		if len(name) == 0 {
			continue
		}
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
	for i := 0; ; i++ {
		elem, err := sitr.Next()
		if err != nil {
			return err
		} else if elem.SeriesID == 0 {
			break
		}

		// Periodically check for interrupt.
		if i&0xFF == 0xFF {
			select {
			case <-itr.opt.InterruptCh:
				return itr.Close()
			default:
			}
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

type MeasurementSliceIterator interface {
	MeasurementIterator
	UnderlyingSlice() [][]byte
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

func (itr *measurementSliceIterator) UnderlyingSlice() [][]byte {
	return itr.names
}

// fileMeasurementSliceIterator is designed to allow a tag value slice
// iterator to use memory from a memory-mapped file, pinning it
// with the underlying file iterators
type fileMeasurementSliceIterator struct {
	measurementSliceIterator
	fileIterators MeasurementIterators
}

func (itr *fileMeasurementSliceIterator) Close() error {
	e1 := itr.fileIterators.Close()
	e2 := itr.measurementSliceIterator.Close()
	if e1 != nil {
		return e1
	} else {
		return e2
	}
}

func newFileMeasurementSliceIterator(names [][]byte, itrs MeasurementIterators) *fileMeasurementSliceIterator {
	return &fileMeasurementSliceIterator{
		measurementSliceIterator: measurementSliceIterator{
			names: names,
		},
		fileIterators: itrs,
	}
}

// MergeMeasurementIterators returns an iterator that merges a set of iterators.
// Iterators that are first in the list take precedence and a deletion by those
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

// IndexSet represents a list of indexes, all belonging to one database.
type IndexSet struct {
	Indexes    []Index                // The set of indexes comprising this IndexSet.
	SeriesFile *SeriesFile            // The Series File associated with the db for this set.
	fieldSets  []*MeasurementFieldSet // field sets for _all_ indexes in this set's DB.
}

// Database returns the database name of the first index.
func (is IndexSet) Database() string {
	if len(is.Indexes) == 0 {
		return ""
	}
	return is.Indexes[0].Database()
}

// HasField determines if any of the field sets on the set of indexes in the
// IndexSet have the provided field for the provided measurement.
func (is IndexSet) HasField(measurement []byte, field string) bool {
	if len(is.Indexes) == 0 {
		return false
	}

	if len(is.fieldSets) == 0 {
		// field sets may not have been initialised yet.
		is.fieldSets = make([]*MeasurementFieldSet, 0, len(is.Indexes))
		for _, idx := range is.Indexes {
			is.fieldSets = append(is.fieldSets, idx.FieldSet())
		}
	}

	for _, fs := range is.fieldSets {
		if fs.Fields(measurement).HasField(field) {
			return true
		}
	}
	return false
}

// MeasurementNamesByExpr returns a slice of measurement names matching the
// provided condition. If no condition is provided then all names are returned.
func (is IndexSet) MeasurementNamesByExpr(auth query.Authorizer, expr influxql.Expr) (_ [][]byte, err error) {
	release := is.SeriesFile.Retain()
	defer release()

	// Return filtered list if expression exists.
	if expr != nil {
		itr, returnErr := is.measurementNamesByExpr(auth, expr)
		if returnErr != nil {
			return nil, returnErr
		} else if itr == nil {
			return nil, nil
		}
		defer func() {
			if e := itr.Close(); err == nil {
				err = e
			}
		}()
		return slices.CopyChunkedByteSlices(itr.UnderlyingSlice(), 1000), nil
	}

	itr, err := is.measurementIterator()
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}
	defer func() {
		if e := itr.Close(); err == nil {
			err = e
		}
	}()

	var names [][]byte
	// Iterate over all measurements if no condition exists.
	for {
		e, err := itr.Next()
		if err != nil {
			return nil, err
		} else if e == nil {
			break
		}

		// Determine if there exists at least one authorised series for the
		// measurement name.
		if is.measurementAuthorizedSeries(auth, e, nil) {
			names = append(names, e)
		}
	}
	return slices.CopyChunkedByteSlices(names, 1000), nil
}

func (is IndexSet) measurementNamesByExpr(auth query.Authorizer, expr influxql.Expr) (MeasurementSliceIterator, error) {
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
				lhs.Close()
				return nil, err
			}

			mis := MeasurementIterators{lhs, rhs}
			if e.Op == influxql.OR {
				return newFileMeasurementSliceIterator(bytesutil.Union(lhs.UnderlyingSlice(), rhs.UnderlyingSlice()), mis), nil
			}
			return newFileMeasurementSliceIterator(bytesutil.Intersect(lhs.UnderlyingSlice(), rhs.UnderlyingSlice()), mis), nil

		default:
			return nil, fmt.Errorf("invalid tag comparison operator")
		}

	case *influxql.ParenExpr:
		return is.measurementNamesByExpr(auth, e.Expr)
	default:
		return nil, fmt.Errorf("invalid measurement expression %#v", expr)
	}
}

// measurementNamesByNameFilter returns matching measurement names in sorted order.
func (is IndexSet) measurementNamesByNameFilter(auth query.Authorizer, op influxql.Token, val string, regex *regexp.Regexp) (MeasurementSliceIterator, error) {
	itr, err := is.measurementIterator()
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}

	var names [][]byte
	for {
		e, err := itr.Next()
		if err != nil {
			itr.Close()
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

		if matched && is.measurementAuthorizedSeries(auth, e, nil) {
			names = append(names, e)
		}
	}
	bytesutil.Sort(names)
	return newFileMeasurementSliceIterator(names, MeasurementIterators{itr}), nil
}

// MeasurementNamesByPredicate returns a slice of measurement names matching the
// provided condition. If no condition is provided then all names are returned.
// This behaves differently from MeasurementNamesByExpr because it will
// return measurements using flux predicates.
func (is IndexSet) MeasurementNamesByPredicate(auth query.Authorizer, expr influxql.Expr) (_ [][]byte, err error) {
	release := is.SeriesFile.Retain()
	defer release()

	// Return filtered list if expression exists.
	if expr != nil {
		itr, returnErr := is.measurementNamesByPredicate(auth, expr)
		if returnErr != nil {
			return nil, returnErr
		}
		if itr != nil {
			defer func() {
				if e := itr.Close(); err == nil {
					err = e
				}
			}()
		}
		return slices.CopyChunkedByteSlices(itr.UnderlyingSlice(), 1000), nil
	}

	itr, err := is.measurementIterator()
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}
	defer func() {
		if e := itr.Close(); err == nil {
			err = e
		}
	}()

	var names [][]byte
	// Iterate over all measurements if no condition exists.
	for {
		e, err := itr.Next()
		if err != nil {
			return nil, err
		} else if e == nil {
			break
		}

		// Determine if there exists at least one authorised series for the
		// measurement name.
		if is.measurementAuthorizedSeries(auth, e, nil) {
			names = append(names, e)
		}
	}
	return slices.CopyChunkedByteSlices(names, 1000), nil
}

func (is IndexSet) measurementNamesByPredicate(auth query.Authorizer, expr influxql.Expr) (MeasurementSliceIterator, error) {
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
			return is.measurementNamesByTagPredicate(auth, e.Op, tag.Val, value, regex)

		case influxql.OR, influxql.AND:
			lhs, err := is.measurementNamesByPredicate(auth, e.LHS)
			if err != nil {
				return nil, err
			}
			rhs, err := is.measurementNamesByPredicate(auth, e.RHS)
			if err != nil {
				lhs.Close()
				return nil, err
			}
			mis := MeasurementIterators{lhs, rhs}

			if e.Op == influxql.OR {
				return newFileMeasurementSliceIterator(bytesutil.Union(lhs.UnderlyingSlice(), rhs.UnderlyingSlice()), mis), nil
			}
			return newFileMeasurementSliceIterator(bytesutil.Intersect(lhs.UnderlyingSlice(), rhs.UnderlyingSlice()), mis), nil

		default:
			return nil, fmt.Errorf("invalid tag comparison operator")
		}

	case *influxql.ParenExpr:
		return is.measurementNamesByPredicate(auth, e.Expr)
	default:
		return nil, fmt.Errorf("%#v", expr)
	}
}

func (is IndexSet) measurementNamesByTagFilter(auth query.Authorizer, op influxql.Token, key, val string, regex *regexp.Regexp) (MeasurementSliceIterator, error) {
	var names [][]byte
	failed := true

	mitr, err := is.measurementIterator()
	if err != nil {
		return nil, err
	} else if mitr == nil {
		return nil, nil
	}
	defer func() {
		if failed {
			mitr.Close()
		}
	}()

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
				sitr = FilterUndeletedSeriesIDIterator(is.SeriesFile, sitr)

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
			authorized = is.measurementAuthorizedSeries(auth, me, nil)
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
	failed = false
	return newFileMeasurementSliceIterator(names, MeasurementIterators{mitr}), nil
}

func (is IndexSet) measurementNamesByTagPredicate(auth query.Authorizer, op influxql.Token, key, val string, regex *regexp.Regexp) (MeasurementSliceIterator, error) {
	var names [][]byte
	failed := true

	mitr, err := is.measurementIterator()
	if err != nil {
		return nil, err
	} else if mitr == nil {
		return nil, nil
	}
	defer func() {
		if failed {
			mitr.Close()
		}
	}()

	var checkMeasurement func(auth query.Authorizer, me []byte) (bool, error)
	switch op {
	case influxql.EQ:
		checkMeasurement = func(auth query.Authorizer, me []byte) (bool, error) {
			return is.measurementHasTagValue(auth, me, []byte(key), []byte(val))
		}
	case influxql.NEQ:
		checkMeasurement = func(auth query.Authorizer, me []byte) (bool, error) {
			// If there is an authorized series in this measurement and that series
			// does not contain the tag key/value.
			ok := is.measurementAuthorizedSeries(auth, me, func(tags models.Tags) bool {
				return tags.GetString(key) == val
			})
			return ok, nil
		}
	case influxql.EQREGEX:
		checkMeasurement = func(auth query.Authorizer, me []byte) (bool, error) {
			return is.measurementHasTagValueRegex(auth, me, []byte(key), regex)
		}
	case influxql.NEQREGEX:
		checkMeasurement = func(auth query.Authorizer, me []byte) (bool, error) {
			// If there is an authorized series in this measurement and that series
			// does not contain the tag key/value.
			ok := is.measurementAuthorizedSeries(auth, me, func(tags models.Tags) bool {
				return regex.MatchString(tags.GetString(key))
			})
			return ok, nil
		}
	default:
		return nil, fmt.Errorf("unsupported operand: %s", op)
	}

	for {
		me, err := mitr.Next()
		if err != nil {
			return nil, err
		} else if me == nil {
			break
		}

		ok, err := checkMeasurement(auth, me)
		if err != nil {
			return nil, err
		} else if ok {
			names = append(names, me)
		}
	}

	bytesutil.Sort(names)
	failed = false
	return newFileMeasurementSliceIterator(names, MeasurementIterators{mitr}), nil
}

// measurementAuthorizedSeries determines if the measurement contains a series
// that is authorized to be read.
func (is IndexSet) measurementAuthorizedSeries(auth query.Authorizer, name []byte, exclude func(tags models.Tags) bool) bool {
	if query.AuthorizerIsOpen(auth) && exclude == nil {
		return true
	}

	if auth == nil {
		auth = query.OpenAuthorizer
	}

	sitr, err := is.measurementSeriesIDIterator(name)
	if err != nil || sitr == nil {
		return false
	}
	defer sitr.Close()
	sitr = FilterUndeletedSeriesIDIterator(is.SeriesFile, sitr)

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
			if exclude != nil && exclude(tags) {
				continue
			}
			return true
		}
	}
}

func (is IndexSet) measurementHasTagValue(auth query.Authorizer, me, key, value []byte) (bool, error) {
	if len(value) == 0 {
		return is.measurementHasEmptyTagValue(auth, me, key)
	}

	hasTagValue, err := is.HasTagValue(me, key, value)
	if err != nil || !hasTagValue {
		return false, err
	}

	// If the authorizer is open, return true.
	if query.AuthorizerIsOpen(auth) {
		return true, nil
	}

	// When an authorizer is present, the measurement should be
	// included only if one of it's series is authorized.
	sitr, err := is.tagValueSeriesIDIterator(me, key, value)
	if err != nil || sitr == nil {
		return false, err
	}
	defer sitr.Close()
	sitr = FilterUndeletedSeriesIDIterator(is.SeriesFile, sitr)

	// Locate a series with this matching tag value that's authorized.
	for {
		se, err := sitr.Next()
		if err != nil || se.SeriesID == 0 {
			return false, err
		}

		name, tags := is.SeriesFile.Series(se.SeriesID)
		if auth.AuthorizeSeriesRead(is.Database(), name, tags) {
			return true, nil
		}
	}
}

func (is IndexSet) measurementHasEmptyTagValue(auth query.Authorizer, me, key []byte) (bool, error) {
	// Any series that does not have a tag key
	// has an empty tag value for that key.
	// Iterate through all of the series to find one
	// series that does not have the tag key.
	sitr, err := is.measurementSeriesIDIterator(me)
	if err != nil || sitr == nil {
		return false, err
	}
	defer sitr.Close()
	sitr = FilterUndeletedSeriesIDIterator(is.SeriesFile, sitr)

	for {
		series, err := sitr.Next()
		if err != nil || series.SeriesID == 0 {
			return false, err
		}

		name, tags := is.SeriesFile.Series(series.SeriesID)
		if len(tags.Get(key)) > 0 {
			// The tag key exists in this series. We need
			// at least one series that does not have the tag
			// keys.
			continue
		}

		// Verify that we can see this series.
		if query.AuthorizerIsOpen(auth) {
			return true, nil
		} else if auth.AuthorizeSeriesRead(is.Database(), name, tags) {
			return true, nil
		}
	}
}

func (is IndexSet) measurementHasTagValueRegex(auth query.Authorizer, me, key []byte, value *regexp.Regexp) (bool, error) {
	// If the regex matches the empty string, do a special check to see
	// if we have an empty tag value.
	if matchEmpty := value.MatchString(""); matchEmpty {
		if ok, err := is.measurementHasEmptyTagValue(auth, me, key); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}

	// Iterate over the tag values and find one that matches the value.
	vitr, err := is.tagValueIterator(me, key)
	if err != nil || vitr == nil {
		return false, err
	}
	defer vitr.Close()

	for {
		ve, err := vitr.Next()
		if err != nil || ve == nil {
			return false, err
		}

		if !value.Match(ve) {
			// The regex does not match this tag value.
			continue
		}

		// If the authorizer is open, then we have found a suitable tag value.
		if query.AuthorizerIsOpen(auth) {
			return true, nil
		}

		// When an authorizer is present, the measurement should only be included
		// if one of the series is authorized.
		if authorized, err := func() (bool, error) {
			sitr, err := is.tagValueSeriesIDIterator(me, key, ve)
			if err != nil || sitr == nil {
				return false, err
			}
			defer sitr.Close()
			sitr = FilterUndeletedSeriesIDIterator(is.SeriesFile, sitr)

			// Locate an authorized series.
			for {
				se, err := sitr.Next()
				if err != nil || se.SeriesID == 0 {
					return false, err
				}

				name, tags := is.SeriesFile.Series(se.SeriesID)
				if auth.AuthorizeSeriesRead(is.Database(), name, tags) {
					return true, nil
				}
			}
		}(); err != nil {
			return false, err
		} else if authorized {
			return true, nil
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
	if query.AuthorizerIsOpen(auth) {
		return true, nil
	}

	release := is.SeriesFile.Retain()
	defer release()

	itr, err := is.tagKeySeriesIDIterator(name, tagKey)
	if err != nil {
		return false, err
	} else if itr == nil {
		return false, nil
	}
	defer itr.Close()
	itr = FilterUndeletedSeriesIDIterator(is.SeriesFile, itr)

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

	itr, err := is.measurementSeriesIDIterator(name)
	if err != nil {
		return nil, err
	}
	return FilterUndeletedSeriesIDIterator(is.SeriesFile, itr), nil
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
	return MergeSeriesIDIterators(a...), nil
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

	itr, err := is.tagKeySeriesIDIterator(name, key)
	if err != nil {
		return nil, err
	}
	return FilterUndeletedSeriesIDIterator(is.SeriesFile, itr), nil
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
	return MergeSeriesIDIterators(a...), nil
}

// TagValueSeriesIDIterator returns a series iterator for a single tag value.
func (is IndexSet) TagValueSeriesIDIterator(name, key, value []byte) (SeriesIDIterator, error) {
	release := is.SeriesFile.Retain()
	defer release()

	itr, err := is.tagValueSeriesIDIterator(name, key, value)
	if err != nil {
		return nil, err
	}
	return FilterUndeletedSeriesIDIterator(is.SeriesFile, itr), nil
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
	return MergeSeriesIDIterators(a...), nil
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
		itr, err := is.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}
		return FilterUndeletedSeriesIDIterator(is.SeriesFile, itr), nil
	}

	itr, err := is.seriesByExprIterator(name, expr)
	if err != nil {
		return nil, err
	}
	return FilterUndeletedSeriesIDIterator(is.SeriesFile, itr), nil
}

type measurementSeriesKeyByExprIterator struct {
	ids      SeriesIDIterator
	is       IndexSet
	auth     query.Authorizer
	once     sync.Once
	releaser func()
}

func (itr *measurementSeriesKeyByExprIterator) Next() ([]byte, error) {
	if itr == nil {
		return nil, nil
	}
	for {
		e, err := itr.ids.Next()
		if err != nil {
			return nil, err
		} else if e.SeriesID == 0 {
			return nil, nil
		}

		seriesKey := itr.is.SeriesFile.SeriesKey(e.SeriesID)
		if len(seriesKey) == 0 {
			continue
		}

		name, tags := ParseSeriesKey(seriesKey)
		if len(name) == 0 {
			continue
		}

		// Check leftover filters. All fields that might be filtered default to zero values
		if e.Expr != nil {
			if v, ok := e.Expr.(*influxql.BooleanLiteral); ok {
				if !v.Val {
					continue
				}
			} else {
				values := make(map[string]interface{}, len(tags))
				for _, t := range tags {
					values[string(t.Key)] = string(t.Value)
				}
				if !influxql.EvalBool(e.Expr, values) {
					continue
				}
			}
		}

		if itr.auth != nil && !itr.auth.AuthorizeSeriesRead(itr.is.Database(), name, tags) {
			continue
		}

		out := models.MakeKey(name, tags)
		// ensure nil is only returned when we are done (or for errors)
		if out == nil {
			out = []byte{}
		}
		return out, nil
	}
}

func (itr *measurementSeriesKeyByExprIterator) Close() error {
	if itr == nil {
		return nil
	}
	itr.once.Do(itr.releaser)
	return itr.ids.Close()
}

// MeasurementSeriesKeyByExprIterator iterates through series, filtered by an expression on the tags.
// Any non-tag expressions will be filtered as if the field had the zero value.
func (is IndexSet) MeasurementSeriesKeyByExprIterator(name []byte, expr influxql.Expr, auth query.Authorizer) (SeriesKeyIterator, error) {
	release := is.SeriesFile.Retain()
	// Create iterator for all matching series.
	ids, err := is.measurementSeriesByExprIterator(name, expr)
	if err != nil {
		release()
		return nil, err
	}
	if ids == nil {
		release()
		return nil, nil
	}
	return &measurementSeriesKeyByExprIterator{
		ids:      ids,
		releaser: release,
		auth:     auth,
		is:       is,
	}, nil
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

	// measurementSeriesByExprIterator filters deleted series; no need to do so here.

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
		// An invalid series key of 0 length should have been caught by the
		// above check, but for extra safety we can pass over it here too.
		if len(name) == 0 {
			continue
		}
		keys = append(keys, models.MakeKey(name, tags))
	}

	bytesutil.Sort(keys)

	return keys, nil
}

func (is IndexSet) seriesByExprIterator(name []byte, expr influxql.Expr) (SeriesIDIterator, error) {
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			// Get the series IDs and filter expressions for the LHS.
			litr, err := is.seriesByExprIterator(name, expr.LHS)
			if err != nil {
				return nil, err
			}

			// Get the series IDs and filter expressions for the RHS.
			ritr, err := is.seriesByExprIterator(name, expr.RHS)
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
			return is.seriesByBinaryExprIterator(name, expr)
		}

	case *influxql.ParenExpr:
		return is.seriesByExprIterator(name, expr.Expr)

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
func (is IndexSet) seriesByBinaryExprIterator(name []byte, n *influxql.BinaryExpr) (SeriesIDIterator, error) {
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
			// This is an expression we do not know how to evaluate. Let the
			// query engine take care of this.
			itr, err := is.measurementSeriesIDIterator(name)
			if err != nil {
				return nil, err
			}
			return newSeriesIDExprIterator(itr, n), nil
		}
		value = n.LHS
	}

	// For fields, return all series from this measurement.
	if key.Val != "_name" && ((key.Type == influxql.Unknown && is.HasField(name, key.Val)) || key.Type == influxql.AnyField || (key.Type != influxql.Tag && key.Type != influxql.Unknown)) {
		itr, err := is.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}
		return newSeriesIDExprIterator(itr, n), nil
	} else if value, ok := value.(*influxql.VarRef); ok {
		// Check if the RHS is a variable and if it is a field.
		if value.Val != "_name" && ((value.Type == influxql.Unknown && is.HasField(name, value.Val)) || key.Type == influxql.AnyField || (value.Type != influxql.Tag && value.Type != influxql.Unknown)) {
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
		// We do not know how to evaluate this expression so pass it
		// on to the query engine.
		itr, err := is.measurementSeriesIDIterator(name)
		if err != nil {
			return nil, err
		}
		return newSeriesIDExprIterator(itr, n), nil
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
	itr, err := is.matchTagValueSeriesIDIterator(name, key, value, matches)
	if err != nil {
		return nil, err
	}
	return FilterUndeletedSeriesIDIterator(is.SeriesFile, itr), nil
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
				} else if itr != nil {
					itrs = append(itrs, itr)
				}
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
			} else if itr != nil {
				itrs = append(itrs, itr)
			}
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
			} else if itr != nil {
				itrs = append(itrs, itr)
			}
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
			} else if itr != nil {
				itrs = append(itrs, itr)
			}
		}
	}

	mitr, err := is.measurementSeriesIDIterator(name)
	if err != nil {
		SeriesIDIterators(itrs).Close()
		return nil, err
	}
	return DifferenceSeriesIDIterators(mitr, MergeSeriesIDIterators(itrs...)), nil
}

// tagValuesByKeyAndExpr retrieves tag values for the provided tag keys.
//
// tagValuesByKeyAndExpr returns sets of values for each key, indexable by the
// position of the tag key in the keys argument.
//
// N.B tagValuesByKeyAndExpr relies on keys being sorted in ascending
// lexicographic order.
//
// tagValuesByKeyAndExpr guarantees to never take any locks on the underlying
// series file.
func (is IndexSet) tagValuesByKeyAndExpr(auth query.Authorizer, name []byte, keys []string, expr influxql.Expr) ([]map[string]struct{}, error) {
	database := is.Database()

	valueExpr, remainingExpr, err := influxql.PartitionExpr(influxql.CloneExpr(expr), func(e influxql.Expr) (bool, error) {
		switch e := e.(type) {
		case *influxql.BinaryExpr:
			switch e.Op {
			case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
				tag, ok := e.LHS.(*influxql.VarRef)
				if ok && tag.Val == "value" {
					return true, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	if remainingExpr == nil {
		remainingExpr = &influxql.BooleanLiteral{Val: true}
	}

	itr, err := is.seriesByExprIterator(name, remainingExpr)
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

		if e.Expr != nil {
			// We don't yet have code that correctly processes expressions that
			// seriesByExprIterator doesn't handle
			lit, ok := e.Expr.(*influxql.BooleanLiteral)
			if !ok {
				return nil, fmt.Errorf("expression too complex for metaquery: %v", e.Expr)
			}
			if !lit.Val {
				continue
			}
		}

		buf := is.SeriesFile.SeriesKey(e.SeriesID)
		if len(buf) == 0 {
			continue
		}

		if auth != nil {
			name, tags := ParseSeriesKey(buf)
			if len(name) == 0 {
				continue
			}
			if !auth.AuthorizeSeriesRead(database, name, tags) {
				continue
			}
		}

		_, buf = ReadSeriesKeyLen(buf)
		if len(buf) == 0 {
			continue
		}
		_, buf = ReadSeriesKeyMeasurement(buf)
		tagN, buf := ReadSeriesKeyTagN(buf)
		for i := 0; i < tagN; i++ {
			var key, value []byte
			key, value, buf = ReadSeriesKeyTag(buf)
			if valueExpr != nil {
				if !influxql.EvalBool(valueExpr, map[string]interface{}{"value": string(value)}) {
					continue
				}
			}

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
				sitr = FilterUndeletedSeriesIDIterator(is.SeriesFile, sitr)

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
	resultSet, err := is.tagValuesByKeyAndExpr(auth, name, keys, expr)
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
	// measurementSeriesByExprIterator filters deleted series IDs; no need to
	// do so here.

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

	// The tag sets require a string for each series key in the set, The series
	// file formatted keys need to be parsed into models format. Since they will
	// end up as strings we can re-use an intermediate buffer for this process.
	var keyBuf []byte
	var tagsBuf models.Tags // Buffer for tags. Tags are not needed outside of each loop iteration.
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

		// NOTE - must not escape this loop iteration.
		_, tagsBuf = ParseSeriesKeyInto(key, tagsBuf)
		if opt.Authorizer != nil && !opt.Authorizer.AuthorizeSeriesRead(db, name, tagsBuf) {
			continue
		}

		var tagsAsKey []byte
		if len(dims) > 0 {
			tagsAsKey = MakeTagsKey(dims, tagsBuf)
		}

		tagSet, ok := tagSets[string(tagsAsKey)]
		if !ok {
			// This TagSet is new, create a new entry for it.
			tagSet = &query.TagSet{
				Key: tagsAsKey,
			}
		}

		// Associate the series and filter with the Tagset.
		keyBuf = models.AppendMakeKey(keyBuf, name, tagsBuf)
		tagSet.AddFilter(string(keyBuf), se.Expr)
		keyBuf = keyBuf[:0]

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
		return nil, fmt.Errorf("error calling Stat on %q in NewIndex: %w", path, err)
	} else if err == nil {
		format = TSI1IndexName
	}

	// Lookup index by format.
	fn := newIndexFuncs[format]
	if fn == nil {
		return nil, fmt.Errorf("invalid index format for %q in NewIndex: %q", path, format)
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
