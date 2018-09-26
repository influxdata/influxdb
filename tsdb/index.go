package tsdb

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"sort"

	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/models"
	"go.uber.org/zap"
)

// Available index types.
const (
	InmemIndexName = "inmem"
	TSI1IndexName  = "tsi1"
)

type Index interface {
	Open() error            // used by this package
	Close() error           // used by this package
	WithLogger(*zap.Logger) // used by this package

	Database() string                                            // used by this package
	MeasurementExists(name []byte) (bool, error)                 // used by engine
	MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) // used by engine
	ForEachMeasurementName(fn func(name []byte) error) error     // used by engine

	InitializeSeries(*SeriesCollection) error                                               // used by engine
	CreateSeriesIfNotExists(key, name []byte, tags models.Tags, typ models.FieldType) error // used by engine
	CreateSeriesListIfNotExists(*SeriesCollection) error                                    // used by engine
	DropSeries(seriesID SeriesID, key []byte, cascade bool) error                           // used by engine
	DropMeasurementIfSeriesNotExist(name []byte) error                                      // used by engine

	MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) // used by engine
	SeriesN() int64                                                    // used by engine
	SeriesSketches() (estimator.Sketch, estimator.Sketch, error)       // used by engine
	SeriesIDSet() *SeriesIDSet                                         // used by idpe

	HasTagKey(name, key []byte) (bool, error)          // used by this package
	HasTagValue(name, key, value []byte) (bool, error) // used by this package

	MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) // used by this package

	TagKeyCardinality(name, key []byte) int // used by engine

	// InfluxQL system iterators
	MeasurementIterator() (MeasurementIterator, error)                          // used by this package
	TagKeyIterator(name []byte) (TagKeyIterator, error)                         // used by this package
	TagValueIterator(name, key []byte) (TagValueIterator, error)                // used by this package
	MeasurementSeriesIDIterator(name []byte) (SeriesIDIterator, error)          // used by this package
	TagKeySeriesIDIterator(name, key []byte) (SeriesIDIterator, error)          // used by this package
	TagValueSeriesIDIterator(name, key, value []byte) (SeriesIDIterator, error) // used by this package

	// // Sets a shared fieldset from the engine.
	FieldSet() *MeasurementFieldSet      // used by this package
	SetFieldSet(fs *MeasurementFieldSet) // used by engine

	// To be removed w/ tsi1.
	SetFieldName(measurement []byte, name string) // used by this package

	Type() string // used by this package
	// Returns a unique reference ID to the index instance.
	// For inmem, returns a reference to the backing Index, not ShardIndex.
	UniqueReferenceID() uintptr // used by this package

	Rebuild() // used by engine
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
		} else if elem.SeriesID.IsZero() {
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
	SeriesID SeriesID
	Expr     influxql.Expr
}

// SeriesIDIterator represents a iterator over a list of series ids.
type SeriesIDIterator interface {
	Next() (SeriesIDElem, error)
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
	return SeriesIDElem{SeriesID: NewSeriesID(uint64(itr.itr.Next()))}, nil
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

// NewSeriesIDSliceIterator returns a SeriesIDIterator that iterates over a slice.
func NewSeriesIDSliceIterator(ids []SeriesID) *SeriesIDSliceIterator {
	return &SeriesIDSliceIterator{ids: ids}
}

// SeriesIDSliceIterator iterates over a slice of series ids.
type SeriesIDSliceIterator struct {
	ids []SeriesID
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
		} else if e.SeriesID.IsZero() {
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
	} else if elem.SeriesID.IsZero() {
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
		if buf.SeriesID.IsZero() {
			elem, err := itr.itrs[i].Next()
			if err != nil {
				return SeriesIDElem{}, nil
			} else if elem.SeriesID.IsZero() {
				continue
			}
			itr.buf[i] = elem
		}

		if elem.SeriesID.IsZero() || buf.SeriesID.Less(elem.SeriesID) {
			elem = *buf
		}
	}

	// Return EOF if no elements remaining.
	if elem.SeriesID.IsZero() {
		return SeriesIDElem{}, nil
	}

	// Clear matching buffers.
	for i := range itr.buf {
		if itr.buf[i].SeriesID == elem.SeriesID {
			itr.buf[i].SeriesID = SeriesID{}
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
		itr0.Close()
		itr1.Close()
		return NewSeriesIDSetIterator(a[0].SeriesIDSet().And(a[1].SeriesIDSet()))
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
		if itr.buf[0].SeriesID.IsZero() {
			if itr.buf[0], err = itr.itrs[0].Next(); err != nil {
				return SeriesIDElem{}, err
			}
		}
		if itr.buf[1].SeriesID.IsZero() {
			if itr.buf[1], err = itr.itrs[1].Next(); err != nil {
				return SeriesIDElem{}, err
			}
		}

		// Exit if either buffer is still empty.
		if itr.buf[0].SeriesID.IsZero() || itr.buf[1].SeriesID.IsZero() {
			return SeriesIDElem{}, nil
		}

		// Skip if both series are not equal.
		if a, b := itr.buf[0].SeriesID, itr.buf[1].SeriesID; a.Less(b) {
			itr.buf[0].SeriesID = SeriesID{}
			continue
		} else if a.Greater(b) {
			itr.buf[1].SeriesID = SeriesID{}
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

		itr.buf[0].SeriesID, itr.buf[1].SeriesID = SeriesID{}, SeriesID{}
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
		itr0.Close()
		itr1.Close()
		ss := NewSeriesIDSet()
		ss.Merge(a[0].SeriesIDSet(), a[1].SeriesIDSet())
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
	if itr.buf[0].SeriesID.IsZero() {
		if itr.buf[0], err = itr.itrs[0].Next(); err != nil {
			return SeriesIDElem{}, err
		}
	}
	if itr.buf[1].SeriesID.IsZero() {
		if itr.buf[1], err = itr.itrs[1].Next(); err != nil {
			return SeriesIDElem{}, err
		}
	}

	// Return non-zero or lesser series.
	if a, b := itr.buf[0].SeriesID, itr.buf[1].SeriesID; a.IsZero() && b.IsZero() {
		return SeriesIDElem{}, nil
	} else if b.IsZero() || (!a.IsZero() && a.Less(b)) {
		elem := itr.buf[0]
		itr.buf[0].SeriesID = SeriesID{}
		return elem, nil
	} else if a.IsZero() || (!b.IsZero() && a.Greater(b)) {
		elem := itr.buf[1]
		itr.buf[1].SeriesID = SeriesID{}
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

	itr.buf[0].SeriesID, itr.buf[1].SeriesID = SeriesID{}, SeriesID{}
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
		itr0.Close()
		itr1.Close()
		return NewSeriesIDSetIterator(a[0].SeriesIDSet().AndNot(a[1].SeriesIDSet()))
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
		if itr.buf[0].SeriesID.IsZero() {
			if itr.buf[0], err = itr.itrs[0].Next(); err != nil {
				return SeriesIDElem{}, err
			}
		}
		if itr.buf[1].SeriesID.IsZero() {
			if itr.buf[1], err = itr.itrs[1].Next(); err != nil {
				return SeriesIDElem{}, err
			}
		}

		// Exit if first buffer is still empty.
		if itr.buf[0].SeriesID.IsZero() {
			return SeriesIDElem{}, nil
		} else if itr.buf[1].SeriesID.IsZero() {
			elem := itr.buf[0]
			itr.buf[0].SeriesID = SeriesID{}
			return elem, nil
		}

		// Return first series if it's less.
		// If second series is less then skip it.
		// If both series are equal then skip both.
		if a, b := itr.buf[0].SeriesID, itr.buf[1].SeriesID; a.Less(b) {
			elem := itr.buf[0]
			itr.buf[0].SeriesID = SeriesID{}
			return elem, nil
		} else if a.Greater(b) {
			itr.buf[1].SeriesID = SeriesID{}
			continue
		} else {
			itr.buf[0].SeriesID, itr.buf[1].SeriesID = SeriesID{}, SeriesID{}
			continue
		}
	}
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
		} else if se.SeriesID.IsZero() {
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
				Tags: nil,
				Key:  tagsAsKey,
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
func NewIndex(id uint64, path string, seriesIDSet *SeriesIDSet, sfile *SeriesFile, options EngineOptions) (Index, error) {
	format := options.IndexVersion

	// Use default format unless existing directory exists.
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		// nop, use default
	} else if err != nil {
		return nil, err
	} else if err == nil {
		format = TSI1IndexName
	}

	// Lookup index by format.
	fn := newIndexFuncs[format]
	if fn == nil {
		return nil, fmt.Errorf("invalid index format: %q", format)
	}

	// TODO(jeff): remove database argument
	return fn(id, "", path, seriesIDSet, sfile, options), nil
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
