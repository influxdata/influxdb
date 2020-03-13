package tsdb

import (
	"bytes"

	"github.com/influxdata/influxql"
)

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

// seriesIDExprIterator is an iterator that attaches an associated expression.
type SeriesIDExprIterator struct {
	itr  SeriesIDIterator
	expr influxql.Expr
}

// newSeriesIDExprIterator returns a new instance of seriesIDExprIterator.
func NewSeriesIDExprIterator(itr SeriesIDIterator, expr influxql.Expr) SeriesIDIterator {
	if itr == nil {
		return nil
	}

	return &SeriesIDExprIterator{
		itr:  itr,
		expr: expr,
	}
}

func (itr *SeriesIDExprIterator) Close() error {
	return itr.itr.Close()
}

// Next returns the next element in the iterator.
func (itr *SeriesIDExprIterator) Next() (SeriesIDElem, error) {
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
// Iterators that are first in the list take precedence and a deletion by those
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

func (itr *seriesIDMergeIterator) Close() (err error) {
	return SeriesIDIterators(itr.itrs).Close()
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
		return NewSeriesIDSetIterator(NewSeriesIDSetNegate(a[0].SeriesIDSet(), a[1].SeriesIDSet()))
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

func (itr *tagKeyMergeIterator) Close() (err error) {
	for i := range itr.itrs {
		if e := itr.itrs[i].Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
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

func (itr *tagValueMergeIterator) Close() (err error) {
	for i := range itr.itrs {
		if e := itr.itrs[i].Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
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
