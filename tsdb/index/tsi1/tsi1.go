package tsi1

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// LoadFactor is the fill percent for RHH indexes.
const LoadFactor = 80

// MeasurementElem represents a generic measurement element.
type MeasurementElem interface {
	Name() []byte
	Deleted() bool
	HasSeries() bool
}

// MeasurementElems represents a list of MeasurementElem.
type MeasurementElems []MeasurementElem

func (a MeasurementElems) Len() int           { return len(a) }
func (a MeasurementElems) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a MeasurementElems) Less(i, j int) bool { return bytes.Compare(a[i].Name(), a[j].Name()) == -1 }

// MeasurementIterator represents a iterator over a list of measurements.
type MeasurementIterator interface {
	Next() MeasurementElem
}

// MergeMeasurementIterators returns an iterator that merges a set of iterators.
// Iterators that are first in the list take precendence and a deletion by those
// early iterators will invalidate elements by later iterators.
func MergeMeasurementIterators(itrs ...MeasurementIterator) MeasurementIterator {
	if len(itrs) == 0 {
		return nil
	}

	return &measurementMergeIterator{
		e:    make(measurementMergeElem, 0, len(itrs)),
		buf:  make([]MeasurementElem, len(itrs)),
		itrs: itrs,
	}
}

type measurementMergeIterator struct {
	e    measurementMergeElem
	buf  []MeasurementElem
	itrs []MeasurementIterator
}

// Next returns the element with the next lowest name across the iterators.
//
// If multiple iterators contain the same name then the first is returned
// and the remaining ones are skipped.
func (itr *measurementMergeIterator) Next() MeasurementElem {
	// Find next lowest name amongst the buffers.
	var name []byte
	for i, buf := range itr.buf {
		// Fill buffer if empty.
		if buf == nil {
			if buf = itr.itrs[i].Next(); buf != nil {
				itr.buf[i] = buf
			} else {
				continue
			}
		}

		// Find next lowest name.
		if name == nil || bytes.Compare(itr.buf[i].Name(), name) == -1 {
			name = itr.buf[i].Name()
		}
	}

	// Return nil if no elements remaining.
	if name == nil {
		return nil
	}

	// Merge all elements together and clear buffers.
	itr.e = itr.e[:0]
	for i, buf := range itr.buf {
		if buf == nil || !bytes.Equal(buf.Name(), name) {
			continue
		}
		itr.e = append(itr.e, buf)
		itr.buf[i] = nil
	}
	return itr.e
}

// measurementMergeElem represents a merged measurement element.
type measurementMergeElem []MeasurementElem

// Name returns the name of the first element.
func (p measurementMergeElem) Name() []byte {
	if len(p) == 0 {
		return nil
	}
	return p[0].Name()
}

// Deleted returns the deleted flag of the first element.
func (p measurementMergeElem) Deleted() bool {
	if len(p) == 0 {
		return false
	}
	return p[0].Deleted()
}

func (p measurementMergeElem) HasSeries() bool {
	for _, v := range p {
		if v.HasSeries() {
			return true
		}
	}
	return false
}

// filterUndeletedMeasurementIterator returns all measurements which are not deleted.
type filterUndeletedMeasurementIterator struct {
	itr MeasurementIterator
}

// FilterUndeletedMeasurementIterator returns an iterator which filters all deleted measurement.
func FilterUndeletedMeasurementIterator(itr MeasurementIterator) MeasurementIterator {
	if itr == nil {
		return nil
	}
	return &filterUndeletedMeasurementIterator{itr: itr}
}

func (itr *filterUndeletedMeasurementIterator) Next() MeasurementElem {
	for {
		e := itr.itr.Next()
		if e == nil {
			return nil
		} else if e.Deleted() {
			continue
		}
		return e
	}
}

// TagKeyElem represents a generic tag key element.
type TagKeyElem interface {
	Key() []byte
	Deleted() bool
	TagValueIterator() TagValueIterator
}

// TagKeyIterator represents a iterator over a list of tag keys.
type TagKeyIterator interface {
	Next() TagKeyElem
}

// MergeTagKeyIterators returns an iterator that merges a set of iterators.
// Iterators that are first in the list take precendence and a deletion by those
// early iterators will invalidate elements by later iterators.
func MergeTagKeyIterators(itrs ...TagKeyIterator) TagKeyIterator {
	if len(itrs) == 0 {
		return nil
	}

	return &tagKeyMergeIterator{
		e:    make(tagKeyMergeElem, 0, len(itrs)),
		buf:  make([]TagKeyElem, len(itrs)),
		itrs: itrs,
	}
}

type tagKeyMergeIterator struct {
	e    tagKeyMergeElem
	buf  []TagKeyElem
	itrs []TagKeyIterator
}

// Next returns the element with the next lowest key across the iterators.
//
// If multiple iterators contain the same key then the first is returned
// and the remaining ones are skipped.
func (itr *tagKeyMergeIterator) Next() TagKeyElem {
	// Find next lowest key amongst the buffers.
	var key []byte
	for i, buf := range itr.buf {
		// Fill buffer.
		if buf == nil {
			if buf = itr.itrs[i].Next(); buf != nil {
				itr.buf[i] = buf
			} else {
				continue
			}
		}

		// Find next lowest key.
		if key == nil || bytes.Compare(buf.Key(), key) == -1 {
			key = buf.Key()
		}
	}

	// Return nil if no elements remaining.
	if key == nil {
		return nil
	}

	// Merge elements together & clear buffer.
	itr.e = itr.e[:0]
	for i, buf := range itr.buf {
		if buf == nil || !bytes.Equal(buf.Key(), key) {
			continue
		}
		itr.e = append(itr.e, buf)
		itr.buf[i] = nil
	}

	return itr.e
}

// tagKeyMergeElem represents a merged tag key element.
type tagKeyMergeElem []TagKeyElem

// Key returns the key of the first element.
func (p tagKeyMergeElem) Key() []byte {
	if len(p) == 0 {
		return nil
	}
	return p[0].Key()
}

// Deleted returns the deleted flag of the first element.
func (p tagKeyMergeElem) Deleted() bool {
	if len(p) == 0 {
		return false
	}
	return p[0].Deleted()
}

// TagValueIterator returns a merge iterator for all elements until a tombstone occurs.
func (p tagKeyMergeElem) TagValueIterator() TagValueIterator {
	if len(p) == 0 {
		return nil
	}

	a := make([]TagValueIterator, 0, len(p))
	for _, e := range p {
		itr := e.TagValueIterator()

		a = append(a, itr)
		if e.Deleted() {
			break
		}
	}
	return MergeTagValueIterators(a...)
}

// TagValueElem represents a generic tag value element.
type TagValueElem interface {
	Value() []byte
	Deleted() bool
}

// TagValueIterator represents a iterator over a list of tag values.
type TagValueIterator interface {
	Next() TagValueElem
}

// MergeTagValueIterators returns an iterator that merges a set of iterators.
// Iterators that are first in the list take precendence and a deletion by those
// early iterators will invalidate elements by later iterators.
func MergeTagValueIterators(itrs ...TagValueIterator) TagValueIterator {
	if len(itrs) == 0 {
		return nil
	}

	return &tagValueMergeIterator{
		e:    make(tagValueMergeElem, 0, len(itrs)),
		buf:  make([]TagValueElem, len(itrs)),
		itrs: itrs,
	}
}

type tagValueMergeIterator struct {
	e    tagValueMergeElem
	buf  []TagValueElem
	itrs []TagValueIterator
}

// Next returns the element with the next lowest value across the iterators.
//
// If multiple iterators contain the same value then the first is returned
// and the remaining ones are skipped.
func (itr *tagValueMergeIterator) Next() TagValueElem {
	// Find next lowest value amongst the buffers.
	var value []byte
	for i, buf := range itr.buf {
		// Fill buffer.
		if buf == nil {
			if buf = itr.itrs[i].Next(); buf != nil {
				itr.buf[i] = buf
			} else {
				continue
			}
		}

		// Find next lowest value.
		if value == nil || bytes.Compare(buf.Value(), value) == -1 {
			value = buf.Value()
		}
	}

	// Return nil if no elements remaining.
	if value == nil {
		return nil
	}

	// Merge elements and clear buffers.
	itr.e = itr.e[:0]
	for i, buf := range itr.buf {
		if buf == nil || !bytes.Equal(buf.Value(), value) {
			continue
		}
		itr.e = append(itr.e, buf)
		itr.buf[i] = nil
	}
	return itr.e
}

// tagValueMergeElem represents a merged tag value element.
type tagValueMergeElem []TagValueElem

// Name returns the value of the first element.
func (p tagValueMergeElem) Value() []byte {
	if len(p) == 0 {
		return nil
	}
	return p[0].Value()
}

// Deleted returns the deleted flag of the first element.
func (p tagValueMergeElem) Deleted() bool {
	if len(p) == 0 {
		return false
	}
	return p[0].Deleted()
}

// SeriesElemKey encodes e as a series key.
func SeriesElemKey(e tsdb.SeriesElem) []byte {
	name, tags := e.Name(), e.Tags()

	// TODO: Precompute allocation size.
	// FIXME: Handle escaping.

	var buf []byte
	buf = append(buf, name...)
	for _, t := range tags {
		buf = append(buf, ',')
		buf = append(buf, t.Key...)
		buf = append(buf, '=')
		buf = append(buf, t.Value...)
	}
	return buf
}

// CompareSeriesElem returns -1 if a < b, 1 if a > b, and 0 if equal.
func CompareSeriesElem(a, b tsdb.SeriesElem) int {
	if cmp := bytes.Compare(a.Name(), b.Name()); cmp != 0 {
		return cmp
	}
	return models.CompareTags(a.Tags(), b.Tags())
}

// seriesElem represents an in-memory implementation of SeriesElem.
type seriesElem struct {
	name    []byte
	tags    models.Tags
	deleted bool
}

func (e *seriesElem) Name() []byte        { return e.name }
func (e *seriesElem) Tags() models.Tags   { return e.tags }
func (e *seriesElem) Deleted() bool       { return e.deleted }
func (e *seriesElem) Expr() influxql.Expr { return nil }

// MergeSeriesIterators returns an iterator that merges a set of iterators.
// Iterators that are first in the list take precendence and a deletion by those
// early iterators will invalidate elements by later iterators.
func MergeSeriesIterators(itrs ...tsdb.SeriesIterator) tsdb.SeriesIterator {
	if n := len(itrs); n == 0 {
		return nil
	} else if n == 1 {
		return itrs[0]
	}

	return &seriesMergeIterator{
		buf:  make([]tsdb.SeriesElem, len(itrs)),
		itrs: itrs,
	}
}

// seriesMergeIterator is an iterator that merges multiple iterators together.
type seriesMergeIterator struct {
	buf  []tsdb.SeriesElem
	itrs []tsdb.SeriesIterator
}

// Next returns the element with the next lowest name/tags across the iterators.
//
// If multiple iterators contain the same name/tags then the first is returned
// and the remaining ones are skipped.
func (itr *seriesMergeIterator) Next() tsdb.SeriesElem {
	// Find next lowest name/tags amongst the buffers.
	var name []byte
	var tags models.Tags
	for i, buf := range itr.buf {
		// Fill buffer.
		if buf == nil {
			if buf = itr.itrs[i].Next(); buf != nil {
				itr.buf[i] = buf
			} else {
				continue
			}
		}

		// If the name is not set the pick the first non-empty name.
		if name == nil {
			name, tags = buf.Name(), buf.Tags()
			continue
		}

		// Set name/tags if they are lower than what has been seen.
		if cmp := bytes.Compare(buf.Name(), name); cmp == -1 || (cmp == 0 && models.CompareTags(buf.Tags(), tags) == -1) {
			name, tags = buf.Name(), buf.Tags()
		}
	}

	// Return nil if no elements remaining.
	if name == nil {
		return nil
	}

	// Refill buffer.
	var e tsdb.SeriesElem
	for i, buf := range itr.buf {
		if buf == nil || !bytes.Equal(buf.Name(), name) || models.CompareTags(buf.Tags(), tags) != 0 {
			continue
		}

		// Copy first matching buffer to the return buffer.
		if e == nil {
			e = buf
		}

		// Clear buffer.
		itr.buf[i] = nil
	}
	return e
}

// IntersectSeriesIterators returns an iterator that only returns series which
// occur in both iterators. If both series have associated expressions then
// they are combined together.
func IntersectSeriesIterators(itr0, itr1 tsdb.SeriesIterator) tsdb.SeriesIterator {
	if itr0 == nil || itr1 == nil {
		return nil
	}

	return &seriesIntersectIterator{itrs: [2]tsdb.SeriesIterator{itr0, itr1}}
}

// seriesIntersectIterator is an iterator that merges two iterators together.
type seriesIntersectIterator struct {
	e    seriesExprElem
	buf  [2]tsdb.SeriesElem
	itrs [2]tsdb.SeriesIterator
}

// Next returns the next element which occurs in both iterators.
func (itr *seriesIntersectIterator) Next() (e tsdb.SeriesElem) {
	for {
		// Fill buffers.
		if itr.buf[0] == nil {
			itr.buf[0] = itr.itrs[0].Next()
		}
		if itr.buf[1] == nil {
			itr.buf[1] = itr.itrs[1].Next()
		}

		// Exit if either buffer is still empty.
		if itr.buf[0] == nil || itr.buf[1] == nil {
			return nil
		}

		// Skip if both series are not equal.
		if cmp := CompareSeriesElem(itr.buf[0], itr.buf[1]); cmp == -1 {
			itr.buf[0] = nil
			continue
		} else if cmp == 1 {
			itr.buf[1] = nil
			continue
		}

		// Merge series together if equal.
		itr.e.SeriesElem = itr.buf[0]

		// Attach expression.
		expr0 := itr.buf[0].Expr()
		expr1 := itr.buf[1].Expr()
		if expr0 == nil {
			itr.e.expr = expr1
		} else if expr1 == nil {
			itr.e.expr = expr0
		} else {
			itr.e.expr = influxql.Reduce(&influxql.BinaryExpr{
				Op:  influxql.AND,
				LHS: expr0,
				RHS: expr1,
			}, nil)
		}

		itr.buf[0], itr.buf[1] = nil, nil
		return &itr.e
	}
}

// UnionSeriesIterators returns an iterator that returns series from both
// both iterators. If both series have associated expressions then they are
// combined together.
func UnionSeriesIterators(itr0, itr1 tsdb.SeriesIterator) tsdb.SeriesIterator {
	// Return other iterator if either one is nil.
	if itr0 == nil {
		return itr1
	} else if itr1 == nil {
		return itr0
	}

	return &seriesUnionIterator{itrs: [2]tsdb.SeriesIterator{itr0, itr1}}
}

// seriesUnionIterator is an iterator that unions two iterators together.
type seriesUnionIterator struct {
	e    seriesExprElem
	buf  [2]tsdb.SeriesElem
	itrs [2]tsdb.SeriesIterator
}

// Next returns the next element which occurs in both iterators.
func (itr *seriesUnionIterator) Next() (e tsdb.SeriesElem) {
	// Fill buffers.
	if itr.buf[0] == nil {
		itr.buf[0] = itr.itrs[0].Next()
	}
	if itr.buf[1] == nil {
		itr.buf[1] = itr.itrs[1].Next()
	}

	// Return the other iterator if either one is empty.
	if itr.buf[0] == nil {
		e, itr.buf[1] = itr.buf[1], nil
		return e
	} else if itr.buf[1] == nil {
		e, itr.buf[0] = itr.buf[0], nil
		return e
	}

	// Return lesser series.
	if cmp := CompareSeriesElem(itr.buf[0], itr.buf[1]); cmp == -1 {
		e, itr.buf[0] = itr.buf[0], nil
		return e
	} else if cmp == 1 {
		e, itr.buf[1] = itr.buf[1], nil
		return e
	}

	// Attach element.
	itr.e.SeriesElem = itr.buf[0]

	// Attach expression.
	expr0 := itr.buf[0].Expr()
	expr1 := itr.buf[1].Expr()
	if expr0 != nil && expr1 != nil {
		itr.e.expr = influxql.Reduce(&influxql.BinaryExpr{
			Op:  influxql.OR,
			LHS: expr0,
			RHS: expr1,
		}, nil)
	} else {
		itr.e.expr = nil
	}

	itr.buf[0], itr.buf[1] = nil, nil
	return &itr.e
}

// DifferenceSeriesIterators returns an iterator that only returns series which
// occur the first iterator but not the second iterator.
func DifferenceSeriesIterators(itr0, itr1 tsdb.SeriesIterator) tsdb.SeriesIterator {
	if itr0 != nil && itr1 == nil {
		return itr0
	} else if itr0 == nil {
		return nil
	}
	return &seriesDifferenceIterator{itrs: [2]tsdb.SeriesIterator{itr0, itr1}}
}

// seriesDifferenceIterator is an iterator that merges two iterators together.
type seriesDifferenceIterator struct {
	buf  [2]tsdb.SeriesElem
	itrs [2]tsdb.SeriesIterator
}

// Next returns the next element which occurs only in the first iterator.
func (itr *seriesDifferenceIterator) Next() (e tsdb.SeriesElem) {
	for {
		// Fill buffers.
		if itr.buf[0] == nil {
			itr.buf[0] = itr.itrs[0].Next()
		}
		if itr.buf[1] == nil {
			itr.buf[1] = itr.itrs[1].Next()
		}

		// Exit if first buffer is still empty.
		if itr.buf[0] == nil {
			return nil
		} else if itr.buf[1] == nil {
			e, itr.buf[0] = itr.buf[0], nil
			return e
		}

		// Return first series if it's less.
		// If second series is less then skip it.
		// If both series are equal then skip both.
		if cmp := CompareSeriesElem(itr.buf[0], itr.buf[1]); cmp == -1 {
			e, itr.buf[0] = itr.buf[0], nil
			return e
		} else if cmp == 1 {
			itr.buf[1] = nil
			continue
		} else {
			itr.buf[0], itr.buf[1] = nil, nil
			continue
		}
	}
}

// filterUndeletedSeriesIterator returns all series which are not deleted.
type filterUndeletedSeriesIterator struct {
	itr tsdb.SeriesIterator
}

// FilterUndeletedSeriesIterator returns an iterator which filters all deleted series.
func FilterUndeletedSeriesIterator(itr tsdb.SeriesIterator) tsdb.SeriesIterator {
	if itr == nil {
		return nil
	}
	return &filterUndeletedSeriesIterator{itr: itr}
}

func (itr *filterUndeletedSeriesIterator) Next() tsdb.SeriesElem {
	for {
		e := itr.itr.Next()
		if e == nil {
			return nil
		} else if e.Deleted() {
			continue
		}
		return e
	}
}

// seriesExprElem holds a series and its associated filter expression.
type seriesExprElem struct {
	tsdb.SeriesElem
	expr influxql.Expr
}

// Expr returns the associated expression.
func (e *seriesExprElem) Expr() influxql.Expr { return e.expr }

// seriesExprIterator is an iterator that attaches an associated expression.
type seriesExprIterator struct {
	itr tsdb.SeriesIterator
	e   seriesExprElem
}

// newSeriesExprIterator returns a new instance of seriesExprIterator.
func newSeriesExprIterator(itr tsdb.SeriesIterator, expr influxql.Expr) tsdb.SeriesIterator {
	if itr == nil {
		return nil
	}

	return &seriesExprIterator{
		itr: itr,
		e: seriesExprElem{
			expr: expr,
		},
	}
}

// Next returns the next element in the iterator.
func (itr *seriesExprIterator) Next() tsdb.SeriesElem {
	itr.e.SeriesElem = itr.itr.Next()
	if itr.e.SeriesElem == nil {
		return nil
	}
	return &itr.e
}

// seriesIDIterator represents a iterator over a list of series ids.
type seriesIDIterator interface {
	next() uint32
}

// writeTo writes write v into w. Updates n.
func writeTo(w io.Writer, v []byte, n *int64) error {
	nn, err := w.Write(v)
	*n += int64(nn)
	return err
}

// writeUint8To writes write v into w. Updates n.
func writeUint8To(w io.Writer, v uint8, n *int64) error {
	nn, err := w.Write([]byte{v})
	*n += int64(nn)
	return err
}

// writeUint16To writes write v into w using big endian encoding. Updates n.
func writeUint16To(w io.Writer, v uint16, n *int64) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], v)
	nn, err := w.Write(buf[:])
	*n += int64(nn)
	return err
}

// writeUint32To writes write v into w using big endian encoding. Updates n.
func writeUint32To(w io.Writer, v uint32, n *int64) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	nn, err := w.Write(buf[:])
	*n += int64(nn)
	return err
}

// writeUint64To writes write v into w using big endian encoding. Updates n.
func writeUint64To(w io.Writer, v uint64, n *int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	nn, err := w.Write(buf[:])
	*n += int64(nn)
	return err
}

// writeUvarintTo writes write v into w using variable length encoding. Updates n.
func writeUvarintTo(w io.Writer, v uint64, n *int64) error {
	var buf [binary.MaxVarintLen64]byte
	i := binary.PutUvarint(buf[:], v)
	nn, err := w.Write(buf[:i])
	*n += int64(nn)
	return err
}

type uint32Slice []uint32

func (a uint32Slice) Len() int           { return len(a) }
func (a uint32Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint32Slice) Less(i, j int) bool { return a[i] < a[j] }

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }

type byteSlices [][]byte

func (a byteSlices) Len() int           { return len(a) }
func (a byteSlices) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byteSlices) Less(i, j int) bool { return bytes.Compare(a[i], a[j]) == -1 }

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

// hexdump is a helper for dumping binary data to stderr.
func hexdump(data []byte) { os.Stderr.Write([]byte(hex.Dump(data))) }
