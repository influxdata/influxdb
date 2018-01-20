package tsi1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/influxdata/influxdb/tsdb"
)

// LoadFactor is the fill percent for RHH indexes.
const LoadFactor = 80

// MeasurementElem represents a generic measurement element.
type MeasurementElem interface {
	Name() []byte
	Deleted() bool
	// HasSeries() bool
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

// tsdbMeasurementIteratorAdapter wraps MeasurementIterator to match the TSDB interface.
// This is needed because TSDB doesn't have a concept of "deleted" measurements.
type tsdbMeasurementIteratorAdapter struct {
	itr MeasurementIterator
}

// NewTSDBMeasurementIteratorAdapter return an iterator which implements tsdb.MeasurementIterator.
func NewTSDBMeasurementIteratorAdapter(itr MeasurementIterator) tsdb.MeasurementIterator {
	if itr == nil {
		return nil
	}
	return &tsdbMeasurementIteratorAdapter{itr: itr}
}

func (itr *tsdbMeasurementIteratorAdapter) Close() error { return nil }

func (itr *tsdbMeasurementIteratorAdapter) Next() ([]byte, error) {
	for {
		e := itr.itr.Next()
		if e == nil {
			return nil, nil
		} else if e.Deleted() {
			continue
		}
		return e.Name(), nil
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

// tsdbTagKeyIteratorAdapter wraps TagKeyIterator to match the TSDB interface.
// This is needed because TSDB doesn't have a concept of "deleted" tag keys.
type tsdbTagKeyIteratorAdapter struct {
	itr TagKeyIterator
}

// NewTSDBTagKeyIteratorAdapter return an iterator which implements tsdb.TagKeyIterator.
func NewTSDBTagKeyIteratorAdapter(itr TagKeyIterator) tsdb.TagKeyIterator {
	if itr == nil {
		return nil
	}
	return &tsdbTagKeyIteratorAdapter{itr: itr}
}

func (itr *tsdbTagKeyIteratorAdapter) Close() error { return nil }

func (itr *tsdbTagKeyIteratorAdapter) Next() ([]byte, error) {
	for {
		e := itr.itr.Next()
		if e == nil {
			return nil, nil
		} else if e.Deleted() {
			continue
		}
		return e.Key(), nil
	}
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

// tsdbTagValueIteratorAdapter wraps TagValueIterator to match the TSDB interface.
// This is needed because TSDB doesn't have a concept of "deleted" tag values.
type tsdbTagValueIteratorAdapter struct {
	itr TagValueIterator
}

// NewTSDBTagValueIteratorAdapter return an iterator which implements tsdb.TagValueIterator.
func NewTSDBTagValueIteratorAdapter(itr TagValueIterator) tsdb.TagValueIterator {
	if itr == nil {
		return nil
	}
	return &tsdbTagValueIteratorAdapter{itr: itr}
}

func (itr *tsdbTagValueIteratorAdapter) Close() error { return nil }

func (itr *tsdbTagValueIteratorAdapter) Next() ([]byte, error) {
	for {
		e := itr.itr.Next()
		if e == nil {
			return nil, nil
		} else if e.Deleted() {
			continue
		}
		return e.Value(), nil
	}
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

/*
type SeriesPointMergeIterator interface {
	Next() (*query.FloatPoint, error)
	Close() error
	Stats() query.IteratorStats
}

func MergeSeriesPointIterators(itrs ...*seriesPointIterator) SeriesPointMergeIterator {
	if n := len(itrs); n == 0 {
		return nil
	} else if n == 1 {
		return itrs[0]
	}

	return &seriesPointMergeIterator{
		buf:  make([]*query.FloatPoint, len(itrs)),
		itrs: itrs,
	}
}

type seriesPointMergeIterator struct {
	buf  []*query.FloatPoint
	itrs []*seriesPointIterator
}

func (itr *seriesPointMergeIterator) Close() error {
	for i := range itr.itrs {
		itr.itrs[i].Close()
	}
	return nil
}
func (itr *seriesPointMergeIterator) Stats() query.IteratorStats {
	return query.IteratorStats{}
}

func (itr *seriesPointMergeIterator) Next() (_ *query.FloatPoint, err error) {
	// Find next lowest point amongst the buffers.
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
		if key == nil || bytes.Compare(buf.Key(), key) == -1 {
			key = buf.Key()
		}
	}

	// Return nil if no elements remaining.
	if key == nil {
		return nil, nil
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

	return itr.e, nil
}
*/

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

// hexdump is a helper for dumping binary data to stderr.
// func hexdump(data []byte) { os.Stderr.Write([]byte(hex.Dump(data))) }

// stack is a helper for dumping a stack trace.
// func stack() string {
// 	return "------------------------\n" + string(debug.Stack()) + "------------------------\n\n"
// }
