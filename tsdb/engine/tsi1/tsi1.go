package tsi1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/cespare/xxhash"
	"github.com/influxdata/influxdb/models"
)

// MeasurementElem represents a generic measurement element.
type MeasurementElem interface {
	Name() []byte
	Deleted() bool
	TagKeyIterator() TagKeyIterator
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

// TagKeyIterator returns a merge iterator for all elements until a tombstone occurs.
func (p measurementMergeElem) TagKeyIterator() TagKeyIterator {
	if len(p) == 0 {
		return nil
	}

	a := make([]TagKeyIterator, 0, len(p))
	for _, e := range p {
		a = append(a, e.TagKeyIterator())
		if e.Deleted() {
			break
		}
	}
	return MergeTagKeyIterators(a...)
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
		a = append(a, e.TagValueIterator())
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
	SeriesIterator() SeriesIterator
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

// SeriesIterator returns a merge iterator for all elements until a tombstone occurs.
func (p tagValueMergeElem) SeriesIterator() SeriesIterator {
	if len(p) == 0 {
		return nil
	}

	a := make([]SeriesIterator, 0, len(p))
	for _, e := range p {
		a = append(a, e.SeriesIterator())
		if e.Deleted() {
			break
		}
	}
	return MergeSeriesIterators(a...)
}

// SeriesElem represents a generic series element.
type SeriesElem interface {
	Name() []byte
	Tags() models.Tags
	Deleted() bool
}

// SeriesIterator represents a iterator over a list of series.
type SeriesIterator interface {
	Next() SeriesElem
}

// MergeSeriesIterators returns an iterator that merges a set of iterators.
// Iterators that are first in the list take precendence and a deletion by those
// early iterators will invalidate elements by later iterators.
func MergeSeriesIterators(itrs ...SeriesIterator) SeriesIterator {
	if len(itrs) == 0 {
		return nil
	}

	itr := &seriesMergeIterator{
		buf:  make([]SeriesElem, len(itrs)),
		itrs: itrs,
	}

	// Initialize buffers.
	for i := range itr.itrs {
		itr.buf[i] = itr.itrs[i].Next()
	}

	return itr
}

type seriesMergeIterator struct {
	buf  []SeriesElem
	itrs []SeriesIterator
}

// Next returns the element with the next lowest name/tags across the iterators.
//
// If multiple iterators contain the same name/tags then the first is returned
// and the remaining ones are skipped.
func (itr *seriesMergeIterator) Next() SeriesElem {
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
	var e SeriesElem
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

// seriesIDIterator represents a iterator over a list of series ids.
type seriesIDIterator interface {
	next() uint32
}

// unionSeriesIDIterators returns an iterator returns a union of iterators.
func unionSeriesIDIterators(itrs ...seriesIDIterator) seriesIDIterator {
	if len(itrs) == 0 {
		return nil
	}

	itr := &unionIterator{
		buf:  make([]uint32, len(itrs)),
		itrs: itrs,
	}

	// Initialize buffers.
	for i := range itr.itrs {
		itr.buf[i] = itr.itrs[i].next()
	}

	return itr
}

type unionIterator struct {
	buf  []uint32
	itrs []seriesIDIterator
}

// next returns the next series id. Duplicates are combined.
func (itr *unionIterator) next() uint32 {
	// Find next series id in the buffers.
	var id uint32
	for i := range itr.buf {
		// Skip empty buffers.
		if itr.buf[i] == 0 {
			continue
		}

		// If the name is not set the pick the first non-empty name.
		if id == 0 || itr.buf[i] < id {
			id = itr.buf[i]
		}
	}

	// Return zero if no elements remaining.
	if id == 0 {
		return 0
	}

	// Refill buffer.
	for i := range itr.buf {
		if itr.buf[i] == id {
			itr.buf[i] = itr.itrs[i].next()
		}
	}
	return id
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

func Hexdump(data []byte) {
	var buf bytes.Buffer
	addr := 0
	for len(data) > 0 {
		n := len(data)
		if n > 16 {
			n = 16
		}

		fmt.Fprintf(&buf, "%07x % x\n", addr, data[:n])

		data = data[n:]
		addr += n
	}
	fmt.Fprintln(&buf, "")

	buf.WriteTo(os.Stderr)
}

// hashKey hashes a key using murmur3.
func hashKey(key []byte) uint32 {
	h := xxhash.Sum64(key)
	if h == 0 {
		h = 1
	}
	return uint32(h)
}

// dist returns the probe distance for a hash in a slot index.
func dist(hash uint32, i, capacity int) int {
	return (i + capacity - (int(hash) % capacity)) % capacity
}

type uint32Slice []uint32

func (a uint32Slice) Len() int           { return len(a) }
func (a uint32Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint32Slice) Less(i, j int) bool { return a[i] < a[j] }

type byteSlices [][]byte

func (a byteSlices) Len() int           { return len(a) }
func (a byteSlices) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byteSlices) Less(i, j int) bool { return bytes.Compare(a[i], a[j]) == -1 }

// copyBytes returns a copy of b.
func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	buf := make([]byte, len(b))
	copy(buf, b)
	return buf
}

// assert will panic with a given formatted message if the given condition is false.
func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assert failed: "+msg, v...))
	}
}
