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
type MeasurementElem struct {
	Deleted bool
	Name    []byte
}

// MeasurementElems represents a list of MeasurementElem.
type MeasurementElems []MeasurementElem

func (a MeasurementElems) Len() int           { return len(a) }
func (a MeasurementElems) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a MeasurementElems) Less(i, j int) bool { return bytes.Compare(a[i].Name, a[j].Name) == -1 }

// MeasurementIterator represents a iterator over a list of measurements.
type MeasurementIterator interface {
	Next() *MeasurementElem
}

// NewMeasurementIterator returns an iterator that operates on an in-memory slice.
func NewMeasurementIterator(elems []MeasurementElem) MeasurementIterator {
	return &measurementIterator{elems: elems}
}

// measurementIterator represents an iterator over a slice of measurements.
type measurementIterator struct {
	elems []MeasurementElem
}

// Next shifts the next element off the list.
func (itr *measurementIterator) Next() (e *MeasurementElem) {
	if len(itr.elems) == 0 {
		return nil
	}
	e, itr.elems = &itr.elems[0], itr.elems[1:]
	return e
}

// MergeMeasurementIterators returns an iterator that merges a set of iterators.
// Iterators that are first in the list take precendence and a deletion by those
// early iterators will invalidate elements by later iterators.
func MergeMeasurementIterators(itrs ...MeasurementIterator) MeasurementIterator {
	itr := &measurementMergeIterator{
		buf:  make([]MeasurementElem, len(itrs)),
		itrs: itrs,
	}

	// Initialize buffers.
	for i := range itr.itrs {
		if e := itr.itrs[i].Next(); e != nil {
			itr.buf[i] = *e
		}
	}

	return itr
}

type measurementMergeIterator struct {
	e    MeasurementElem
	buf  []MeasurementElem
	itrs []MeasurementIterator
}

// Next returns the element with the next lowest name across the iterators.
//
// If multiple iterators contain the same name then the first is returned
// and the remaining ones are skipped.
func (itr *measurementMergeIterator) Next() *MeasurementElem {
	itr.e = MeasurementElem{}

	// Find next lowest name amongst the buffers.
	var name []byte
	for i := range itr.buf {
		if len(itr.buf[i].Name) == 0 {
			continue
		} else if name == nil || bytes.Compare(itr.buf[i].Name, name) == -1 {
			name = itr.buf[i].Name
		}
	}

	// Return nil if no elements remaining.
	if len(name) == 0 {
		return nil
	}

	// Refill buffer.
	for i := range itr.buf {
		if !bytes.Equal(itr.buf[i].Name, name) {
			continue
		}

		// Copy first matching buffer to the return buffer.
		if len(itr.e.Name) == 0 {
			itr.e = itr.buf[i]
		}

		// Fill buffer with next element.
		if e := itr.itrs[i].Next(); e != nil {
			itr.buf[i] = *e
		} else {
			itr.buf[i] = MeasurementElem{}
		}
	}

	return &itr.e
}

// TagKeyElem represents a generic tag key element.
type TagKeyElem struct {
	Key     []byte
	Deleted bool
}

// TagKeyIterator represents a iterator over a list of tag keys.
type TagKeyIterator interface {
	Next() *TagKeyElem
}

// NewTagKeyIterator returns an iterator that operates on an in-memory slice.
func NewTagKeyIterator(a []TagKeyElem) TagKeyIterator {
	return &tagKeyIterator{elems: a}
}

// tagKeyIterator represents an iterator over a slice of tag keys.
type tagKeyIterator struct {
	elems []TagKeyElem
}

// Next returns the next element.
func (itr *tagKeyIterator) Next() (e *TagKeyElem) {
	if len(itr.elems) == 0 {
		return nil
	}
	e, itr.elems = &itr.elems[0], itr.elems[1:]
	return e
}

// MergeTagKeyIterators returns an iterator that merges a set of iterators.
// Iterators that are first in the list take precendence and a deletion by those
// early iterators will invalidate elements by later iterators.
func MergeTagKeyIterators(itrs ...TagKeyIterator) TagKeyIterator {
	itr := &tagKeyMergeIterator{
		buf:  make([]TagKeyElem, len(itrs)),
		itrs: itrs,
	}

	// Initialize buffers.
	for i := range itr.itrs {
		if e := itr.itrs[i].Next(); e != nil {
			itr.buf[i] = *e
		}
	}

	return itr
}

type tagKeyMergeIterator struct {
	e    TagKeyElem
	buf  []TagKeyElem
	itrs []TagKeyIterator
}

// Next returns the element with the next lowest key across the iterators.
//
// If multiple iterators contain the same key then the first is returned
// and the remaining ones are skipped.
func (itr *tagKeyMergeIterator) Next() *TagKeyElem {
	itr.e = TagKeyElem{}

	// Find next lowest key amongst the buffers.
	var key []byte
	for i := range itr.buf {
		if len(itr.buf[i].Key) == 0 {
			continue
		} else if key == nil || bytes.Compare(itr.buf[i].Key, key) == -1 {
			key = itr.buf[i].Key
		}
	}

	// Return nil if no elements remaining.
	if len(key) == 0 {
		return nil
	}

	// Refill buffer.
	for i := range itr.buf {
		if !bytes.Equal(itr.buf[i].Key, key) {
			continue
		}

		// Copy first matching buffer to the return buffer.
		if len(itr.e.Key) == 0 {
			itr.e = itr.buf[i]
		}

		// Fill buffer with next element.
		if e := itr.itrs[i].Next(); e != nil {
			itr.buf[i] = *e
		} else {
			itr.buf[i] = TagKeyElem{}
		}
	}

	return &itr.e
}

// TagValueElem represents a generic tag value element.
type TagValueElem struct {
	Value   []byte
	Deleted bool
}

// TagValueIterator represents a iterator over a list of tag values.
type TagValueIterator interface {
	Next() *TagValueElem
}

// NewTagValueIterator returns an iterator that operates on an in-memory slice.
func NewTagValueIterator(a []TagValueElem) TagValueIterator {
	return &tagValueIterator{elems: a}
}

// tagValueIterator represents an iterator over a slice of tag values.
type tagValueIterator struct {
	elems []TagValueElem
}

// Next returns the next element.
func (itr *tagValueIterator) Next() (e *TagValueElem) {
	if len(itr.elems) == 0 {
		return nil
	}
	e, itr.elems = &itr.elems[0], itr.elems[1:]
	return e
}

// MergeTagValueIterators returns an iterator that merges a set of iterators.
// Iterators that are first in the list take precendence and a deletion by those
// early iterators will invalidate elements by later iterators.
func MergeTagValueIterators(itrs ...TagValueIterator) TagValueIterator {
	itr := &tagValueMergeIterator{
		buf:  make([]TagValueElem, len(itrs)),
		itrs: itrs,
	}

	// Initialize buffers.
	for i := range itr.itrs {
		if e := itr.itrs[i].Next(); e != nil {
			itr.buf[i] = *e
		}
	}

	return itr
}

type tagValueMergeIterator struct {
	e    TagValueElem
	buf  []TagValueElem
	itrs []TagValueIterator
}

// Next returns the element with the next lowest value across the iterators.
//
// If multiple iterators contain the same value then the first is returned
// and the remaining ones are skipped.
func (itr *tagValueMergeIterator) Next() *TagValueElem {
	itr.e = TagValueElem{}

	// Find next lowest value amongst the buffers.
	var value []byte
	for i := range itr.buf {
		if len(itr.buf[i].Value) == 0 {
			continue
		} else if value == nil || bytes.Compare(itr.buf[i].Value, value) == -1 {
			value = itr.buf[i].Value
		}
	}

	// Return nil if no elements remaining.
	if len(value) == 0 {
		return nil
	}

	// Refill buffer.
	for i := range itr.buf {
		if !bytes.Equal(itr.buf[i].Value, value) {
			continue
		}

		// Copy first matching buffer to the return buffer.
		if len(itr.e.Value) == 0 {
			itr.e = itr.buf[i]
		}

		// Fill buffer with next element.
		if e := itr.itrs[i].Next(); e != nil {
			itr.buf[i] = *e
		} else {
			itr.buf[i] = TagValueElem{}
		}
	}

	return &itr.e
}

// SeriesElem represents a generic series element.
type SeriesElem struct {
	Name    []byte
	Tags    models.Tags
	Deleted bool
}

// SeriesIterator represents a iterator over a list of series.
type SeriesIterator interface {
	Next() *SeriesElem
}

// NewSeriesIterator returns an iterator that operates on an in-memory slice.
func NewSeriesIterator(a []SeriesElem) SeriesIterator {
	return &seriesIterator{elems: a}
}

// seriesIterator represents an iterator over a slice of tag values.
type seriesIterator struct {
	elems []SeriesElem
}

// Next returns the next element.
func (itr *seriesIterator) Next() (e *SeriesElem) {
	if len(itr.elems) == 0 {
		return nil
	}
	e, itr.elems = &itr.elems[0], itr.elems[1:]
	return e
}

// MergeSeriesIterators returns an iterator that merges a set of iterators.
// Iterators that are first in the list take precendence and a deletion by those
// early iterators will invalidate elements by later iterators.
func MergeSeriesIterators(itrs ...SeriesIterator) SeriesIterator {
	itr := &seriesMergeIterator{
		buf:  make([]SeriesElem, len(itrs)),
		itrs: itrs,
	}

	// Initialize buffers.
	for i := range itr.itrs {
		if e := itr.itrs[i].Next(); e != nil {
			itr.buf[i] = *e
		}
	}

	return itr
}

type seriesMergeIterator struct {
	e    SeriesElem
	buf  []SeriesElem
	itrs []SeriesIterator
}

// Next returns the element with the next lowest name/tags across the iterators.
//
// If multiple iterators contain the same name/tags then the first is returned
// and the remaining ones are skipped.
func (itr *seriesMergeIterator) Next() *SeriesElem {
	itr.e = SeriesElem{}

	// Find next lowest name/tags amongst the buffers.
	var name []byte
	var tags models.Tags
	for i := range itr.buf {
		// Skip empty buffers.
		if len(itr.buf[i].Name) == 0 {
			continue
		}

		// If the name is not set the pick the first non-empty name.
		if name == nil {
			name, tags = itr.buf[i].Name, itr.buf[i].Tags
			continue
		}

		// Set name/tags if they are lower than what has been seen.
		if cmp := bytes.Compare(itr.buf[i].Name, name); cmp == -1 || (cmp == 0 && models.CompareTags(itr.buf[i].Tags, tags) == -1) {
			name, tags = itr.buf[i].Name, itr.buf[i].Tags
		}
	}

	// Return nil if no elements remaining.
	if len(name) == 0 {
		return nil
	}

	// Refill buffer.
	for i := range itr.buf {
		if !bytes.Equal(itr.buf[i].Name, name) || models.CompareTags(itr.buf[i].Tags, tags) != 0 {
			continue
		}

		// Copy first matching buffer to the return buffer.
		if len(itr.e.Name) == 0 {
			itr.e = itr.buf[i]
		}

		// Fill buffer with next element.
		if e := itr.itrs[i].Next(); e != nil {
			itr.buf[i] = *e
		} else {
			itr.buf[i] = SeriesElem{}
		}
	}

	return &itr.e
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
	addr := 0
	for len(data) > 0 {
		n := len(data)
		if n > 16 {
			n = 16
		}

		fmt.Fprintf(os.Stderr, "%07x % x\n", addr, data[:n])

		data = data[n:]
		addr += n
	}
	fmt.Fprintln(os.Stderr, "")
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
