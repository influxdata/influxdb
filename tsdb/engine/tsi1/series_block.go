package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
)

// ErrSeriesOverflow is returned when too many series are added to a series writer.
var ErrSeriesOverflow = errors.New("series overflow")

// Series list field size constants.
const (
	// Series list trailer field sizes.
	TermListOffsetSize   = 8
	TermListSizeSize     = 8
	SeriesDataOffsetSize = 8
	SeriesDataSizeSize   = 8

	SeriesBlockTrailerSize = TermListOffsetSize +
		TermListSizeSize +
		SeriesDataOffsetSize +
		SeriesDataSizeSize

	// Other field sizes
	TermCountSize   = 4
	SeriesCountSize = 4
	SeriesIDSize    = 4
)

// Series flag constants.
const (
	SeriesTombstoneFlag = 0x01
)

// SeriesBlock represents the section of the index which holds the term
// dictionary and a sorted list of series keys.
type SeriesBlock struct {
	data       []byte
	termData   []byte
	seriesData []byte
}

// Series returns a series element.
func (blk *SeriesBlock) Series(name []byte, tags models.Tags) SeriesElem {
	panic("TODO: Add hashmap to series block")
	panic("TODO: Lookup series by hashmap")
}

// SeriesOffset returns offset of the encoded series key.
// Returns 0 if the key does not exist in the series list.
func (blk *SeriesBlock) SeriesOffset(key []byte) (offset uint32, deleted bool) {
	offset = uint32(len(blk.termData) + SeriesCountSize)
	data := blk.seriesData[SeriesCountSize:]

	for i, n := uint32(0), blk.SeriesCount(); i < n; i++ {
		// Read series flag.
		flag := data[0]
		data = data[1:]

		// Read series length.
		ln, sz := binary.Uvarint(data)
		data = data[sz:]

		// Return offset if the series key matches.
		if bytes.Equal(key, data[:ln]) {
			deleted = (flag & SeriesTombstoneFlag) != 0
			return offset, deleted
		}

		// Update offset & move data forward.
		data = data[ln:]
		offset += uint32(ln) + uint32(sz)
	}

	return 0, false
}

// EncodeSeries returns a dictionary-encoded series key.
func (blk *SeriesBlock) EncodeSeries(name []byte, tags models.Tags) []byte {
	// Build a buffer with the minimum space for the name, tag count, and tags.
	buf := make([]byte, 2+len(tags))
	return blk.AppendEncodeSeries(buf[:0], name, tags)
}

// AppendEncodeSeries appends an encoded series value to dst and returns the new slice.
func (blk *SeriesBlock) AppendEncodeSeries(dst []byte, name []byte, tags models.Tags) []byte {
	var buf [binary.MaxVarintLen32]byte

	// Append encoded name.
	n := binary.PutUvarint(buf[:], uint64(blk.EncodeTerm(name)))
	dst = append(dst, buf[:n]...)

	// Append encoded tag count.
	n = binary.PutUvarint(buf[:], uint64(len(tags)))
	dst = append(dst, buf[:n]...)

	// Append tags.
	for _, t := range tags {
		n := binary.PutUvarint(buf[:], uint64(blk.EncodeTerm(t.Key)))
		dst = append(dst, buf[:n]...)

		n = binary.PutUvarint(buf[:], uint64(blk.EncodeTerm(t.Value)))
		dst = append(dst, buf[:n]...)
	}

	return dst
}

// decodeElemAt decodes a series element at a offset.
func (blk *SeriesBlock) decodeElemAt(offset uint32, e *seriesBlockElem) {
	data := blk.data[offset:]

	// Read flag.
	e.flag, data = data[0], data[1:]
	e.size = 1

	// Read length.
	n, sz := binary.Uvarint(data)
	data = data[sz:]
	e.size += int64(sz) + int64(n)

	// Decode the name and tags into the element.
	blk.DecodeSeries(data[:n], &e.name, &e.tags)
}

// DecodeSeries decodes a dictionary encoded series into a name and tagset.
func (blk *SeriesBlock) DecodeSeries(buf []byte, name *[]byte, tags *models.Tags) {
	// Read name.
	offset, n := binary.Uvarint(buf)
	*name, buf = blk.DecodeTerm(uint32(offset)), buf[n:]

	// Read tag count.
	tagN, n := binary.Uvarint(buf)
	buf = buf[n:]

	// Clear tags, if necessary.
	if len(*tags) > 0 {
		*tags = (*tags)[:0]
	}

	// Loop over tag key/values.
	for i := 0; i < int(tagN); i++ {
		// Read key.
		offset, n := binary.Uvarint(buf)
		key := blk.DecodeTerm(uint32(offset))
		buf = buf[n:]

		// Read value.
		offset, n = binary.Uvarint(buf)
		value := blk.DecodeTerm(uint32(offset))
		buf = buf[n:]

		// Add to tagset.
		tags.Set(key, value)
	}

	// Ensure that the whole slice was read.
	if len(buf) != 0 {
		panic(fmt.Sprintf("remaining unmarshaled data in series: % x", buf))
	}
}

// DecodeTerm returns the term at the given offset.
func (blk *SeriesBlock) DecodeTerm(offset uint32) []byte {
	buf := blk.termData[offset:]

	// Read length at offset.
	i, n := binary.Uvarint(buf)
	buf = buf[n:]

	// Return term data.
	return buf[:i]
}

// EncodeTerm returns the offset of v within data.
func (blk *SeriesBlock) EncodeTerm(v []byte) uint32 {
	offset := uint32(TermCountSize)
	data := blk.termData[offset:]

	for i, n := uint32(0), blk.TermCount(); i < n; i++ {
		// Read term length.
		ln, sz := binary.Uvarint(data)
		data = data[sz:]

		// Return offset if the term matches.
		if bytes.Equal(v, data[:ln]) {
			return offset
		}

		// Update offset & move data forward.
		data = data[ln:]
		offset += uint32(ln) + uint32(sz)
	}

	return 0
}

// TermCount returns the number of terms within the dictionary.
func (blk *SeriesBlock) TermCount() uint32 {
	return binary.BigEndian.Uint32(blk.termData[:TermCountSize])
}

// SeriesCount returns the number of series.
func (blk *SeriesBlock) SeriesCount() uint32 {
	return binary.BigEndian.Uint32(blk.seriesData[:SeriesCountSize])
}

// SeriesIterator returns an iterator over all the series.
func (blk *SeriesBlock) SeriesIterator() SeriesIterator {
	return &seriesBlockIterator{
		n:      blk.SeriesCount(),
		offset: uint32(len(blk.termData) + SeriesCountSize),
		sblk:   blk,
	}
}

// UnmarshalBinary unpacks data into the series list.
//
// If data is an mmap then it should stay open until the series list is no
// longer used because data access is performed directly from the byte slice.
func (blk *SeriesBlock) UnmarshalBinary(data []byte) error {
	t := ReadSeriesBlockTrailer(data)

	// Save entire block.
	blk.data = data

	// Slice term list data.
	blk.termData = data[t.TermList.Offset:]
	blk.termData = blk.termData[:t.TermList.Size]

	// Slice series data data.
	blk.seriesData = data[t.SeriesData.Offset:]
	blk.seriesData = blk.seriesData[:t.SeriesData.Size]

	return nil
}

// seriesBlockIterator is an iterator over a series ids in a series list.
type seriesBlockIterator struct {
	i, n   uint32
	offset uint32
	sblk   *SeriesBlock
	e      seriesBlockElem // buffer
}

// Next returns the next series element.
func (itr *seriesBlockIterator) Next() SeriesElem {
	// Exit if at the end.
	if itr.i == itr.n {
		return nil
	}

	// Read next element.
	itr.sblk.decodeElemAt(itr.offset, &itr.e)

	// Move iterator and offset forward.
	itr.i++
	itr.offset += uint32(itr.e.size)

	return &itr.e
}

// seriesDecodeIterator decodes a series id iterator into unmarshaled elements.
type seriesDecodeIterator struct {
	itr  seriesIDIterator
	sblk *SeriesBlock
	e    seriesBlockElem // buffer
}

// newSeriesDecodeIterator returns a new instance of seriesDecodeIterator.
func newSeriesDecodeIterator(sblk *SeriesBlock) seriesDecodeIterator {
	return seriesDecodeIterator{sblk: sblk}
}

// Next returns the next series element.
func (itr *seriesDecodeIterator) Next() SeriesElem {
	// Read next series id.
	id := itr.itr.next()
	if id == 0 {
		return nil
	}

	// Read next element.
	itr.sblk.decodeElemAt(id, &itr.e)
	return &itr.e
}

// seriesBlockElem represents a series element in the series list.
type seriesBlockElem struct {
	flag byte
	name []byte
	tags models.Tags
	size int64
}

// Deleted returns true if the tombstone flag is set.
func (e *seriesBlockElem) Deleted() bool { return (e.flag & SeriesTombstoneFlag) != 0 }

// Name returns the measurement name.
func (e *seriesBlockElem) Name() []byte { return e.name }

// Tags returns the tag set.
func (e *seriesBlockElem) Tags() models.Tags { return e.tags }

// Expr always returns a nil expression.
// This is only used by higher level query planning.
func (e *seriesBlockElem) Expr() influxql.Expr { return nil }

// SeriesBlockWriter writes a SeriesBlock.
type SeriesBlockWriter struct {
	terms  map[string]int // term frequency
	series []serie        // series list

	// Term list is available after writer has been written.
	termList *TermList
}

// NewSeriesBlockWriter returns a new instance of SeriesBlockWriter.
func NewSeriesBlockWriter() *SeriesBlockWriter {
	return &SeriesBlockWriter{
		terms: make(map[string]int),
	}
}

// Add adds a series to the writer's set.
// Returns an ErrSeriesOverflow if no more series can be held in the writer.
func (sw *SeriesBlockWriter) Add(name []byte, tags models.Tags) error {
	return sw.append(name, tags, false)
}

// Delete marks a series as tombstoned.
func (sw *SeriesBlockWriter) Delete(name []byte, tags models.Tags) error {
	return sw.append(name, tags, true)
}

func (sw *SeriesBlockWriter) append(name []byte, tags models.Tags, deleted bool) error {
	// Ensure writer doesn't add too many series.
	if len(sw.series) == math.MaxInt32 {
		return ErrSeriesOverflow
	}

	// Increment term counts.
	sw.terms[string(name)]++
	for _, t := range tags {
		sw.terms[string(t.Key)]++
		sw.terms[string(t.Value)]++
	}

	// Append series to list.
	sw.series = append(sw.series, serie{
		name:    copyBytes(name),
		tags:    models.CopyTags(tags),
		deleted: deleted,
	})

	return nil
}

// WriteTo computes the dictionary encoding of the series and writes to w.
func (sw *SeriesBlockWriter) WriteTo(w io.Writer) (n int64, err error) {
	var t SeriesBlockTrailer

	terms := NewTermList(sw.terms)

	// Write term dictionary.
	t.TermList.Offset = n
	nn, err := sw.writeTermListTo(w, terms)
	n += nn
	if err != nil {
		return n, err
	}
	t.TermList.Size = n - t.TermList.Offset

	// Write dictionary-encoded series list.
	t.SeriesData.Offset = n
	nn, err = sw.writeSeriesTo(w, terms, uint32(n))
	n += nn
	if err != nil {
		return n, err
	}
	t.SeriesData.Size = n - t.SeriesData.Offset

	// Write trailer.
	if err := sw.writeTrailerTo(w, t, &n); err != nil {
		return n, err
	}

	// Save term list for future encoding.
	sw.termList = terms

	return n, nil
}

// writeTermListTo writes the terms to w.
func (sw *SeriesBlockWriter) writeTermListTo(w io.Writer, terms *TermList) (n int64, err error) {
	buf := make([]byte, binary.MaxVarintLen32)

	// Write term count.
	binary.BigEndian.PutUint32(buf[:4], uint32(terms.Len()))
	nn, err := w.Write(buf[:4])
	n += int64(nn)
	if err != nil {
		return n, err
	}

	// Write terms.
	for i := range terms.a {
		e := &terms.a[i]

		// Ensure that we can reference the offset using a uint32.
		if n > math.MaxUint32 {
			return n, errors.New("series dictionary exceeded max size")
		}

		// Track starting offset of the term.
		e.offset = uint32(n)

		// Join varint(length) & term in buffer.
		sz := binary.PutUvarint(buf, uint64(len(e.term)))
		buf = append(buf[:sz], e.term...)

		// Write buffer to writer.
		nn, err := w.Write(buf)
		n += int64(nn)
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

// writeSeriesTo writes dictionary-encoded series to w in sorted order.
func (sw *SeriesBlockWriter) writeSeriesTo(w io.Writer, terms *TermList, offset uint32) (n int64, err error) {
	buf := make([]byte, binary.MaxVarintLen32+1)

	// Ensure series are sorted.
	sort.Sort(series(sw.series))

	// Write series count.
	binary.BigEndian.PutUint32(buf[:4], uint32(len(sw.series)))
	nn, err := w.Write(buf[:4])
	n += int64(nn)
	if err != nil {
		return n, err
	}

	// Write series.
	var seriesBuf []byte
	for i := range sw.series {
		s := &sw.series[i]

		// Ensure that we can reference the series using a uint32.
		if int64(offset)+n > math.MaxUint32 {
			return n, errors.New("series list exceeded max size")
		}

		// Track offset of the series.
		s.offset = uint32(offset + uint32(n))

		// Write encoded series to a separate buffer.
		seriesBuf = terms.AppendEncodedSeries(seriesBuf[:0], s.name, s.tags)

		// Join flag, varint(length), & dictionary-encoded series in buffer.
		buf[0] = 0 // TODO(benbjohnson): series tombstone
		sz := binary.PutUvarint(buf[1:], uint64(len(seriesBuf)))
		buf = append(buf[:1+sz], seriesBuf...)

		// Write buffer to writer.
		nn, err := w.Write(buf)
		n += int64(nn)
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

// writeTrailerTo writes offsets to the end of the series list.
func (sw *SeriesBlockWriter) writeTrailerTo(w io.Writer, t SeriesBlockTrailer, n *int64) error {
	if err := writeUint64To(w, uint64(t.TermList.Offset), n); err != nil {
		return err
	} else if err := writeUint64To(w, uint64(t.TermList.Size), n); err != nil {
		return err
	}

	if err := writeUint64To(w, uint64(t.SeriesData.Offset), n); err != nil {
		return err
	} else if err := writeUint64To(w, uint64(t.SeriesData.Size), n); err != nil {
		return err
	}

	return nil
}

// Offset returns the series offset from the writer.
// Only valid after the series list has been written to a writer.
func (sw *SeriesBlockWriter) Offset(name []byte, tags models.Tags) uint32 {
	// Find position of series.
	i := sort.Search(len(sw.series), func(i int) bool {
		s := &sw.series[i]
		if cmp := bytes.Compare(s.name, name); cmp != 0 {
			return cmp != -1
		}
		return models.CompareTags(s.tags, tags) != -1
	})

	// Ignore if it's not an exact match.
	if i >= len(sw.series) {
		return 0
	} else if s := &sw.series[i]; !bytes.Equal(s.name, name) || !s.tags.Equal(tags) {
		return 0
	}

	// Return offset & deleted flag of series.
	return sw.series[i].offset
}

// ReadSeriesBlockTrailer returns the series list trailer from data.
func ReadSeriesBlockTrailer(data []byte) SeriesBlockTrailer {
	var t SeriesBlockTrailer

	// Slice trailer data.
	buf := data[len(data)-SeriesBlockTrailerSize:]

	// Read term list info.
	t.TermList.Offset = int64(binary.BigEndian.Uint64(buf[0:TermListOffsetSize]))
	buf = buf[TermListOffsetSize:]
	t.TermList.Size = int64(binary.BigEndian.Uint64(buf[0:TermListSizeSize]))
	buf = buf[TermListSizeSize:]

	// Read series data info.
	t.SeriesData.Offset = int64(binary.BigEndian.Uint64(buf[0:SeriesDataOffsetSize]))
	buf = buf[SeriesDataOffsetSize:]
	t.SeriesData.Size = int64(binary.BigEndian.Uint64(buf[0:SeriesDataSizeSize]))
	buf = buf[SeriesDataSizeSize:]

	return t
}

// SeriesBlockTrailer represents meta data written to the end of the series list.
type SeriesBlockTrailer struct {
	TermList struct {
		Offset int64
		Size   int64
	}
	SeriesData struct {
		Offset int64
		Size   int64
	}
}

type serie struct {
	name    []byte
	tags    models.Tags
	deleted bool
	offset  uint32
}

type series []serie

func (a series) Len() int      { return len(a) }
func (a series) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a series) Less(i, j int) bool {
	if cmp := bytes.Compare(a[i].name, a[j].name); cmp != 0 {
		return cmp == -1
	}
	return models.CompareTags(a[i].tags, a[j].tags) == -1
}
