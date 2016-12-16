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
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/pkg/rhh"
)

// ErrSeriesOverflow is returned when too many series are added to a series writer.
var ErrSeriesOverflow = errors.New("series overflow")

// Series list field size constants.
const (
	// Series list trailer field sizes.
	SeriesBlockTrailerSize = 0 +
		8 + 8 + // term data offset/size
		8 + 8 + // series data offset/size
		8 + 8 + // term index offset/size
		8 + 8 + // series index offset/size
		8 + 8 + // series sketch offset/size
		8 + 8 + // tombstone series sketch offset/size
		8 + 8 + // series count and tombstone count
		0

	// Other field sizes
	TermCountSize   = 4
	SeriesCountSize = 4
	TermOffsetSize  = 4
	SeriesIDSize    = 4
)

// Series flag constants.
const (
	SeriesTombstoneFlag = 0x01
)

// SeriesBlock represents the section of the index which holds the term
// dictionary and a sorted list of series keys.
type SeriesBlock struct {
	data []byte

	// Term data & index/capacity.
	termData   []byte
	termIndex  []byte
	termIndexN uint32

	// Series data & index/capacity.
	seriesData   []byte
	seriesIndex  []byte
	seriesIndexN uint32

	// Series counts.
	seriesN    int64
	tombstoneN int64

	// Series block sketch and tombstone sketch for cardinality
	// estimation.
	sketch, tsketch estimator.Sketch
}

// Series returns a series element.
func (blk *SeriesBlock) Series(name []byte, tags models.Tags) SeriesElem {
	// Dictionary encode series.
	buf := make([]byte, 20)
	buf = blk.AppendEncodeSeries(buf[:0], name, tags)

	n := blk.seriesIndexN
	hash := hashKey(buf)
	pos := int(hash % n)

	// Track current distance
	var d int
	for {
		// Find offset of series.
		offset := binary.BigEndian.Uint32(blk.seriesIndex[pos*SeriesIDSize:])

		// Evaluate encoded value matches expected.
		if offset > 0 {
			// Parse into element.
			var e SeriesBlockElem
			blk.decodeElemAt(offset, &e)

			// Return if name match.
			if bytes.Equal(e.name, name) && models.CompareTags(e.tags, tags) == 0 {
				return &e
			}

			// Check if we've exceeded the probe distance.
			if d > dist(hashKey(buf), pos, int(n)) {
				return nil
			}
		}

		// Move position forward.
		pos = (pos + 1) % int(n)
		d++

		if uint32(d) > n {
			return nil
		}
	}
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
func (blk *SeriesBlock) decodeElemAt(offset uint32, e *SeriesBlockElem) {
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

// EncodeTerm returns the offset of v within data. Returns 0 if not found.
func (blk *SeriesBlock) EncodeTerm(v []byte) uint32 {
	n := blk.termIndexN
	hash := hashKey(v)
	pos := int(hash % n)

	// Track current distance
	var d int
	for {
		// Find offset of term.
		offset := binary.BigEndian.Uint32(blk.termIndex[pos*TermOffsetSize:])

		// Evaluate encoded value matches expected.
		if offset > 0 {
			// Parse term.
			data := blk.data[offset:]
			i, sz := binary.Uvarint(data)
			data = data[sz : int(i)+sz]

			// Return if term matches.
			if bytes.Equal(data, v) {
				return offset
			}

			// Check if we've exceeded the probe distance.
			if d > dist(hashKey(data), pos, int(n)) {
				return 0
			}
		}

		// Move position forward.
		pos = (pos + 1) % int(n)
		d++

		if uint32(d) > n {
			return 0
		}
	}
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
	blk.termData = data[t.Term.Data.Offset:]
	blk.termData = blk.termData[:t.Term.Data.Size]

	// Slice term list index.
	blk.termIndex = data[t.Term.Index.Offset:]
	blk.termIndex = blk.termIndex[:t.Term.Index.Size]
	blk.termIndexN = binary.BigEndian.Uint32(blk.termIndex[:4])
	blk.termIndex = blk.termIndex[4:]

	// Slice series data.
	blk.seriesData = data[t.Series.Data.Offset:]
	blk.seriesData = blk.seriesData[:t.Series.Data.Size]

	// Slice series hash index.
	blk.seriesIndex = data[t.Series.Index.Offset:]
	blk.seriesIndex = blk.seriesIndex[:t.Series.Index.Size]
	blk.seriesIndexN = binary.BigEndian.Uint32(blk.seriesIndex[:4])
	blk.seriesIndex = blk.seriesIndex[4:]

	// Initialise sketches. We're currently using HLL+.
	var s, ts = hll.NewDefaultPlus(), hll.NewDefaultPlus()
	if err := s.UnmarshalBinary(data[t.Sketch.Offset:][:t.Sketch.Size]); err != nil {
		return err
	}
	blk.sketch = s

	if err := ts.UnmarshalBinary(data[t.TSketch.Offset:][:t.TSketch.Size]); err != nil {
		return err
	}
	blk.tsketch = ts

	// Set the series and tombstone counts
	blk.seriesN, blk.tombstoneN = t.SeriesN, t.TombstoneN

	return nil
}

// seriesBlockIterator is an iterator over a series ids in a series list.
type seriesBlockIterator struct {
	i, n   uint32
	offset uint32
	sblk   *SeriesBlock
	e      SeriesBlockElem // buffer
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
	e    SeriesBlockElem // buffer
}

// newSeriesDecodeIterator returns a new instance of seriesDecodeIterator.
func newSeriesDecodeIterator(sblk *SeriesBlock, itr seriesIDIterator) *seriesDecodeIterator {
	return &seriesDecodeIterator{sblk: sblk, itr: itr}
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

// SeriesBlockElem represents a series element in the series list.
type SeriesBlockElem struct {
	flag byte
	name []byte
	tags models.Tags
	size int64
}

// Deleted returns true if the tombstone flag is set.
func (e *SeriesBlockElem) Deleted() bool { return (e.flag & SeriesTombstoneFlag) != 0 }

// Name returns the measurement name.
func (e *SeriesBlockElem) Name() []byte { return e.name }

// Tags returns the tag set.
func (e *SeriesBlockElem) Tags() models.Tags { return e.tags }

// Expr always returns a nil expression.
// This is only used by higher level query planning.
func (e *SeriesBlockElem) Expr() influxql.Expr { return nil }

// SeriesBlockWriter writes a SeriesBlock.
type SeriesBlockWriter struct {
	terms  map[string]int // term frequency
	series []serie        // series list

	// Term list is available after writer has been written.
	termList *TermList

	// Series sketch and tombstoned series sketch. These must be
	// set before calling WriteTo.
	Sketch, TSketch estimator.Sketch
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
	for _, s := range sw.series {
		if s.deleted {
			t.TombstoneN++
		} else {
			t.SeriesN++
		}
	}

	// The sketches must be set before calling WriteTo.
	if sw.Sketch == nil {
		return 0, errors.New("series sketch not set")
	} else if sw.TSketch == nil {
		return 0, errors.New("series tombstone sketch not set")
	}

	terms := NewTermList(sw.terms)

	// Write term dictionary.
	t.Term.Data.Offset = n
	nn, err := sw.writeTermListTo(w, terms)
	n += nn
	if err != nil {
		return n, err
	}
	t.Term.Data.Size = n - t.Term.Data.Offset

	// Write dictionary-encoded series list.
	t.Series.Data.Offset = n
	nn, err = sw.writeSeriesTo(w, terms, uint32(n))
	n += nn
	if err != nil {
		return n, err
	}
	t.Series.Data.Size = n - t.Series.Data.Offset

	// Write term index.
	t.Term.Index.Offset = n
	if err := sw.writeTermIndexTo(w, terms, &n); err != nil {
		return n, err
	}
	t.Term.Index.Size = n - t.Term.Index.Offset

	// Write dictionary-encoded series hash index.
	t.Series.Index.Offset = n
	if err := sw.writeSeriesIndexTo(w, terms, &n); err != nil {
		return n, err
	}
	t.Series.Index.Size = n - t.Series.Index.Offset

	// Write the sketches out.
	t.Sketch.Offset = n
	if err := writeSketchTo(w, sw.Sketch, &n); err != nil {
		return n, err
	}
	t.Sketch.Size = n - t.Sketch.Offset

	t.TSketch.Offset = n
	if err := writeSketchTo(w, sw.TSketch, &n); err != nil {
		return n, err
	}
	t.TSketch.Size = n - t.TSketch.Offset

	// Write trailer.
	nn, err = t.WriteTo(w)
	n += nn
	if err != nil {
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
		buf[0] = s.flag()
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

// writeTermIndexTo writes hash map lookup of terms to w.
func (sw *SeriesBlockWriter) writeTermIndexTo(w io.Writer, terms *TermList, n *int64) error {
	// Build hash map of series.
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   terms.Len(),
		LoadFactor: 90,
	})
	for _, e := range terms.a {
		m.Put([]byte(e.term), e.offset)
	}

	// Encode hash map length.
	if err := writeUint32To(w, uint32(m.Cap()), n); err != nil {
		return err
	}

	// Encode hash map offset entries.
	for i := 0; i < m.Cap(); i++ {
		_, v := m.Elem(i)
		offset, _ := v.(uint32)

		if err := writeUint32To(w, offset, n); err != nil {
			return err
		}
	}

	return nil
}

// writeSeriesIndexTo writes hash map lookup of series to w.
func (sw *SeriesBlockWriter) writeSeriesIndexTo(w io.Writer, terms *TermList, n *int64) error {
	// Build hash map of series.
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   len(sw.series),
		LoadFactor: 90,
	})
	for _, s := range sw.series {
		m.Put(terms.AppendEncodedSeries(nil, s.name, s.tags), s.offset)
	}

	// Encode hash map length.
	if err := writeUint32To(w, uint32(m.Cap()), n); err != nil {
		return err
	}

	// Encode hash map offset entries.
	for i := 0; i < m.Cap(); i++ {
		_, v := m.Elem(i)
		offset, _ := v.(uint32)

		if err := writeUint32To(w, uint32(offset), n); err != nil {
			return err
		}
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

	// Read term data info.
	t.Term.Data.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.Term.Data.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read term index info.
	t.Term.Index.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.Term.Index.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read series data info.
	t.Series.Data.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.Series.Data.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read series hash index info.
	t.Series.Index.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.Series.Index.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read series sketch info.
	t.Sketch.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.Sketch.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read tombstone series sketch info.
	t.TSketch.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.TSketch.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	return t
}

// SeriesBlockTrailer represents meta data written to the end of the series list.
type SeriesBlockTrailer struct {
	Term struct {
		Data struct {
			Offset int64
			Size   int64
		}
		Index struct {
			Offset int64
			Size   int64
		}
	}

	Series struct {
		Data struct {
			Offset int64
			Size   int64
		}
		Index struct {
			Offset int64
			Size   int64
		}
	}

	// Offset and size of cardinality sketch for measurements.
	Sketch struct {
		Offset int64
		Size   int64
	}

	// Offset and size of cardinality sketch for tombstoned measurements.
	TSketch struct {
		Offset int64
		Size   int64
	}

	SeriesN    int64
	TombstoneN int64
}

func (t SeriesBlockTrailer) WriteTo(w io.Writer) (n int64, err error) {
	if err := writeUint64To(w, uint64(t.Term.Data.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.Term.Data.Size), &n); err != nil {
		return n, err
	}

	if err := writeUint64To(w, uint64(t.Term.Index.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.Term.Index.Size), &n); err != nil {
		return n, err
	}

	if err := writeUint64To(w, uint64(t.Series.Data.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.Series.Data.Size), &n); err != nil {
		return n, err
	}

	if err := writeUint64To(w, uint64(t.Series.Index.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.Series.Index.Size), &n); err != nil {
		return n, err
	}

	// Write measurement sketch info.
	if err := writeUint64To(w, uint64(t.Sketch.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.Sketch.Size), &n); err != nil {
		return n, err
	}

	// Write tombstone measurement sketch info.
	if err := writeUint64To(w, uint64(t.TSketch.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.TSketch.Size), &n); err != nil {
		return n, err
	}

	// Write series and tombstone count.
	if err := writeUint64To(w, uint64(t.SeriesN), &n); err != nil {
		return n, err
	}
	return n, writeUint64To(w, uint64(t.TombstoneN), &n)
}

type serie struct {
	name    []byte
	tags    models.Tags
	deleted bool
	offset  uint32
}

func (s *serie) flag() uint8 {
	var flag byte
	if s.deleted {
		flag |= SeriesTombstoneFlag
	}
	return flag
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
