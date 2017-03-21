package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

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
		8 + 8 + // series data offset/size
		8 + 8 + // series index offset/size
		8 + 8 + // series sketch offset/size
		8 + 8 + // tombstone series sketch offset/size
		8 + 8 + // series count and tombstone count
		0

	// Other field sizes
	SeriesCountSize = 8
	SeriesIDSize    = 8
)

// Series flag constants.
const (
	SeriesTombstoneFlag = 0x01
)

// SeriesBlock represents the section of the index that holds series data.
type SeriesBlock struct {
	data []byte

	// Series data & index/capacity.
	seriesData   []byte
	seriesIndex  []byte
	seriesIndexN uint64

	// Exact series counts for this block.
	seriesN    int64
	tombstoneN int64

	// Series block sketch and tombstone sketch for cardinality estimation.
	// While we have exact counts for the block, these sketches allow us to
	// estimate cardinality across multiple blocks (which might contain
	// duplicate series).
	sketch, tsketch estimator.Sketch
}

// HasSeries returns flags indicating if the series exists and if it is tombstoned.
func (blk *SeriesBlock) HasSeries(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool) {
	buf = AppendSeriesKey(buf[:0], name, tags)
	bufN := uint64(len(buf))

	n := blk.seriesIndexN
	hash := rhh.HashKey(buf)
	pos := int(hash % n)

	// Track current distance
	var d int
	for {
		// Find offset of series.
		offset := binary.BigEndian.Uint64(blk.seriesIndex[pos*SeriesIDSize:])
		if offset == 0 {
			return false, false
		}

		// Evaluate encoded value matches expected.
		key := ReadSeriesKey(blk.data[offset+1 : offset+1+bufN])
		if bytes.Equal(buf, key) {
			return true, (blk.data[offset] & SeriesTombstoneFlag) != 0
		}

		// Check if we've exceeded the probe distance.
		max := rhh.Dist(rhh.HashKey(key), pos, int(n))
		if d > max {
			return false, false
		}

		// Move position forward.
		pos = (pos + 1) % int(n)
		d++

		if uint64(d) > n {
			return false, false
		}
	}
}

// Series returns a series element.
func (blk *SeriesBlock) Series(name []byte, tags models.Tags) SeriesElem {
	buf := AppendSeriesKey(nil, name, tags)
	bufN := uint64(len(buf))

	n := blk.seriesIndexN
	hash := rhh.HashKey(buf)
	pos := int(hash % n)

	// Track current distance
	var d int
	for {
		// Find offset of series.
		offset := binary.BigEndian.Uint64(blk.seriesIndex[pos*SeriesIDSize:])
		if offset == 0 {
			return nil
		}

		// Evaluate encoded value matches expected.
		key := ReadSeriesKey(blk.data[offset+1 : offset+1+bufN])
		if bytes.Equal(buf, key) {
			var e SeriesBlockElem
			e.UnmarshalBinary(blk.data[offset:])
			return &e
		}

		// Check if we've exceeded the probe distance.
		if d > rhh.Dist(rhh.HashKey(key), pos, int(n)) {
			return nil
		}

		// Move position forward.
		pos = (pos + 1) % int(n)
		d++

		if uint64(d) > n {
			return nil
		}
	}
}

// SeriesCount returns the number of series.
func (blk *SeriesBlock) SeriesCount() uint64 {
	return uint64(blk.seriesN + blk.tombstoneN)
}

// SeriesIterator returns an iterator over all the series.
func (blk *SeriesBlock) SeriesIterator() SeriesIterator {
	return &seriesBlockIterator{
		n:      blk.SeriesCount(),
		offset: 1,
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

	// Slice series data.
	blk.seriesData = data[t.Series.Data.Offset:]
	blk.seriesData = blk.seriesData[:t.Series.Data.Size]

	// Slice series hash index.
	blk.seriesIndex = data[t.Series.Index.Offset:]
	blk.seriesIndex = blk.seriesIndex[:t.Series.Index.Size]
	blk.seriesIndexN = binary.BigEndian.Uint64(blk.seriesIndex[:8])
	blk.seriesIndex = blk.seriesIndex[8:]

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
	i, n   uint64
	offset uint64
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
	itr.e.UnmarshalBinary(itr.sblk.data[itr.offset:])

	// Move iterator and offset forward.
	itr.i++
	itr.offset += uint64(itr.e.size)

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
	itr.e.UnmarshalBinary(itr.sblk.data[id:])
	return &itr.e
}

// SeriesBlockElem represents a series element in the series list.
type SeriesBlockElem struct {
	flag byte
	name []byte
	tags models.Tags
	size int
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

// UnmarshalBinary unmarshals data into e.
func (e *SeriesBlockElem) UnmarshalBinary(data []byte) error {
	start := len(data)

	// Parse flag data.
	e.flag, data = data[0], data[1:]

	// Parse total size.
	_, szN := binary.Uvarint(data)
	data = data[szN:]

	// Parse name.
	n, data := binary.BigEndian.Uint16(data[:2]), data[2:]
	e.name, data = data[:n], data[n:]

	// Parse tags.
	e.tags = e.tags[:0]
	tagN, data := binary.BigEndian.Uint16(data[:2]), data[2:]
	for i := uint16(0); i < tagN; i++ {
		var tag models.Tag

		n, data = binary.BigEndian.Uint16(data[:2]), data[2:]
		tag.Key, data = data[:n], data[n:]

		n, data = binary.BigEndian.Uint16(data[:2]), data[2:]
		tag.Value, data = data[:n], data[n:]

		e.tags = append(e.tags, tag)
	}

	// Save length of elem.
	e.size = start - len(data)

	return nil
}

// AppendSeriesElem serializes flag/name/tags to dst and returns the new buffer.
func AppendSeriesElem(dst []byte, flag byte, name []byte, tags models.Tags) []byte {
	dst = append(dst, flag)
	return AppendSeriesKey(dst, name, tags)
}

// AppendSeriesKey serializes name and tags to a byte slice.
// The total length is prepended as a uvarint.
func AppendSeriesKey(dst []byte, name []byte, tags models.Tags) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	origLen := len(dst)

	// Size of name/tags. Does not include total length.
	size := 0 + //
		2 + // size of measurement
		len(name) + // measurement
		2 + // number of tags
		(4 * len(tags)) + // length of each tag key and value
		tags.Size() // tag keys/values

	// Variable encode length.
	n := binary.PutUvarint(buf, uint64(size))

	// If caller doesn't provide a buffer then pre-allocate an exact one.
	if dst == nil {
		dst = make([]byte, 0, size+n)
	}

	// Append total length.
	dst = append(dst, buf[:n]...)

	// Append name.
	binary.BigEndian.PutUint16(buf, uint16(len(name)))
	dst = append(dst, buf[:2]...)
	dst = append(dst, name...)

	// Append tag count.
	binary.BigEndian.PutUint16(buf, uint16(len(tags)))
	dst = append(dst, buf[:2]...)

	// Append tags.
	for _, tag := range tags {
		binary.BigEndian.PutUint16(buf, uint16(len(tag.Key)))
		dst = append(dst, buf[:2]...)
		dst = append(dst, tag.Key...)

		binary.BigEndian.PutUint16(buf, uint16(len(tag.Value)))
		dst = append(dst, buf[:2]...)
		dst = append(dst, tag.Value...)
	}

	// Verify that the total length equals the encoded byte count.
	if len(dst)-origLen != size+n {
		panic(fmt.Sprintf("series key encoding does not match calculated total length: exp=%d, actual=%d, key=%x", size+n, len(dst), dst))
	}

	return dst
}

// ReadSeriesKey returns the series key from the beginning of the buffer.
func ReadSeriesKey(data []byte) []byte {
	sz, n := binary.Uvarint(data)
	return data[:int(sz)+n]
}

func CompareSeriesKeys(a, b []byte) int {
	// Read total size.
	_, i := binary.Uvarint(a)
	a = a[i:]
	_, i = binary.Uvarint(b)
	b = b[i:]

	// Read names.
	var n uint16
	n, a = binary.BigEndian.Uint16(a), a[2:]
	name0, a := a[:n], a[n:]
	n, b = binary.BigEndian.Uint16(b), b[2:]
	name1, b := b[:n], b[n:]

	// Compare names, return if not equal.
	if cmp := bytes.Compare(name0, name1); cmp != 0 {
		return cmp
	}

	// Read tag counts.
	tagN0, a := binary.BigEndian.Uint16(a), a[2:]
	tagN1, b := binary.BigEndian.Uint16(b), b[2:]

	// Compare each tag in order.
	for i := uint16(0); ; i++ {
		// Check for EOF.
		if i == tagN0 && i == tagN1 {
			return 0
		} else if i == tagN0 {
			return -1
		} else if i == tagN1 {
			return 1
		}

		// Read keys.
		var key0, key1 []byte
		n, a = binary.BigEndian.Uint16(a), a[2:]
		key0, a = a[:n], a[n:]
		n, b = binary.BigEndian.Uint16(b), b[2:]
		key1, b = b[:n], b[n:]

		// Compare keys.
		if cmp := bytes.Compare(key0, key1); cmp != 0 {
			return cmp
		}

		// Read values.
		var value0, value1 []byte
		n, a = binary.BigEndian.Uint16(a), a[2:]
		value0, a = a[:n], a[n:]
		n, b = binary.BigEndian.Uint16(b), b[2:]
		value1, b = b[:n], b[n:]

		// Compare values.
		if cmp := bytes.Compare(value0, value1); cmp != 0 {
			return cmp
		}
	}
}

// SeriesBlockEncoder encodes series to a SeriesBlock in an underlying writer.
type SeriesBlockEncoder struct {
	w io.Writer

	// Double buffer for writing series.
	// First elem is current buffer, second is previous buffer.
	buf [2][]byte

	// Track bytes written, sections, & offsets.
	n       int64
	trailer SeriesBlockTrailer
	offsets *rhh.HashMap

	// Series sketch and tombstoned series sketch. These must be
	// set before calling WriteTo.
	sketch, tSketch estimator.Sketch
}

// NewSeriesBlockEncoder returns a new instance of SeriesBlockEncoder.
func NewSeriesBlockEncoder(w io.Writer) *SeriesBlockEncoder {
	return &SeriesBlockEncoder{
		w: w,

		offsets: rhh.NewHashMap(rhh.Options{LoadFactor: LoadFactor}),

		sketch:  hll.NewDefaultPlus(),
		tSketch: hll.NewDefaultPlus(),
	}
}

// N returns the number of bytes written.
func (enc *SeriesBlockEncoder) N() int64 { return enc.n }

// Offset returns the series offset from the encoder.
// Returns zero if series cannot be found.
func (enc *SeriesBlockEncoder) Offset(key []byte) uint64 {
	v, _ := enc.offsets.Get(key).(uint64)
	return v
}

// Encode writes a series to the underlying writer.
// The series must be lexicographical sorted after the previous encoded series.
func (enc *SeriesBlockEncoder) Encode(name []byte, tags models.Tags, deleted bool) error {
	// An initial empty byte must be written.
	if err := enc.ensureHeaderWritten(); err != nil {
		return err
	}

	// Generate the series element.
	buf := AppendSeriesElem(enc.buf[0][:0], encodeSerieFlag(deleted), name, tags)

	// Verify series is after previous series.
	if enc.buf[1] != nil {
		// Skip the first byte since it is the flag. Remaining bytes are key.
		key0, key1 := buf[1:], enc.buf[1][1:]

		if cmp := CompareSeriesKeys(key0, key1); cmp == -1 {
			return fmt.Errorf("series out of order: prev=%s, new=%s", enc.buf[1], buf)
		} else if cmp == 0 {
			return fmt.Errorf("series already encoded: %s", buf)
		}
	}

	// Swap double buffer.
	enc.buf[0], enc.buf[1] = enc.buf[1], buf

	// Write encoded series to writer.
	offset := enc.n
	if err := writeTo(enc.w, buf, &enc.n); err != nil {
		return err
	}

	// Save offset to generate index later.
	enc.offsets.Put(copyBytes(buf[1:]), uint64(offset))

	// Update sketches & trailer.
	if deleted {
		enc.trailer.TombstoneN++
		enc.tSketch.Add(buf)
	} else {
		enc.trailer.SeriesN++
		enc.sketch.Add(buf)
	}

	return nil
}

// Close writes the index and trailer.
// This should be called at the end once all series have been encoded.
func (enc *SeriesBlockEncoder) Close() error {
	if err := enc.ensureHeaderWritten(); err != nil {
		return err
	}

	// Write dictionary-encoded series list.
	enc.trailer.Series.Data.Offset = 1
	enc.trailer.Series.Data.Size = enc.n - enc.trailer.Series.Data.Offset

	// Write dictionary-encoded series hash index.
	enc.trailer.Series.Index.Offset = enc.n
	if err := enc.writeSeriesIndex(); err != nil {
		return err
	}
	enc.trailer.Series.Index.Size = enc.n - enc.trailer.Series.Index.Offset

	// Write the sketches out.
	enc.trailer.Sketch.Offset = enc.n
	if err := writeSketchTo(enc.w, enc.sketch, &enc.n); err != nil {
		return err
	}
	enc.trailer.Sketch.Size = enc.n - enc.trailer.Sketch.Offset

	enc.trailer.TSketch.Offset = enc.n
	if err := writeSketchTo(enc.w, enc.tSketch, &enc.n); err != nil {
		return err
	}
	enc.trailer.TSketch.Size = enc.n - enc.trailer.TSketch.Offset

	// Write trailer.
	nn, err := enc.trailer.WriteTo(enc.w)
	enc.n += nn
	if err != nil {
		return err
	}

	return nil
}

// writeSeriesIndex writes hash map lookup of series to w.
func (enc *SeriesBlockEncoder) writeSeriesIndex() error {
	// Encode hash map length.
	if err := writeUint64To(enc.w, uint64(enc.offsets.Cap()), &enc.n); err != nil {
		return err
	}

	// Encode hash map offset entries.
	for i := 0; i < enc.offsets.Cap(); i++ {
		_, v := enc.offsets.Elem(i)
		offset, _ := v.(uint64)

		if err := writeUint64To(enc.w, uint64(offset), &enc.n); err != nil {
			return err
		}
	}
	return nil
}

// ensureHeaderWritten writes a single empty byte at the front of the file
// so that series offsets will always be non-zero.
func (enc *SeriesBlockEncoder) ensureHeaderWritten() error {
	if enc.n > 0 {
		return nil
	}

	if _, err := enc.w.Write([]byte{0}); err != nil {
		return err
	}
	enc.n++

	return nil
}

// ReadSeriesBlockTrailer returns the series list trailer from data.
func ReadSeriesBlockTrailer(data []byte) SeriesBlockTrailer {
	var t SeriesBlockTrailer

	// Slice trailer data.
	buf := data[len(data)-SeriesBlockTrailerSize:]

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

	// Read series & tombstone count.
	t.SeriesN, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.TombstoneN, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	return t
}

// SeriesBlockTrailer represents meta data written to the end of the series list.
type SeriesBlockTrailer struct {
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
	} else if err := writeUint64To(w, uint64(t.TombstoneN), &n); err != nil {
		return n, err
	}

	return n, nil
}

type serie struct {
	name    []byte
	tags    models.Tags
	deleted bool
	offset  uint64
}

func (s *serie) flag() uint8 { return encodeSerieFlag(s.deleted) }

func encodeSerieFlag(deleted bool) byte {
	var flag byte
	if deleted {
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
