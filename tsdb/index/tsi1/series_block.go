package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/pkg/mmap"
	"github.com/influxdata/influxdb/pkg/rhh"
)

// ErrSeriesOverflow is returned when too many series are added to a series writer.
var ErrSeriesOverflow = errors.New("series overflow")

// Series list field size constants.
const (
	// Series list trailer field sizes.
	SeriesBlockTrailerSize = 0 +
		4 + 4 + // series data offset/size
		4 + 4 + 4 + // series index offset/size/capacity
		8 + 4 + 4 + // bloom filter false positive rate, offset/size
		4 + 4 + // series sketch offset/size
		4 + 4 + // tombstone series sketch offset/size
		4 + 4 + // series count and tombstone count
		0

	// Other field sizes
	SeriesCountSize = 4
	SeriesIDSize    = 4
)

// Series flag constants.
const (
	// Marks the series as having been deleted.
	SeriesTombstoneFlag = 0x01

	// Marks the following bytes as a hash index.
	// These bytes should be skipped by an iterator.
	SeriesHashIndexFlag = 0x02
)

// MaxSeriesBlockHashSize is the maximum number of series in a single hash.
const MaxSeriesBlockHashSize = (65536 * LoadFactor) / 100

// SeriesBlock represents the section of the index that holds series data.
type SeriesBlock struct {
	data []byte

	// Series data & index/capacity.
	seriesData    []byte
	seriesIndexes []seriesBlockIndex

	// Exact series counts for this block.
	seriesN    int32
	tombstoneN int32

	// Bloom filter used for fast series existence check.
	filter *bloom.Filter

	// Series block sketch and tombstone sketch for cardinality estimation.
	// While we have exact counts for the block, these sketches allow us to
	// estimate cardinality across multiple blocks (which might contain
	// duplicate series).
	sketch, tsketch estimator.Sketch
}

// HasSeries returns flags indicating if the series exists and if it is tombstoned.
func (blk *SeriesBlock) HasSeries(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool) {
	offset, tombstoned := blk.Offset(name, tags, buf)
	return offset != 0, tombstoned
}

// Series returns a series element.
func (blk *SeriesBlock) Series(name []byte, tags models.Tags) SeriesElem {
	offset, _ := blk.Offset(name, tags, nil)
	if offset == 0 {
		return nil
	}

	var e SeriesBlockElem
	e.UnmarshalBinary(blk.data[offset:])
	return &e
}

// Offset returns the byte offset of the series within the block.
func (blk *SeriesBlock) Offset(name []byte, tags models.Tags, buf []byte) (offset uint32, tombstoned bool) {
	// Exit if no series indexes exist.
	if len(blk.seriesIndexes) == 0 {
		return 0, false
	}

	// Compute series key.
	buf = AppendSeriesKey(buf[:0], name, tags)
	bufN := uint32(len(buf))

	// Quickly check the bloom filter.
	// If the key doesn't exist then we know for sure that it doesn't exist.
	// If it does exist then we need to do a hash index check to verify. False
	// positives are possible with a bloom filter.
	if !blk.filter.Contains(buf) {
		return 0, false
	}

	// Find the correct partition.
	// Use previous index unless an exact match on the min value.
	i := sort.Search(len(blk.seriesIndexes), func(i int) bool {
		return CompareSeriesKeys(blk.seriesIndexes[i].min, buf) != -1
	})
	if i >= len(blk.seriesIndexes) || !bytes.Equal(blk.seriesIndexes[i].min, buf) {
		i--
	}
	seriesIndex := blk.seriesIndexes[i]

	// Search within partition.
	n := int64(seriesIndex.capacity)
	hash := rhh.HashKey(buf)
	pos := hash % n

	// Track current distance
	var d int64
	for {
		// Find offset of series.
		offset := binary.BigEndian.Uint32(seriesIndex.data[pos*SeriesIDSize:])
		if offset == 0 {
			return 0, false
		}

		// Evaluate encoded value matches expected.
		key := ReadSeriesKey(blk.data[offset+1 : offset+1+bufN])
		if bytes.Equal(buf, key) {
			return offset, (blk.data[offset] & SeriesTombstoneFlag) != 0
		}

		// Check if we've exceeded the probe distance.
		max := rhh.Dist(rhh.HashKey(key), pos, n)
		if d > max {
			return 0, false
		}

		// Move position forward.
		pos = (pos + 1) % n
		d++

		if d > n {
			return 0, false
		}
	}
}

// SeriesCount returns the number of series.
func (blk *SeriesBlock) SeriesCount() uint32 {
	return uint32(blk.seriesN + blk.tombstoneN)
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

	// Read in all index partitions.
	buf := data[t.Series.Index.Offset:]
	buf = buf[:t.Series.Index.Size]
	blk.seriesIndexes = make([]seriesBlockIndex, t.Series.Index.N)
	for i := range blk.seriesIndexes {
		idx := &blk.seriesIndexes[i]

		// Read data block.
		var offset, size uint32
		offset, buf = binary.BigEndian.Uint32(buf[:4]), buf[4:]
		size, buf = binary.BigEndian.Uint32(buf[:4]), buf[4:]
		idx.data = blk.data[offset : offset+size]

		// Read block capacity.
		idx.capacity, buf = int32(binary.BigEndian.Uint32(buf[:4])), buf[4:]

		// Read min key.
		var n uint32
		n, buf = binary.BigEndian.Uint32(buf[:4]), buf[4:]
		idx.min, buf = buf[:n], buf[n:]
	}
	if len(buf) != 0 {
		return fmt.Errorf("data remaining in index list buffer: %d", len(buf))
	}

	// Initialize bloom filter.
	filter, err := bloom.NewFilterBuffer(data[t.Bloom.Offset:][:t.Bloom.Size], t.Bloom.K)
	if err != nil {
		return err
	}
	blk.filter = filter

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

// seriesBlockIndex represents a partitioned series block index.
type seriesBlockIndex struct {
	data     []byte
	min      []byte
	capacity int32
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
	for {
		// Exit if at the end.
		if itr.i == itr.n {
			return nil
		}

		// If the current element is a hash index partition then skip it.
		if flag := itr.sblk.data[itr.offset]; flag&SeriesHashIndexFlag != 0 {
			// Skip flag
			itr.offset++

			// Read index capacity.
			n := binary.BigEndian.Uint32(itr.sblk.data[itr.offset:])
			itr.offset += 4

			// Skip over index.
			itr.offset += n * SeriesIDSize
			continue
		}

		// Read next element.
		itr.e.UnmarshalBinary(itr.sblk.data[itr.offset:])

		// Move iterator and offset forward.
		itr.i++
		itr.offset += uint32(itr.e.size)

		return &itr.e
	}
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
	tagN, szN := binary.Uvarint(data)
	data = data[szN:]

	for i := uint64(0); i < tagN; i++ {
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
	buf := make([]byte, binary.MaxVarintLen32)
	origLen := len(dst)

	// The tag count is variable encoded, so we need to know ahead of time what
	// the size of the tag count value will be.
	tcBuf := make([]byte, binary.MaxVarintLen32)
	tcSz := binary.PutUvarint(tcBuf, uint64(len(tags)))

	// Size of name/tags. Does not include total length.
	size := 0 + //
		2 + // size of measurement
		len(name) + // measurement
		tcSz + // size of number of tags
		(4 * len(tags)) + // length of each tag key and value
		tags.Size() // size of tag keys/values

	// Variable encode length.
	totalSz := binary.PutUvarint(buf, uint64(size))

	// If caller doesn't provide a buffer then pre-allocate an exact one.
	if dst == nil {
		dst = make([]byte, 0, size+totalSz)
	}

	// Append total length.
	dst = append(dst, buf[:totalSz]...)

	// Append name.
	binary.BigEndian.PutUint16(buf, uint16(len(name)))
	dst = append(dst, buf[:2]...)
	dst = append(dst, name...)

	// Append tag count.
	dst = append(dst, tcBuf[:tcSz]...)

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
	if got, exp := len(dst)-origLen, size+totalSz; got != exp {
		panic(fmt.Sprintf("series key encoding does not match calculated total length: actual=%d, exp=%d, key=%x", got, exp, dst))
	}

	return dst
}

// ReadSeriesKey returns the series key from the beginning of the buffer.
func ReadSeriesKey(data []byte) []byte {
	sz, n := binary.Uvarint(data)
	return data[:int(sz)+n]
}

func CompareSeriesKeys(a, b []byte) int {
	// Handle 'nil' keys.
	if len(a) == 0 && len(b) == 0 {
		return 0
	} else if len(a) == 0 {
		return -1
	} else if len(b) == 0 {
		return 1
	}

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
	tagN0, i := binary.Uvarint(a)
	a = a[i:]

	tagN1, i := binary.Uvarint(b)
	b = b[i:]

	// Compare each tag in order.
	for i := uint64(0); ; i++ {
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

type seriesKeys [][]byte

func (a seriesKeys) Len() int      { return len(a) }
func (a seriesKeys) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a seriesKeys) Less(i, j int) bool {
	return CompareSeriesKeys(a[i], a[j]) == -1
}

// SeriesBlockEncoder encodes series to a SeriesBlock in an underlying writer.
type SeriesBlockEncoder struct {
	w io.Writer

	// Double buffer for writing series.
	// First elem is current buffer, second is previous buffer.
	buf [2][]byte

	// Track bytes written, sections, & offsets.
	n        int64
	trailer  SeriesBlockTrailer
	offsets  *rhh.HashMap
	indexMin []byte
	indexes  []seriesBlockIndexEncodeInfo

	// Bloom filter to check for series existance.
	filter *bloom.Filter

	// Series sketch and tombstoned series sketch. These must be
	// set before calling WriteTo.
	sketch, tSketch estimator.Sketch
}

// NewSeriesBlockEncoder returns a new instance of SeriesBlockEncoder.
func NewSeriesBlockEncoder(w io.Writer, n uint32, m, k uint64) *SeriesBlockEncoder {
	return &SeriesBlockEncoder{
		w: w,

		offsets: rhh.NewHashMap(rhh.Options{
			Capacity:   MaxSeriesBlockHashSize,
			LoadFactor: LoadFactor,
		}),

		filter: bloom.NewFilter(m, k),

		sketch:  hll.NewDefaultPlus(),
		tSketch: hll.NewDefaultPlus(),
	}
}

// N returns the number of bytes written.
func (enc *SeriesBlockEncoder) N() int64 { return enc.n }

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
			return fmt.Errorf("series out of order: prev=%q, new=%q", enc.buf[1], buf)
		} else if cmp == 0 {
			return fmt.Errorf("series already encoded: %s", buf)
		}
	}

	// Flush a hash index, if necessary.
	if err := enc.checkFlushIndex(buf[1:]); err != nil {
		return err
	}

	// Swap double buffer.
	enc.buf[0], enc.buf[1] = enc.buf[1], buf

	// Write encoded series to writer.
	offset := enc.n
	if err := writeTo(enc.w, buf, &enc.n); err != nil {
		return err
	}

	// Save offset to generate index later.
	// Key is copied by the RHH map.
	enc.offsets.Put(buf[1:], uint32(offset))

	// Update bloom filter.
	enc.filter.Insert(buf[1:])

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

	// Flush outstanding hash index.
	if err := enc.flushIndex(); err != nil {
		return err
	}

	// Write dictionary-encoded series list.
	enc.trailer.Series.Data.Offset = 1
	enc.trailer.Series.Data.Size = int32(enc.n) - enc.trailer.Series.Data.Offset

	// Write dictionary-encoded series hash index.
	enc.trailer.Series.Index.Offset = int32(enc.n)
	if err := enc.writeIndexEntries(); err != nil {
		return err
	}
	enc.trailer.Series.Index.Size = int32(enc.n) - enc.trailer.Series.Index.Offset

	// Flush bloom filter.
	enc.trailer.Bloom.K = enc.filter.K()
	enc.trailer.Bloom.Offset = int32(enc.n)
	if err := writeTo(enc.w, enc.filter.Bytes(), &enc.n); err != nil {
		return err
	}
	enc.trailer.Bloom.Size = int32(enc.n) - enc.trailer.Bloom.Offset

	// Write the sketches out.
	enc.trailer.Sketch.Offset = int32(enc.n)
	if err := writeSketchTo(enc.w, enc.sketch, &enc.n); err != nil {
		return err
	}
	enc.trailer.Sketch.Size = int32(enc.n) - enc.trailer.Sketch.Offset

	enc.trailer.TSketch.Offset = int32(enc.n)
	if err := writeSketchTo(enc.w, enc.tSketch, &enc.n); err != nil {
		return err
	}
	enc.trailer.TSketch.Size = int32(enc.n) - enc.trailer.TSketch.Offset

	// Write trailer.
	nn, err := enc.trailer.WriteTo(enc.w)
	enc.n += nn
	if err != nil {
		return err
	}

	return nil
}

// writeIndexEntries writes a list of series hash index entries.
func (enc *SeriesBlockEncoder) writeIndexEntries() error {
	enc.trailer.Series.Index.N = int32(len(enc.indexes))

	for _, idx := range enc.indexes {
		// Write offset/size.
		if err := writeUint32To(enc.w, uint32(idx.offset), &enc.n); err != nil {
			return err
		} else if err := writeUint32To(enc.w, uint32(idx.size), &enc.n); err != nil {
			return err
		}

		// Write capacity.
		if err := writeUint32To(enc.w, uint32(idx.capacity), &enc.n); err != nil {
			return err
		}

		// Write min key.
		if err := writeUint32To(enc.w, uint32(len(idx.min)), &enc.n); err != nil {
			return err
		} else if err := writeTo(enc.w, idx.min, &enc.n); err != nil {
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

// checkFlushIndex flushes a hash index segment if the index is too large.
// The min argument specifies the lowest series key in the next index, if one is created.
func (enc *SeriesBlockEncoder) checkFlushIndex(min []byte) error {
	// Ignore if there is still room in the index.
	if enc.offsets.Len() < MaxSeriesBlockHashSize {
		return nil
	}

	// Flush index values.
	if err := enc.flushIndex(); err != nil {
		return nil
	}

	// Reset index and save minimum series key.
	enc.offsets.Reset()
	enc.indexMin = make([]byte, len(min))
	copy(enc.indexMin, min)

	return nil
}

// flushIndex flushes the hash index segment.
func (enc *SeriesBlockEncoder) flushIndex() error {
	if enc.offsets.Len() == 0 {
		return nil
	}

	// Write index segment flag.
	if err := writeUint8To(enc.w, SeriesHashIndexFlag, &enc.n); err != nil {
		return err
	}
	// Write index capacity.
	// This is used for skipping over when iterating sequentially.
	if err := writeUint32To(enc.w, uint32(enc.offsets.Cap()), &enc.n); err != nil {
		return err
	}

	// Determine size.
	var sz int64 = enc.offsets.Cap() * 4

	// Save current position to ensure size is correct by the end.
	offset := enc.n

	// Encode hash map offset entries.
	for i := int64(0); i < enc.offsets.Cap(); i++ {
		_, v := enc.offsets.Elem(i)
		seriesOffset, _ := v.(uint32)

		if err := writeUint32To(enc.w, uint32(seriesOffset), &enc.n); err != nil {
			return err
		}
	}

	// Determine total size.
	size := enc.n - offset

	// Verify actual size equals calculated size.
	if size != sz {
		return fmt.Errorf("series hash index size mismatch: %d <> %d", size, sz)
	}

	// Add to index entries.
	enc.indexes = append(enc.indexes, seriesBlockIndexEncodeInfo{
		offset:   uint32(offset),
		size:     uint32(size),
		capacity: uint32(enc.offsets.Cap()),
		min:      enc.indexMin,
	})

	// Clear next min.
	enc.indexMin = nil

	return nil
}

// seriesBlockIndexEncodeInfo stores offset information for seriesBlockIndex structures.
type seriesBlockIndexEncodeInfo struct {
	offset   uint32
	size     uint32
	capacity uint32
	min      []byte
}

// ReadSeriesBlockTrailer returns the series list trailer from data.
func ReadSeriesBlockTrailer(data []byte) SeriesBlockTrailer {
	var t SeriesBlockTrailer

	// Slice trailer data.
	buf := data[len(data)-SeriesBlockTrailerSize:]

	// Read series data info.
	t.Series.Data.Offset, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]
	t.Series.Data.Size, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]

	// Read series hash index info.
	t.Series.Index.Offset, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]
	t.Series.Index.Size, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]
	t.Series.Index.N, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]

	// Read bloom filter info.
	t.Bloom.K, buf = binary.BigEndian.Uint64(buf[0:8]), buf[8:]
	t.Bloom.Offset, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]
	t.Bloom.Size, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]

	// Read series sketch info.
	t.Sketch.Offset, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]
	t.Sketch.Size, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]

	// Read tombstone series sketch info.
	t.TSketch.Offset, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]
	t.TSketch.Size, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]

	// Read series & tombstone count.
	t.SeriesN, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]
	t.TombstoneN, buf = int32(binary.BigEndian.Uint32(buf[0:4])), buf[4:]

	return t
}

// SeriesBlockTrailer represents meta data written to the end of the series list.
type SeriesBlockTrailer struct {
	Series struct {
		Data struct {
			Offset int32
			Size   int32
		}
		Index struct {
			Offset int32
			Size   int32
			N      int32
		}
	}

	// Bloom filter info.
	Bloom struct {
		K      uint64
		Offset int32
		Size   int32
	}

	// Offset and size of cardinality sketch for measurements.
	Sketch struct {
		Offset int32
		Size   int32
	}

	// Offset and size of cardinality sketch for tombstoned measurements.
	TSketch struct {
		Offset int32
		Size   int32
	}

	SeriesN    int32
	TombstoneN int32
}

func (t SeriesBlockTrailer) WriteTo(w io.Writer) (n int64, err error) {
	if err := writeUint32To(w, uint32(t.Series.Data.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint32To(w, uint32(t.Series.Data.Size), &n); err != nil {
		return n, err
	}

	if err := writeUint32To(w, uint32(t.Series.Index.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint32To(w, uint32(t.Series.Index.Size), &n); err != nil {
		return n, err
	} else if err := writeUint32To(w, uint32(t.Series.Index.N), &n); err != nil {
		return n, err
	}

	// Write bloom filter info.
	if err := writeUint64To(w, t.Bloom.K, &n); err != nil {
		return n, err
	} else if err := writeUint32To(w, uint32(t.Bloom.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint32To(w, uint32(t.Bloom.Size), &n); err != nil {
		return n, err
	}

	// Write measurement sketch info.
	if err := writeUint32To(w, uint32(t.Sketch.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint32To(w, uint32(t.Sketch.Size), &n); err != nil {
		return n, err
	}

	// Write tombstone measurement sketch info.
	if err := writeUint32To(w, uint32(t.TSketch.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint32To(w, uint32(t.TSketch.Size), &n); err != nil {
		return n, err
	}

	// Write series and tombstone count.
	if err := writeUint32To(w, uint32(t.SeriesN), &n); err != nil {
		return n, err
	} else if err := writeUint32To(w, uint32(t.TombstoneN), &n); err != nil {
		return n, err
	}

	return n, nil
}

type serie struct {
	name    []byte
	tags    models.Tags
	deleted bool
	offset  uint32
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

// mapIndexFileSeriesBlock maps a writer to a series block.
// Returns the series block and the mmap byte slice (if mmap is used).
// The memory-mapped slice MUST be unmapped by the caller.
func mapIndexFileSeriesBlock(w io.Writer) (*SeriesBlock, []byte, error) {
	switch w := w.(type) {
	case *bytes.Buffer:
		return mapIndexFileSeriesBlockBuffer(w)
	case *os.File:
		return mapIndexFileSeriesBlockFile(w)
	default:
		return nil, nil, fmt.Errorf("invalid tsi1 writer type: %T", w)
	}
}

// mapIndexFileSeriesBlockBuffer maps a buffer to a series block.
func mapIndexFileSeriesBlockBuffer(buf *bytes.Buffer) (*SeriesBlock, []byte, error) {
	data := buf.Bytes()
	data = data[len(FileSignature):] // Skip file signature.

	var sblk SeriesBlock
	if err := sblk.UnmarshalBinary(data); err != nil {
		return nil, nil, err
	}
	return &sblk, nil, nil
}

// mapIndexFileSeriesBlockFile memory-maps a file to a series block.
func mapIndexFileSeriesBlockFile(f *os.File) (*SeriesBlock, []byte, error) {
	// Open a read-only memory map of the existing data.
	data, err := mmap.Map(f.Name())
	if err != nil {
		return nil, nil, err
	}
	sblk_data := data[len(FileSignature):] // Skip file signature.

	// Unmarshal block on top of mmap.
	var sblk SeriesBlock
	if err := sblk.UnmarshalBinary(sblk_data); err != nil {
		mmap.Unmap(data)
		return nil, nil, err
	}

	return &sblk, data, nil
}
