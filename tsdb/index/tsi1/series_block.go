package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
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
		8 + 8 + // series data offset/size
		8 + 8 + // series index offset/size
		8 + 8 + // series sketch offset/size
		8 + 8 + // tombstone series sketch offset/size
		8 + 8 + // series count and tombstone count
		0

	// Other field sizes
	SeriesCountSize = 4
	SeriesIDSize    = 4
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
	seriesIndexN uint32

	// Series counts.
	seriesN    int64
	tombstoneN int64

	// Series block sketch and tombstone sketch for cardinality
	// estimation.
	sketch, tsketch estimator.Sketch
}

// HasSeries returns flags indicating if the series exists and if it is tombstoned.
func (blk *SeriesBlock) HasSeries(name []byte, tags models.Tags) (exists, tombstoned bool) {
	buf := AppendSeriesKey(make([]byte, 0, 256), name, tags)
	bufN := uint32(len(buf))

	n := blk.seriesIndexN
	hash := hashKey(buf)
	pos := int(hash % n)

	// Track current distance
	var d int
	for {
		// Find offset of series.
		offset := binary.BigEndian.Uint32(blk.seriesIndex[pos*SeriesIDSize:])
		if offset == 0 {
			return false, false
		}

		// Evaluate encoded value matches expected.
		key := blk.data[offset+1 : offset+1+bufN]
		if bytes.Equal(buf, key) {
			return true, (blk.data[offset] & SeriesTombstoneFlag) != 0
		}

		// Check if we've exceeded the probe distance.
		max := dist(hashKey(key), pos, int(n))
		if d > max {
			return false, false
		}

		// Move position forward.
		pos = (pos + 1) % int(n)
		d++

		if uint32(d) > n {
			return false, false
		}
	}
}

// Series returns a series element.
func (blk *SeriesBlock) Series(name []byte, tags models.Tags) SeriesElem {
	buf := AppendSeriesKey(nil, name, tags)
	bufN := uint32(len(buf))

	n := blk.seriesIndexN
	hash := hashKey(buf)
	pos := int(hash % n)

	// Track current distance
	var d int
	for {
		// Find offset of series.
		offset := binary.BigEndian.Uint32(blk.seriesIndex[pos*SeriesIDSize:])
		if offset == 0 {
			return nil
		}

		// Evaluate encoded value matches expected.
		key := blk.data[offset+1 : offset+1+bufN]
		if bytes.Equal(buf, key) {
			var e SeriesBlockElem
			e.UnmarshalBinary(blk.data[offset:])
			return &e
		}

		// Check if we've exceeded the probe distance.
		if d > dist(hashKey(key), pos, int(n)) {
			return nil
		}

		// Move position forward.
		pos = (pos + 1) % int(n)
		d++

		if uint32(d) > n {
			return nil
		}
	}
}

// SeriesCount returns the number of series.
func (blk *SeriesBlock) SeriesCount() uint32 {
	return binary.BigEndian.Uint32(blk.seriesData[:SeriesCountSize])
}

// SeriesIterator returns an iterator over all the series.
func (blk *SeriesBlock) SeriesIterator() SeriesIterator {
	return &seriesBlockIterator{
		n:      blk.SeriesCount(),
		offset: uint32(SeriesCountSize),
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
	itr.e.UnmarshalBinary(itr.sblk.data[itr.offset:])

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
func AppendSeriesKey(dst []byte, name []byte, tags models.Tags) []byte {
	buf := make([]byte, 2)

	// Append name.
	binary.BigEndian.PutUint16(buf, uint16(len(name)))
	dst = append(dst, buf...)
	dst = append(dst, name...)

	// Append tag count.
	binary.BigEndian.PutUint16(buf, uint16(len(tags)))
	dst = append(dst, buf...)

	// Append tags.
	for _, tag := range tags {
		binary.BigEndian.PutUint16(buf, uint16(len(tag.Key)))
		dst = append(dst, buf...)
		dst = append(dst, tag.Key...)

		binary.BigEndian.PutUint16(buf, uint16(len(tag.Value)))
		dst = append(dst, buf...)
		dst = append(dst, tag.Value...)
	}

	return dst
}

// SeriesBlockWriter writes a SeriesBlock.
type SeriesBlockWriter struct {
	series []serie // series list

	// Series sketch and tombstoned series sketch. These must be
	// set before calling WriteTo.
	Sketch, TSketch estimator.Sketch
}

// NewSeriesBlockWriter returns a new instance of SeriesBlockWriter.
func NewSeriesBlockWriter() *SeriesBlockWriter {
	return &SeriesBlockWriter{}
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

	// Write dictionary-encoded series list.
	t.Series.Data.Offset = n
	if err := sw.writeSeriesTo(w, &n); err != nil {
		return n, err
	}
	t.Series.Data.Size = n - t.Series.Data.Offset

	// Write dictionary-encoded series hash index.
	t.Series.Index.Offset = n
	if err := sw.writeSeriesIndexTo(w, &n); err != nil {
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
	nn, err := t.WriteTo(w)
	n += nn
	if err != nil {
		return n, err
	}

	return n, nil
}

// writeSeriesTo writes series to w in sorted order.
func (sw *SeriesBlockWriter) writeSeriesTo(w io.Writer, n *int64) error {
	// Ensure series are sorted.
	sort.Sort(series(sw.series))

	// Write series count.
	if err := writeUint32To(w, uint32(len(sw.series)), n); err != nil {
		return err
	}

	// Write series.
	buf := make([]byte, 40)
	for i := range sw.series {
		s := &sw.series[i]

		// Ensure that we can reference the series using a uint32.
		if *n > math.MaxUint32 {
			return errors.New("series list exceeded max size")
		}

		// Track offset of the series.
		s.offset = uint32(*n)

		// Write series to buffer.
		buf = AppendSeriesElem(buf[:0], s.flag(), s.name, s.tags)

		// Write buffer to writer.
		if err := writeTo(w, buf, n); err != nil {
			return err
		}
	}

	return nil
}

// writeSeriesIndexTo writes hash map lookup of series to w.
func (sw *SeriesBlockWriter) writeSeriesIndexTo(w io.Writer, n *int64) error {
	// Build hash map of series.
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   len(sw.series),
		LoadFactor: 90,
	})
	for _, s := range sw.series {
		m.Put(AppendSeriesKey(nil, s.name, s.tags), s.offset)
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
