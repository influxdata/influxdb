package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/influxdata/influxdb/pkg/murmur3"
	"github.com/influxdata/influxdb/pkg/rhh"
)

// TagSetVersion is the version of the tag set block.
const TagSetVersion = 1

// Tag key flag constants.
const (
	TagKeyTombstoneFlag = 0x01
)

// Tag value flag constants.
const (
	TagValueTombstoneFlag = 0x01
)

// TagSet variable size constants.
const (
	// TagSet trailer fields
	TagSetVersionSize    = 2
	TagSetSize           = 8
	TagSetHashOffsetSize = 8
	TagSetTrailerSize    = TagSetVersionSize + TagSetSize + TagSetHashOffsetSize

	// TagSet key block fields.
	TagKeyNSize      = 4
	TagKeyOffsetSize = 8

	// TagSet value block fields.
	TagValueNSize      = 4
	TagValueOffsetSize = 8
)

// TagSet errors.
var (
	ErrUnsupportedTagSetVersion = errors.New("unsupported tag set version")
	ErrTagSetSizeMismatch       = errors.New("tag set size mismatch")
)

// TagSet represents tag key/value data for a single measurement.
type TagSet struct {
	data     []byte
	hashData []byte

	hoff    uint64 // hash index offset
	version int    // tag set version
}

// Verison returns the encoding version parsed from the data.
// Only valid after UnmarshalBinary() has been successfully invoked.
func (ts *TagSet) Version() int { return ts.version }

// TagValueSeriesN returns the number of series ids associated with a tag value.
func (ts *TagSet) TagValueSeriesN(key, value []byte) int {
	velem := ts.tagValueElem(key, value)
	if velem.value == nil {
		return 0
	}
	return int(velem.seriesN)
}

// TagValueSeriesIDs returns the series IDs associated with a tag value.
func (ts *TagSet) TagValueSeriesIDs(key, value []byte) []uint32 {
	// Find value element.
	velem := ts.tagValueElem(key, value)
	if velem.value == nil {
		return nil
	}

	// Build slice of series ids.
	a := make([]uint32, velem.seriesN)
	for i := range a {
		a[i] = velem.seriesID(i)
	}
	return a
}

// tagKeyElem returns an element for a tag key.
// Returns an element with a nil key if not found.
func (ts *TagSet) tagKeyElem(key []byte) tagKeyElem {
	keyN := binary.BigEndian.Uint32(ts.hashData[:TagKeyNSize])
	hash := hashKey(key)
	pos := int(hash) % int(keyN)

	// Track current distance
	var d int

	for {
		// Find offset of tag key.
		offset := binary.BigEndian.Uint64(ts.hashData[TagKeyNSize+(pos*TagKeyOffsetSize):])

		// Parse into element.
		var e tagKeyElem
		e.UnmarshalBinary(ts.data[offset:])

		// Return if keys match.
		if bytes.Equal(e.key, key) {
			return e
		}

		// Check if we've exceeded the probe distance.
		if d > dist(hashKey(e.key), pos, int(keyN)) {
			return tagKeyElem{}
		}

		// Move position forward.
		pos = (pos + 1) % int(keyN)
		d++
	}
}

// tagValueElem returns an element for a tag value.
// Returns an element with a nil value if not found.
func (ts *TagSet) tagValueElem(key, value []byte) tagValueElem {
	// Find key element, exit if not found.
	kelem := ts.tagKeyElem(key)
	if kelem.key == nil {
		return tagValueElem{}
	}

	hashData := ts.data[kelem.valueOffset:]
	valueN := binary.BigEndian.Uint32(hashData[:TagValueNSize])
	hash := hashKey(value)
	pos := int(hash) % int(valueN)

	// Track current distance
	var d int

	for {
		// Find offset of tag value.
		offset := binary.BigEndian.Uint64(hashData[TagValueNSize+(pos*TagValueOffsetSize):])

		// Parse into element.
		var e tagValueElem
		e.UnmarshalBinary(ts.data[offset:])

		// Return if values match.
		if bytes.Equal(e.value, value) {
			return e
		}

		// Check if we've exceeded the probe distance.
		if d > dist(hashKey(e.value), pos, int(valueN)) {
			return tagValueElem{}
		}

		// Move position forward.
		pos = (pos + 1) % int(valueN)
		d++
	}
}

// UnmarshalBinary unpacks data into the tag set. Tag set is not copied so data
// should be retained and unchanged after being passed into this function.
func (ts *TagSet) UnmarshalBinary(data []byte) error {
	// Parse version.
	if len(data) < TagSetVersion {
		return io.ErrShortBuffer
	}
	versionOffset := len(data) - TagSetVersionSize
	ts.version = int(binary.BigEndian.Uint16(data[versionOffset:]))

	// Ensure version matches.
	if ts.version != TagSetVersion {
		return ErrUnsupportedTagSetVersion
	}

	// Parse size & validate.
	szOffset := versionOffset - TagSetSize
	sz := binary.BigEndian.Uint64(data[szOffset:])
	if uint64(len(data)) != sz+TagSetTrailerSize {
		return ErrTagSetSizeMismatch
	}

	// Parse hash index offset.
	hoffOffset := szOffset - TagSetHashOffsetSize
	hoff := binary.BigEndian.Uint64(data[hoffOffset:])

	// Save data block & hash block.
	ts.data = data[:hoff]
	ts.hashData = data[hoff:hoffOffset]

	return nil
}

// tagKeyElem represents an intenral tag key element.
type tagKeyElem struct {
	flag        byte
	key         []byte
	valueOffset uint64
}

// UnmarshalBinary unmarshals data into e.
func (e *tagKeyElem) UnmarshalBinary(data []byte) {
	// Parse flag data.
	e.flag, data = data[0], data[1:]

	// Parse value offset.
	e.valueOffset, data = binary.BigEndian.Uint64(data), data[8:]

	// Parse key.
	sz, n := binary.Uvarint(data)
	data = data[n:]
	e.key = data[:sz]
}

// tagValueElem represents an intenral tag value element.
type tagValueElem struct {
	flag       byte
	value      []byte
	seriesN    uint64
	seriesData []byte
}

// seriesID returns series ID at an index.
func (e *tagValueElem) seriesID(i int) uint32 {
	return binary.BigEndian.Uint32(e.seriesData[i*SeriesIDSize:])
}

// UnmarshalBinary unmarshals data into e.
func (e *tagValueElem) UnmarshalBinary(data []byte) {
	// Parse flag data.
	e.flag, data = data[0], data[1:]

	// Parse value.
	sz, n := binary.Uvarint(data)
	e.value, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse series count.
	e.seriesN, n = binary.Uvarint(data)
	data = data[n:]

	// Save reference to series data.
	e.seriesData = data[:e.seriesN*SeriesIDSize]
}

// TagSetWriter writes a TagSet section.
type TagSetWriter struct {
	sets map[string]tagSet

	// Starting offset of the writer.
	Offset int64
}

// NewTagSetWriter returns a new TagSetWriter.
func NewTagSetWriter() *TagSetWriter {
	return &TagSetWriter{
		sets: make(map[string]tagSet),
	}
}

// AddTagValueSeries associates series id with a tag/value pair.
func (tsw *TagSetWriter) AddTagValueSeries(key, value []byte, seriesID uint32) {
	ts := tsw.createTagSetIfNotExists(key)
	tv := ts.values[string(value)]
	tv.seriesIDs = append(tv.seriesIDs, seriesID)
	ts.values[string(value)] = tv
}

// AddSeries associates series id with a map of key/value pairs.
// This is not optimized and is only provided for ease of use.
func (tsw *TagSetWriter) AddSeries(m map[string]string, seriesID uint32) {
	for k, v := range m {
		tsw.AddTagValueSeries([]byte(k), []byte(v), seriesID)
	}
}

// DeleteTag marks a tag key as tombstoned.
// The tag must not be used after deleting it.
func (tsw *TagSetWriter) DeleteTag(key []byte) {
	ts := tsw.sets[string(key)]
	ts.deleted = true
	tsw.sets[string(key)] = ts
}

// DeleteTagValue marks a tag value as tombstoned.
func (tsw *TagSetWriter) DeleteTagValue(key, value []byte) {
	ts := tsw.createTagSetIfNotExists(key)
	tv := ts.values[string(value)]
	tv.deleted = true
	ts.values[string(value)] = tv
}

// createTagSetIfNotExists returns the tag set for a key.
func (tsw *TagSetWriter) createTagSetIfNotExists(key []byte) tagSet {
	ts, ok := tsw.sets[string(key)]
	if ok {
		return ts
	}

	ts = tagSet{values: make(map[string]tagValue)}
	tsw.sets[string(key)] = ts
	return ts
}

// WriteTo encodes the tag values & tag key blocks.
func (tsw *TagSetWriter) WriteTo(w io.Writer) (n int64, err error) {
	// Build key hash map with an exact capacity.
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   len(tsw.sets),
		LoadFactor: 100,
	})
	for key := range tsw.sets {
		ts := tsw.sets[key]
		m.Put([]byte(key), &ts)
	}

	// Write value blocks in key map order.
	for i := 0; i < m.Len(); i++ {
		k, v := m.Elem(i)
		if k == nil {
			panic("rhh nil key")
		}
		ts := v.(*tagSet)

		// Write value block.
		hoff, err := tsw.writeTagValueBlockTo(w, ts.values, &n)
		if err != nil {
			return n, err
		}

		// Save offset of hash index so we can use it in the key block.
		ts.offset = uint64(hoff)
	}

	// Write key block to point to value blocks.
	hoff, err := tsw.writeTagKeyBlockTo(w, m, &n)
	if err != nil {
		return n, err
	}

	// Write trailer.
	err = tsw.writeTrailerTo(w, hoff, &n)
	if err != nil {
		return n, err
	}

	return n, nil
}

// writeTagValueBlockTo encodes values from a tag set into w.
// Returns the offset of the hash index (hoff).
func (tsw *TagSetWriter) writeTagValueBlockTo(w io.Writer, values map[string]tagValue, n *int64) (hoff int64, err error) {
	// Build RHH map from tag values.
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   len(values),
		LoadFactor: 100,
	})
	for value, tv := range values {
		m.Put([]byte(value), tv)
	}

	// Encode value list.
	offsets := make([]int64, m.Len())
	for i := 0; i < m.Len(); i++ {
		k, v := m.Elem(i)
		if k == nil {
			panic("rhh nil key")
		}
		tv := v.(tagValue)

		// Save current offset so we can use it in the hash index.
		offsets[i] = *n

		// Write value block.
		if err := tsw.writeTagValueTo(w, k, tv, n); err != nil {
			return hoff, err
		}
	}

	// Save starting offset of hash index.
	hoff = *n

	// Encode hash map length.
	if err := writeUint32To(w, uint32(m.Len()), n); err != nil {
		return hoff, err
	}

	// Encode hash map offset entries.
	for i := range offsets {
		if err := writeUint64To(w, uint64(offsets[i]), n); err != nil {
			return hoff, err
		}
	}

	return hoff, nil
}

// writeTagValueTo encodes a single tag value entry into w.
func (tsw *TagSetWriter) writeTagValueTo(w io.Writer, v []byte, tv tagValue, n *int64) error {
	// Write flag.
	if err := writeUint8To(w, tv.flag(), n); err != nil {
		return err
	}

	// Write value.
	if err := writeUvarintTo(w, uint64(len(v)), n); err != nil {
		return err
	} else if err := writeTo(w, v, n); err != nil {
		return err
	}

	// Write series count.
	if err := writeUvarintTo(w, uint64(len(tv.seriesIDs)), n); err != nil {
		return err
	}

	// Write series ids.
	for _, seriesID := range tv.seriesIDs {
		if err := writeUint32To(w, seriesID, n); err != nil {
			return err
		}
	}

	return nil
}

// writeTagKeyBlockTo encodes keys from a tag set into w.
func (tsw *TagSetWriter) writeTagKeyBlockTo(w io.Writer, m *rhh.HashMap, n *int64) (hoff int64, err error) {
	// Encode key list.
	offsets := make([]int64, m.Len())
	for i := 0; i < m.Len(); i++ {
		k, v := m.Elem(i)
		if k == nil {
			panic("rhh nil key")
		}
		ts := v.(*tagSet)

		// Save current offset so we can use it in the hash index.
		offsets[i] = *n

		// Write key entry.
		if err := tsw.writeTagKeyTo(w, k, ts, n); err != nil {
			return hoff, err
		}
	}

	// Save starting offset of hash index.
	hoff = *n

	// Encode hash map length.
	if err := writeUint32To(w, uint32(m.Len()), n); err != nil {
		return hoff, err
	}

	// Encode hash map offset entries.
	for i := range offsets {
		if err := writeUint64To(w, uint64(offsets[i]), n); err != nil {
			return hoff, err
		}
	}

	return hoff, nil
}

// writeTagKeyTo encodes a single tag key entry into w.
func (tsw *TagSetWriter) writeTagKeyTo(w io.Writer, k []byte, ts *tagSet, n *int64) error {
	if err := writeUint8To(w, ts.flag(), n); err != nil {
		return err
	}
	if err := writeUint64To(w, ts.offset, n); err != nil {
		return err
	}
	if err := writeUvarintTo(w, uint64(len(k)), n); err != nil {
		return err
	}
	if err := writeTo(w, k, n); err != nil {
		return err
	}
	return nil
}

// writeTrailerTo encodes the trailer containing sizes and offsets to w.
func (tsw *TagSetWriter) writeTrailerTo(w io.Writer, hoff int64, n *int64) error {
	// Save current size of the write.
	sz := *n

	// Write hash index offset, total size, and v
	if err := writeUint64To(w, uint64(hoff), n); err != nil {
		return err
	}
	if err := writeUint64To(w, uint64(sz), n); err != nil {
		return err
	}
	if err := writeUint16To(w, TagSetVersion, n); err != nil {
		return err
	}
	return nil
}

type tagSet struct {
	values  map[string]tagValue
	deleted bool
	offset  uint64
}

func (ts tagSet) flag() byte {
	var flag byte
	if ts.deleted {
		flag |= TagKeyTombstoneFlag
	}
	return flag
}

type tagValue struct {
	seriesIDs []uint32
	deleted   bool
}

func (tv tagValue) flag() byte {
	var flag byte
	if tv.deleted {
		flag |= TagValueTombstoneFlag
	}
	return flag
}

// hashKey hashes a key using murmur3.
func hashKey(key []byte) uint32 {
	h := murmur3.Sum32(key)
	if h == 0 {
		h = 1
	}
	return h
}

// dist returns the probe distance for a hash in a slot index.
func dist(hash uint32, i, capacity int) int {
	return (i + capacity - (int(hash) % capacity)) % capacity
}
