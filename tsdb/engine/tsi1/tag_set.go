package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

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

// Version returns the encoding version parsed from the data.
// Only valid after UnmarshalBinary() has been successfully invoked.
func (ts *TagSet) Version() int { return ts.version }

// TagKeyElem returns an element for a tag key.
// Returns an element with a nil key if not found.
func (ts *TagSet) TagKeyElem(key []byte) TagSetKeyElem {
	keyN := binary.BigEndian.Uint32(ts.hashData[:TagKeyNSize])
	hash := hashKey(key)
	pos := int(hash % keyN)

	// Track current distance
	var d int

	for {
		// Find offset of tag key.
		offset := binary.BigEndian.Uint64(ts.hashData[TagKeyNSize+(pos*TagKeyOffsetSize):])

		// Evaluate key if offset is not empty.
		if offset > 0 {
			// Parse into element.
			var e TagSetKeyElem
			e.UnmarshalBinary(ts.data[offset:])

			// Return if keys match.
			if bytes.Equal(e.Key, key) {
				return e
			}

			// Check if we've exceeded the probe distance.
			if d > dist(hashKey(e.Key), pos, int(keyN)) {
				return TagSetKeyElem{}
			}
		}

		// Move position forward.
		pos = (pos + 1) % int(keyN)
		d++
	}
}

// TagValueElem returns an element for a tag value.
// Returns an element with a nil value if not found.
func (ts *TagSet) TagValueElem(key, value []byte) TagSetValueElem {
	// Find key element, exit if not found.
	kelem := ts.TagKeyElem(key)
	if len(kelem.Key) == 0 {
		return TagSetValueElem{}
	}

	hashData := ts.data[kelem.Offset:]
	valueN := binary.BigEndian.Uint32(hashData[:TagValueNSize])
	hash := hashKey(value)
	pos := int(hash % valueN)

	// Track current distance
	var d int

	for {
		// Find offset of tag value.
		offset := binary.BigEndian.Uint64(hashData[TagValueNSize+(pos*TagValueOffsetSize):])

		// Evaluate value if offset is not empty.
		if offset > 0 {
			// Parse into element.
			var e TagSetValueElem
			e.UnmarshalBinary(ts.data[offset:])

			// Return if values match.
			if bytes.Equal(e.Value, value) {
				return e
			}

			// Check if we've exceeded the probe distance.
			if d > dist(hashKey(e.Value), pos, int(valueN)) {
				return TagSetValueElem{}
			}
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

// TagSetKeyElem represents a tag key element in a TagSetBlock.
type TagSetKeyElem struct {
	Flag   byte
	Key    []byte
	Offset uint64 // Value block offset
}

// UnmarshalBinary unmarshals data into e.
func (e *TagSetKeyElem) UnmarshalBinary(data []byte) {
	// Parse flag data.
	e.Flag, data = data[0], data[1:]

	// Parse value offset.
	e.Offset, data = binary.BigEndian.Uint64(data), data[8:]

	// Parse key.
	sz, n := binary.Uvarint(data)
	data = data[n:]
	e.Key = data[:sz]
}

// TagSetValueElem represents a tag value element.
type TagSetValueElem struct {
	Flag   byte
	Value  []byte
	Series struct {
		N    uint64 // Series count
		Data []byte // Raw series data
	}
}

// SeriesID returns series ID at an index.
func (e *TagSetValueElem) SeriesID(i int) uint32 {
	return binary.BigEndian.Uint32(e.Series.Data[i*SeriesIDSize:])
}

// SeriesIDs returns a list decoded series ids.
func (e *TagSetValueElem) SeriesIDs() []uint32 {
	a := make([]uint32, e.Series.N)
	for i := 0; i < int(e.Series.N); i++ {
		a[i] = e.SeriesID(i)
	}
	return a
}

// UnmarshalBinary unmarshals data into e.
func (e *TagSetValueElem) UnmarshalBinary(data []byte) {
	// Parse flag data.
	e.Flag, data = data[0], data[1:]

	// Parse value.
	sz, n := binary.Uvarint(data)
	e.Value, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse series count.
	e.Series.N, n = binary.Uvarint(data)
	data = data[n:]

	// Save reference to series data.
	e.Series.Data = data[:e.Series.N*SeriesIDSize]
}

// TagSetWriter writes a TagSet section.
type TagSetWriter struct {
	sets map[string]tagSet
}

// NewTagSetWriter returns a new TagSetWriter.
func NewTagSetWriter() *TagSetWriter {
	return &TagSetWriter{
		sets: make(map[string]tagSet),
	}
}

// DeleteTag marks a key as deleted.
func (tsw *TagSetWriter) DeleteTag(key []byte) {
	ts := tsw.sets[string(key)]
	ts.deleted = true
	tsw.sets[string(key)] = ts
}

// AddTagValue adds a key/value pair with an associated list of series.
func (tsw *TagSetWriter) AddTagValue(key, value []byte, deleted bool, seriesIDs []uint32) {
	ts, ok := tsw.sets[string(key)]
	if !ok || ts.values == nil {
		ts.values = make(map[string]tagValue)
		tsw.sets[string(key)] = ts
	}

	tv := ts.values[string(value)]
	tv.deleted = deleted
	tv.seriesIDs = seriesIDs
	ts.values[string(value)] = tv
}

/*
// AddSeries associates series id with a map of key/value pairs.
// This is not optimized and is only provided for ease of use.
func (tsw *TagSetWriter) AddSeries(m map[string]string, seriesID uint32) {
	for k, v := range m {
		tsw.AddTagValueSeries([]byte(k), []byte(v), seriesID)
	}
}
*/

// WriteTo encodes the tag values & tag key blocks.
func (tsw *TagSetWriter) WriteTo(w io.Writer) (n int64, err error) {
	// Write padding byte so no offsets are zero.
	if err := writeUint8To(w, 0, &n); err != nil {
		return n, err
	}

	// Build key hash map.
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   len(tsw.sets),
		LoadFactor: 90,
	})
	for key := range tsw.sets {
		ts := tsw.sets[key]
		m.Put([]byte(key), &ts)
	}

	// Write value blocks in key map order.
	for i := 0; i < m.Cap(); i++ {
		_, v := m.Elem(i)
		if v == nil {
			continue
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
		LoadFactor: 90,
	})
	for value, tv := range values {
		m.Put([]byte(value), tv)
	}

	// Encode value list.
	offsets := make([]int64, m.Cap())
	for i := 0; i < m.Cap(); i++ {
		k, v := m.Elem(i)
		tv, _ := v.(tagValue)

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
	if err := writeUint32To(w, uint32(m.Cap()), n); err != nil {
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
	offsets := make([]int64, m.Cap())
	for i := 0; i < m.Cap(); i++ {
		k, v := m.Elem(i)
		if v == nil {
			continue
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
	if err := writeUint32To(w, uint32(m.Cap()), n); err != nil {
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
