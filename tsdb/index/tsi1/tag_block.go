package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/influxdata/influxdb/pkg/rhh"
)

// TagBlockVersion is the version of the tag block.
const TagBlockVersion = 1

// Tag key flag constants.
const (
	TagKeyTombstoneFlag = 0x01
)

// Tag value flag constants.
const (
	TagValueTombstoneFlag = 0x01
)

// TagBlock variable size constants.
const (
	// TagBlock key block fields.
	TagKeyNSize      = 8
	TagKeyOffsetSize = 8

	// TagBlock value block fields.
	TagValueNSize      = 8
	TagValueOffsetSize = 8
)

// TagBlock errors.
var (
	ErrUnsupportedTagBlockVersion = errors.New("unsupported tag block version")
	ErrTagBlockSizeMismatch       = errors.New("tag block size mismatch")
)

// TagBlock represents tag key/value block for a single measurement.
type TagBlock struct {
	data []byte

	valueData []byte
	keyData   []byte
	hashData  []byte

	version int // tag block version
}

// Version returns the encoding version parsed from the data.
// Only valid after UnmarshalBinary() has been successfully invoked.
func (blk *TagBlock) Version() int { return blk.version }

// UnmarshalBinary unpacks data into the tag block. Tag block is not copied so data
// should be retained and unchanged after being passed into this function.
func (blk *TagBlock) UnmarshalBinary(data []byte) error {
	// Read trailer.
	t, err := ReadTagBlockTrailer(data)
	if err != nil {
		return err
	}

	// Verify data size is correct.
	if int64(len(data)) != t.Size {
		return ErrTagBlockSizeMismatch
	}

	// Save data section.
	blk.valueData = data[t.ValueData.Offset:]
	blk.valueData = blk.valueData[:t.ValueData.Size]

	// Save key data section.
	blk.keyData = data[t.KeyData.Offset:]
	blk.keyData = blk.keyData[:t.KeyData.Size]

	// Save hash index block.
	blk.hashData = data[t.HashIndex.Offset:]
	blk.hashData = blk.hashData[:t.HashIndex.Size]

	// Save entire block.
	blk.data = data

	return nil
}

// TagKeyElem returns an element for a tag key.
// Returns an element with a nil key if not found.
func (blk *TagBlock) TagKeyElem(key []byte) TagKeyElem {
	keyN := binary.BigEndian.Uint64(blk.hashData[:TagKeyNSize])
	hash := hashKey(key)
	pos := int(hash % keyN)

	// Track current distance
	var d int
	for {
		// Find offset of tag key.
		offset := binary.BigEndian.Uint64(blk.hashData[TagKeyNSize+(pos*TagKeyOffsetSize):])
		if offset == 0 {
			return nil
		}

		// Evaluate key if offset is not empty.
		if offset > 0 {
			// Parse into element.
			var e TagBlockKeyElem
			e.unmarshal(blk.data[offset:], blk.data)

			// Return if keys match.
			if bytes.Equal(e.key, key) {
				return &e
			}

			// Check if we've exceeded the probe distance.
			if d > dist(hashKey(e.key), pos, int(keyN)) {
				return nil
			}

		}

		// Move position forward.
		pos = (pos + 1) % int(keyN)
		d++

		if uint64(d) > keyN {
			return nil
		}
	}
}

// TagValueElem returns an element for a tag value.
func (blk *TagBlock) TagValueElem(key, value []byte) TagValueElem {
	// Find key element, exit if not found.
	kelem, _ := blk.TagKeyElem(key).(*TagBlockKeyElem)
	if kelem == nil {
		return nil
	}

	// Slice hash index data.
	hashData := kelem.hashIndex.buf

	valueN := binary.BigEndian.Uint64(hashData[:TagValueNSize])
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
			var e TagBlockValueElem
			e.unmarshal(blk.data[offset:])

			// Return if values match.
			if bytes.Equal(e.value, value) {
				return &e
			}

			// Check if we've exceeded the probe distance.
			if d > dist(hashKey(e.value), pos, int(valueN)) {
				return nil
			}
		}

		// Move position forward.
		pos = (pos + 1) % int(valueN)
		d++

		if uint64(d) > valueN {
			return nil
		}
	}
}

// TagKeyIterator returns an iterator over all the keys in the block.
func (blk *TagBlock) TagKeyIterator() TagKeyIterator {
	return &tagBlockKeyIterator{
		blk:     blk,
		keyData: blk.keyData,
	}
}

// tagBlockKeyIterator represents an iterator over all keys in a TagBlock.
type tagBlockKeyIterator struct {
	blk     *TagBlock
	keyData []byte
	e       TagBlockKeyElem
}

// Next returns the next element in the iterator.
func (itr *tagBlockKeyIterator) Next() TagKeyElem {
	// Exit when there is no data left.
	if len(itr.keyData) == 0 {
		return nil
	}

	// Unmarshal next element & move data forward.
	itr.e.unmarshal(itr.keyData, itr.blk.data)
	itr.keyData = itr.keyData[itr.e.size:]

	assert(len(itr.e.Key()) > 0, "invalid zero-length tag key")
	return &itr.e
}

// tagBlockValueIterator represents an iterator over all values for a tag key.
type tagBlockValueIterator struct {
	data []byte
	e    TagBlockValueElem
}

// Next returns the next element in the iterator.
func (itr *tagBlockValueIterator) Next() TagValueElem {
	// Exit when there is no data left.
	if len(itr.data) == 0 {
		return nil
	}

	// Unmarshal next element & move data forward.
	itr.e.unmarshal(itr.data)
	itr.data = itr.data[itr.e.size:]

	assert(len(itr.e.Value()) > 0, "invalid zero-length tag value")
	return &itr.e
}

// TagBlockKeyElem represents a tag key element in a TagBlock.
type TagBlockKeyElem struct {
	flag byte
	key  []byte

	// Value data
	data struct {
		offset uint64
		size   uint64
		buf    []byte
	}

	// Value hash index data
	hashIndex struct {
		offset uint64
		size   uint64
		buf    []byte
	}

	size int

	// Reusable iterator.
	itr tagBlockValueIterator
}

// Deleted returns true if the key has been tombstoned.
func (e *TagBlockKeyElem) Deleted() bool { return (e.flag & TagKeyTombstoneFlag) != 0 }

// Key returns the key name of the element.
func (e *TagBlockKeyElem) Key() []byte { return e.key }

// TagValueIterator returns an iterator over the key's values.
func (e *TagBlockKeyElem) TagValueIterator() TagValueIterator {
	return &tagBlockValueIterator{data: e.data.buf}
}

// unmarshal unmarshals buf into e.
// The data argument represents the entire block data.
func (e *TagBlockKeyElem) unmarshal(buf, data []byte) {
	start := len(buf)

	// Parse flag data.
	e.flag, buf = buf[0], buf[1:]

	// Parse data offset/size.
	e.data.offset, buf = binary.BigEndian.Uint64(buf), buf[8:]
	e.data.size, buf = binary.BigEndian.Uint64(buf), buf[8:]

	// Slice data.
	e.data.buf = data[e.data.offset:]
	e.data.buf = e.data.buf[:e.data.size]

	// Parse hash index offset/size.
	e.hashIndex.offset, buf = binary.BigEndian.Uint64(buf), buf[8:]
	e.hashIndex.size, buf = binary.BigEndian.Uint64(buf), buf[8:]

	// Slice hash index data.
	e.hashIndex.buf = data[e.hashIndex.offset:]
	e.hashIndex.buf = e.hashIndex.buf[:e.hashIndex.size]

	// Parse key.
	n, sz := binary.Uvarint(buf)
	e.key, buf = buf[sz:sz+int(n)], buf[int(n)+sz:]

	// Save length of elem.
	e.size = start - len(buf)
}

// TagBlockValueElem represents a tag value element.
type TagBlockValueElem struct {
	flag   byte
	value  []byte
	series struct {
		n    uint64 // Series count
		data []byte // Raw series data
	}

	size int
}

// Deleted returns true if the element has been tombstoned.
func (e *TagBlockValueElem) Deleted() bool { return (e.flag & TagValueTombstoneFlag) != 0 }

// Value returns the value for the element.
func (e *TagBlockValueElem) Value() []byte { return e.value }

// SeriesN returns the series count.
func (e *TagBlockValueElem) SeriesN() uint64 { return e.series.n }

// SeriesID returns series ID at an index.
func (e *TagBlockValueElem) SeriesID(i int) uint64 {
	return binary.BigEndian.Uint64(e.series.data[i*SeriesIDSize:])
}

// SeriesIDs returns a list decoded series ids.
func (e *TagBlockValueElem) SeriesIDs() []uint64 {
	a := make([]uint64, e.series.n)
	for i := 0; i < int(e.series.n); i++ {
		a[i] = e.SeriesID(i)
	}
	return a
}

// unmarshal unmarshals buf into e.
func (e *TagBlockValueElem) unmarshal(buf []byte) {
	start := len(buf)

	// Parse flag data.
	e.flag, buf = buf[0], buf[1:]

	// Parse value.
	sz, n := binary.Uvarint(buf)
	e.value, buf = buf[n:n+int(sz)], buf[n+int(sz):]

	// Parse series count.
	e.series.n, n = binary.Uvarint(buf)
	buf = buf[n:]

	// Save reference to series data.
	e.series.data = buf[:e.series.n*SeriesIDSize]
	buf = buf[e.series.n*SeriesIDSize:]

	// Save length of elem.
	e.size = start - len(buf)
}

// TagBlockTrailerSize is the total size of the on-disk trailer.
const TagBlockTrailerSize = 0 +
	8 + 8 + // value data offset/size
	8 + 8 + // key data offset/size
	8 + 8 + // hash index offset/size
	8 + // size
	2 // version

// TagBlockTrailer represents meta data at the end of a TagBlock.
type TagBlockTrailer struct {
	Version int   // Encoding version
	Size    int64 // Total size w/ trailer

	// Offset & size of value data section.
	ValueData struct {
		Offset int64
		Size   int64
	}

	// Offset & size of key data section.
	KeyData struct {
		Offset int64
		Size   int64
	}

	// Offset & size of hash map section.
	HashIndex struct {
		Offset int64
		Size   int64
	}
}

// WriteTo writes the trailer to w.
func (t *TagBlockTrailer) WriteTo(w io.Writer) (n int64, err error) {
	// Write data info.
	if err := writeUint64To(w, uint64(t.ValueData.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.ValueData.Size), &n); err != nil {
		return n, err
	}

	// Write key data info.
	if err := writeUint64To(w, uint64(t.KeyData.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.KeyData.Size), &n); err != nil {
		return n, err
	}

	// Write hash index info.
	if err := writeUint64To(w, uint64(t.HashIndex.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.HashIndex.Size), &n); err != nil {
		return n, err
	}

	// Write total size & encoding version.
	if err := writeUint64To(w, uint64(t.Size), &n); err != nil {
		return n, err
	} else if err := writeUint16To(w, IndexFileVersion, &n); err != nil {
		return n, err
	}

	return n, nil
}

// ReadTagBlockTrailer returns the tag block trailer from data.
func ReadTagBlockTrailer(data []byte) (TagBlockTrailer, error) {
	var t TagBlockTrailer

	// Read version.
	t.Version = int(binary.BigEndian.Uint16(data[len(data)-2:]))
	if t.Version != TagBlockVersion {
		return t, ErrUnsupportedTagBlockVersion
	}

	// Slice trailer data.
	buf := data[len(data)-TagBlockTrailerSize:]

	// Read data section info.
	t.ValueData.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.ValueData.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read key section info.
	t.KeyData.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.KeyData.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read hash section info.
	t.HashIndex.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.HashIndex.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read total size.
	t.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	return t, nil
}

// TagBlockWriter writes a TagBlock section.
type TagBlockWriter struct {
	sets map[string]tagSet
}

// NewTagBlockWriter returns a new TagBlockWriter.
func NewTagBlockWriter() *TagBlockWriter {
	return &TagBlockWriter{
		sets: make(map[string]tagSet),
	}
}

// DeleteTag marks a key as deleted.
func (tw *TagBlockWriter) DeleteTag(key []byte) {
	assert(len(key) > 0, "cannot delete zero-length tag")

	ts := tw.sets[string(key)]
	ts.deleted = true
	tw.sets[string(key)] = ts
}

// AddTagValue adds a key/value pair with an associated list of series.
func (tw *TagBlockWriter) AddTagValue(key, value []byte, deleted bool, seriesIDs []uint64) {
	assert(len(key) > 0, "cannot add zero-length key")
	assert(len(value) > 0, "cannot add zero-length value")
	assert(len(seriesIDs) > 0, "cannot add tag value without series ids")

	ts, ok := tw.sets[string(key)]
	if !ok || ts.values == nil {
		ts.values = make(map[string]tagValue)
		tw.sets[string(key)] = ts
	}

	tv := ts.values[string(value)]
	tv.deleted = deleted
	tv.seriesIDs = seriesIDs
	ts.values[string(value)] = tv
}

// WriteTo encodes the tag values & tag key blocks.
func (tw *TagBlockWriter) WriteTo(w io.Writer) (n int64, err error) {
	// Initialize trailer.
	var t TagBlockTrailer
	t.Version = TagBlockVersion

	// Write padding byte so no offsets are zero.
	if err := writeUint8To(w, 0, &n); err != nil {
		return n, err
	}

	// Build key hash map.
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   len(tw.sets),
		LoadFactor: 90,
	})
	for key := range tw.sets {
		ts := tw.sets[key]
		m.Put([]byte(key), &ts)
	}

	// Write value blocks in key map order.
	t.ValueData.Offset = n
	for i := 0; i < m.Cap(); i++ {
		_, v := m.Elem(i)
		if v == nil {
			continue
		}
		ts := v.(*tagSet)

		// Write value block.
		if err := tw.writeTagValueBlockTo(w, ts, &n); err != nil {
			return n, err
		}
	}
	t.ValueData.Size = n - t.ValueData.Offset

	// Write key block to point to value blocks.
	if err := tw.writeTagKeyBlockTo(w, m, &t, &n); err != nil {
		return n, err
	}

	// Compute total size w/ trailer.
	t.Size = n + TagBlockTrailerSize

	// Write trailer.
	nn, err := t.WriteTo(w)
	n += nn
	if err != nil {
		return n, err
	}

	return n, nil
}

// writeTagValueBlockTo encodes values from a tag set into w.
func (tw *TagBlockWriter) writeTagValueBlockTo(w io.Writer, ts *tagSet, n *int64) error {
	// Build RHH map from tag values.
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   len(ts.values),
		LoadFactor: 90,
	})
	for value := range ts.values {
		tv := ts.values[value]
		m.Put([]byte(value), &tv)
	}

	// Encode value list.
	ts.data.offset = *n
	for _, k := range m.Keys() {
		tv := m.Get(k).(*tagValue)

		// Save current offset so we can use it in the hash index.
		tv.offset = *n

		// Write value block.
		if err := tw.writeTagValueTo(w, k, tv, n); err != nil {
			return err
		}
	}
	ts.data.size = *n - ts.data.offset

	// Encode hash map length.
	ts.hashIndex.offset = *n
	if err := writeUint64To(w, uint64(m.Cap()), n); err != nil {
		return err
	}

	// Encode hash map offset entries.
	for i := 0; i < m.Cap(); i++ {
		var offset int64

		_, v := m.Elem(i)
		if v != nil {
			offset = v.(*tagValue).offset
		}

		if err := writeUint64To(w, uint64(offset), n); err != nil {
			return err
		}
	}
	ts.hashIndex.size = *n - ts.hashIndex.offset

	return nil
}

// writeTagValueTo encodes a single tag value entry into w.
func (tw *TagBlockWriter) writeTagValueTo(w io.Writer, v []byte, tv *tagValue, n *int64) error {
	assert(len(v) > 0, "cannot write zero-length tag value")

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
		if err := writeUint64To(w, seriesID, n); err != nil {
			return err
		}
	}

	return nil
}

// writeTagKeyBlockTo encodes keys from a tag set into w.
func (tw *TagBlockWriter) writeTagKeyBlockTo(w io.Writer, m *rhh.HashMap, t *TagBlockTrailer, n *int64) error {
	// Encode key list in sorted order.
	t.KeyData.Offset = *n
	for _, k := range m.Keys() {
		ts := m.Get(k).(*tagSet)

		// Save current offset so we can use it in the hash index.
		ts.offset = *n

		// Write key entry.
		if err := tw.writeTagKeyTo(w, k, ts, n); err != nil {
			return err
		}
	}
	t.KeyData.Size = *n - t.KeyData.Offset

	// Encode hash map length.
	t.HashIndex.Offset = *n
	if err := writeUint64To(w, uint64(m.Cap()), n); err != nil {
		return err
	}

	// Encode hash map offset entries.
	for i := 0; i < m.Cap(); i++ {
		var offset int64

		_, v := m.Elem(i)
		if v != nil {
			offset = v.(*tagSet).offset
		}

		if err := writeUint64To(w, uint64(offset), n); err != nil {
			return err
		}
	}
	t.HashIndex.Size = *n - t.HashIndex.Offset

	return nil
}

// writeTagKeyTo encodes a single tag key entry into w.
func (tw *TagBlockWriter) writeTagKeyTo(w io.Writer, k []byte, ts *tagSet, n *int64) error {
	assert(len(k) > 0, "cannot write zero-length tag key")

	if err := writeUint8To(w, ts.flag(), n); err != nil {
		return err
	}

	if err := writeUint64To(w, uint64(ts.data.offset), n); err != nil {
		return err
	} else if err := writeUint64To(w, uint64(ts.data.size), n); err != nil {
		return err
	}

	if err := writeUint64To(w, uint64(ts.hashIndex.offset), n); err != nil {
		return err
	} else if err := writeUint64To(w, uint64(ts.hashIndex.size), n); err != nil {
		return err
	}

	if err := writeUvarintTo(w, uint64(len(k)), n); err != nil {
		return err
	} else if err := writeTo(w, k, n); err != nil {
		return err
	}
	return nil
}

type tagSet struct {
	deleted bool
	data    struct {
		offset int64
		size   int64
	}
	hashIndex struct {
		offset int64
		size   int64
	}
	values map[string]tagValue

	offset int64
}

func (ts tagSet) flag() byte {
	var flag byte
	if ts.deleted {
		flag |= TagKeyTombstoneFlag
	}
	return flag
}

type tagValue struct {
	seriesIDs []uint64
	deleted   bool

	offset int64
}

func (tv tagValue) flag() byte {
	var flag byte
	if tv.deleted {
		flag |= TagValueTombstoneFlag
	}
	return flag
}
