package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"

	"github.com/influxdata/influxdb/pkg/rhh"
)

// MeasurementBlockVersion is the version of the measurement block.
const MeasurementBlockVersion = 1

// Measurement flag constants.
const (
	MeasurementTombstoneFlag = 0x01
)

// Measurement field size constants.
const (
	// 1 byte offset for the block to ensure non-zero offsets.
	MeasurementFillSize = 1

	// Measurement trailer fields
	MeasurementBlockVersionSize = 2
	MeasurementBlockSize        = 8
	MeasurementHashOffsetSize   = 8
	MeasurementTrailerSize      = MeasurementBlockVersionSize + MeasurementBlockSize + MeasurementHashOffsetSize

	// Measurement key block fields.
	MeasurementNSize      = 4
	MeasurementOffsetSize = 8
)

// Measurement errors.
var (
	ErrUnsupportedMeasurementBlockVersion = errors.New("unsupported meaurement block version")
	ErrMeasurementBlockSizeMismatch       = errors.New("meaurement block size mismatch")
)

// MeasurementBlock represents a collection of all measurements in an index.
type MeasurementBlock struct {
	data     []byte
	hashData []byte

	version int // block version
}

// Version returns the encoding version parsed from the data.
// Only valid after UnmarshalBinary() has been successfully invoked.
func (blk *MeasurementBlock) Version() int { return blk.version }

// Elem returns an element for a measurement.
func (blk *MeasurementBlock) Elem(name []byte) (e MeasurementBlockElem, ok bool) {
	n := binary.BigEndian.Uint32(blk.hashData[:MeasurementNSize])
	hash := hashKey(name)
	pos := int(hash % n)

	// Track current distance
	var d int

	for {
		// Find offset of measurement.
		offset := binary.BigEndian.Uint64(blk.hashData[MeasurementNSize+(pos*MeasurementOffsetSize):])

		// Evaluate name if offset is not empty.
		if offset > 0 {
			// Parse into element.
			var e MeasurementBlockElem
			e.UnmarshalBinary(blk.data[offset:])

			// Return if name match.
			if bytes.Equal(e.Name, name) {
				return e, true
			}

			// Check if we've exceeded the probe distance.
			if d > dist(hashKey(e.Name), pos, int(n)) {
				return MeasurementBlockElem{}, false
			}
		}

		// Move position forward.
		pos = (pos + 1) % int(n)
		d++

		if uint32(d) > n {
			panic("empty hash data block")
		}
	}
}

// UnmarshalBinary unpacks data into the block. Block is not copied so data
// should be retained and unchanged after being passed into this function.
func (blk *MeasurementBlock) UnmarshalBinary(data []byte) error {
	// Read trailer.
	t, err := ReadMeasurementBlockTrailer(data)
	if err != nil {
		return err
	}

	// Verify data size is correct.
	if int64(len(data)) != t.Size {
		return ErrMeasurementBlockSizeMismatch
	}

	// Save data section.
	blk.data = data[t.Data.Offset:]
	blk.data = blk.data[:t.Data.Size]

	// Save hash index block.
	blk.hashData = data[t.HashIndex.Offset:]
	blk.hashData = blk.hashData[:t.HashIndex.Size]

	return nil
}

// Iterator returns an iterator over all measurements.
func (blk *MeasurementBlock) Iterator() MeasurementIterator {
	return &blockMeasurementIterator{data: blk.data[MeasurementFillSize:]}
}

// blockMeasurementIterator iterates over a list measurements in a block.
type blockMeasurementIterator struct {
	elem MeasurementElem
	data []byte
}

// Next returns the next measurement. Returns nil when iterator is complete.
func (itr *blockMeasurementIterator) Next() *MeasurementElem {
	// Return nil when we run out of data.
	if len(itr.data) == 0 {
		return nil
	}

	// Unmarshal the element at the current position.
	var elem MeasurementBlockElem
	elem.UnmarshalBinary(itr.data)

	// Copy to a generic measurement element.
	itr.elem = MeasurementElem{
		Name:    elem.Name,
		Deleted: elem.Deleted(),
	}

	// Move the data forward past the record.
	itr.data = itr.data[elem.Size:]

	return &itr.elem
}

// ReadMeasurementBlockTrailer returns the trailer from data.
func ReadMeasurementBlockTrailer(data []byte) (MeasurementBlockTrailer, error) {
	var t MeasurementBlockTrailer

	// Read version.
	versionOffset := len(data) - MeasurementBlockVersionSize
	t.Version = int(binary.BigEndian.Uint16(data[versionOffset:]))

	if t.Version != MeasurementBlockVersion {
		return t, ErrUnsupportedMeasurementBlockVersion
	}

	// Parse total size.
	szOffset := versionOffset - MeasurementBlockSize
	sz := int64(binary.BigEndian.Uint64(data[szOffset:]))
	t.Size = int64(sz + MeasurementTrailerSize)

	// Parse hash index offset.
	hoffOffset := szOffset - MeasurementHashOffsetSize
	t.HashIndex.Offset = int64(binary.BigEndian.Uint64(data[hoffOffset:]))
	t.HashIndex.Size = int64(hoffOffset) - t.HashIndex.Offset

	// Compute data size.
	t.Data.Offset = 0
	t.Data.Size = t.HashIndex.Offset

	return t, nil
}

// MeasurementBlockTrailer represents meta data at the end of a MeasurementBlock.
type MeasurementBlockTrailer struct {
	Version int   // Encoding version
	Size    int64 // Total size w/ trailer

	// Offset & size of data section.
	Data struct {
		Offset int64
		Size   int64
	}

	// Offset & size of hash map section.
	HashIndex struct {
		Offset int64
		Size   int64
	}
}

// MeasurementBlockElem represents an internal measurement element.
type MeasurementBlockElem struct {
	Flag byte   // flag
	Name []byte // measurement name

	TagSet struct {
		Offset int64
		Size   int64
	}

	Series struct {
		N    uint32 // series count
		Data []byte // serialized series data
	}

	// Size in bytes, set after unmarshaling.
	Size int
}

// Deleted returns true if the tombstone flag is set.
func (e *MeasurementBlockElem) Deleted() bool {
	return (e.Flag & MeasurementTombstoneFlag) != 0
}

// SeriesID returns series ID at an index.
func (e *MeasurementBlockElem) SeriesID(i int) uint32 {
	return binary.BigEndian.Uint32(e.Series.Data[i*SeriesIDSize:])
}

// SeriesIDs returns a list of decoded series ids.
func (e *MeasurementBlockElem) SeriesIDs() []uint32 {
	a := make([]uint32, e.Series.N)
	for i := 0; i < int(e.Series.N); i++ {
		a[i] = e.SeriesID(i)
	}
	return a
}

// UnmarshalBinary unmarshals data into e.
func (e *MeasurementBlockElem) UnmarshalBinary(data []byte) error {
	start := len(data)

	// Parse flag data.
	e.Flag, data = data[0], data[1:]

	// Parse tagset offset.
	e.TagSet.Offset, data = int64(binary.BigEndian.Uint64(data)), data[8:]
	e.TagSet.Size, data = int64(binary.BigEndian.Uint64(data)), data[8:]

	// Parse name.
	sz, n := binary.Uvarint(data)
	e.Name, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse series data.
	v, n := binary.Uvarint(data)
	e.Series.N, data = uint32(v), data[n:]
	e.Series.Data, data = data[:e.Series.N*SeriesIDSize], data[e.Series.N*SeriesIDSize:]

	// Save length of elem.
	e.Size = start - len(data)

	return nil
}

// MeasurementBlockWriter writes a measurement block.
type MeasurementBlockWriter struct {
	mms map[string]measurement
}

// NewMeasurementBlockWriter returns a new MeasurementBlockWriter.
func NewMeasurementBlockWriter() *MeasurementBlockWriter {
	return &MeasurementBlockWriter{
		mms: make(map[string]measurement),
	}
}

// Add adds a measurement with series and tag set offset/size.
func (mw *MeasurementBlockWriter) Add(name []byte, offset, size int64, seriesIDs []uint32) {
	mm := mw.mms[string(name)]
	mm.tagSet.offset = offset
	mm.tagSet.size = size
	mm.seriesIDs = seriesIDs
	mw.mms[string(name)] = mm
}

// Delete marks a measurement as tombstoned.
func (mw *MeasurementBlockWriter) Delete(name []byte) {
	mm := mw.mms[string(name)]
	mm.deleted = true
	mw.mms[string(name)] = mm
}

// WriteTo encodes the measurements to w.
func (mw *MeasurementBlockWriter) WriteTo(w io.Writer) (n int64, err error) {
	// Write padding byte so no offsets are zero.
	if err := writeUint8To(w, 0, &n); err != nil {
		return n, err
	}

	// Sort names.
	names := make([]string, 0, len(mw.mms))
	for name := range mw.mms {
		names = append(names, name)
	}
	sort.Strings(names)

	// Encode key list.
	for _, name := range names {
		// Retrieve measurement and save offset.
		mm := mw.mms[name]
		mm.offset = n
		mw.mms[name] = mm

		// Write measurement
		if err := mw.writeMeasurementTo(w, []byte(name), &mm, &n); err != nil {
			return n, err
		}
	}

	// Save starting offset of hash index.
	hoff := n

	// Build key hash map
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   len(names),
		LoadFactor: 90,
	})
	for name := range mw.mms {
		mm := mw.mms[name]
		m.Put([]byte(name), &mm)
	}

	// Encode hash map length.
	if err := writeUint32To(w, uint32(m.Cap()), &n); err != nil {
		return n, err
	}

	// Encode hash map offset entries.
	for i := 0; i < m.Cap(); i++ {
		_, v := m.Elem(i)

		var offset int64
		if mm, ok := v.(*measurement); ok {
			offset = mm.offset
		}

		if err := writeUint64To(w, uint64(offset), &n); err != nil {
			return n, err
		}
	}

	// Write trailer.
	if err = mw.writeTrailerTo(w, hoff, &n); err != nil {
		return n, err
	}

	return n, nil
}

// writeMeasurementTo encodes a single measurement entry into w.
func (mw *MeasurementBlockWriter) writeMeasurementTo(w io.Writer, name []byte, mm *measurement, n *int64) error {
	// Write flag & tagset block offset.
	if err := writeUint8To(w, mm.flag(), n); err != nil {
		return err
	}
	if err := writeUint64To(w, uint64(mm.tagSet.offset), n); err != nil {
		return err
	} else if err := writeUint64To(w, uint64(mm.tagSet.size), n); err != nil {
		return err
	}

	// Write measurement name.
	if err := writeUvarintTo(w, uint64(len(name)), n); err != nil {
		return err
	}
	if err := writeTo(w, name, n); err != nil {
		return err
	}

	// Write series count & ids.
	if err := writeUvarintTo(w, uint64(len(mm.seriesIDs)), n); err != nil {
		return err
	}
	for _, seriesID := range mm.seriesIDs {
		if err := writeUint32To(w, seriesID, n); err != nil {
			return err
		}
	}

	return nil
}

// writeTrailerTo encodes the trailer containing sizes and offsets to w.
func (mw *MeasurementBlockWriter) writeTrailerTo(w io.Writer, hoff int64, n *int64) error {
	// Save current size of the write.
	sz := *n

	// Write hash index offset, total size, and v
	if err := writeUint64To(w, uint64(hoff), n); err != nil {
		return err
	}
	if err := writeUint64To(w, uint64(sz), n); err != nil {
		return err
	}
	if err := writeUint16To(w, MeasurementBlockVersion, n); err != nil {
		return err
	}
	return nil
}

type measurement struct {
	deleted bool
	tagSet  struct {
		offset int64
		size   int64
	}
	seriesIDs []uint32
	offset    int64
}

func (mm measurement) flag() byte {
	var flag byte
	if mm.deleted {
		flag |= MeasurementTombstoneFlag
	}
	return flag
}
