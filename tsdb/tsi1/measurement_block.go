package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"unsafe"

	"github.com/influxdata/platform/pkg/rhh"
	"github.com/influxdata/platform/tsdb"
)

// MeasurementBlockVersion is the version of the measurement block.
const MeasurementBlockVersion = 1

// Measurement flag constants.
const (
	MeasurementTombstoneFlag   = 0x01
	MeasurementSeriesIDSetFlag = 0x02
)

// Measurement field size constants.
const (
	// 1 byte offset for the block to ensure non-zero offsets.
	MeasurementFillSize = 1

	// Measurement trailer fields
	MeasurementTrailerSize = 0 +
		2 + // version
		8 + 8 + // data offset/size
		8 + 8 + // hash index offset/size
		// legacy sketch info. we used to have HLL sketches, but they were
		// removed. we keep the offset and length bytes in the trailer so
		// that we don't have to do a migration, but they are unused.
		8 + 8 + 8 + 8

	// Measurement key block fields.
	MeasurementNSize      = 8
	MeasurementOffsetSize = 8

	SeriesIDSize = 8
)

// Measurement errors.
var (
	ErrUnsupportedMeasurementBlockVersion = errors.New("unsupported measurement block version")
	ErrMeasurementBlockSizeMismatch       = errors.New("measurement block size mismatch")
)

// MeasurementBlock represents a collection of all measurements in an index.
type MeasurementBlock struct {
	data     []byte
	hashData []byte

	version int // block version
}

// bytes estimates the memory footprint of this MeasurementBlock, in bytes.
func (blk *MeasurementBlock) bytes() int {
	var b int
	// Do not count contents of blk.data or blk.hashData because they reference into an external []byte
	b += int(unsafe.Sizeof(*blk))
	return b
}

// Version returns the encoding version parsed from the data.
// Only valid after UnmarshalBinary() has been successfully invoked.
func (blk *MeasurementBlock) Version() int { return blk.version }

// Elem returns an element for a measurement.
func (blk *MeasurementBlock) Elem(name []byte) (e MeasurementBlockElem, ok bool) {
	n := int64(binary.BigEndian.Uint64(blk.hashData[:MeasurementNSize]))
	hash := rhh.HashKey(name)
	pos := hash % n

	// Track current distance
	var d int64
	for {
		// Find offset of measurement.
		offset := binary.BigEndian.Uint64(blk.hashData[MeasurementNSize+(pos*MeasurementOffsetSize):])
		if offset == 0 {
			return MeasurementBlockElem{}, false
		}

		// Evaluate name if offset is not empty.
		if offset > 0 {
			// Parse into element.
			var e MeasurementBlockElem
			e.UnmarshalBinary(blk.data[offset:])

			// Return if name match.
			if bytes.Equal(e.name, name) {
				return e, true
			}

			// Check if we've exceeded the probe distance.
			if d > rhh.Dist(rhh.HashKey(e.name), pos, n) {
				return MeasurementBlockElem{}, false
			}
		}

		// Move position forward.
		pos = (pos + 1) % n
		d++

		if d > n {
			return MeasurementBlockElem{}, false
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

// SeriesIDIterator returns an iterator for all series ids in a measurement.
func (blk *MeasurementBlock) SeriesIDIterator(name []byte) tsdb.SeriesIDIterator {
	// Find measurement element.
	e, ok := blk.Elem(name)
	if !ok {
		return &rawSeriesIDIterator{}
	}
	if e.seriesIDSet != nil {
		return tsdb.NewSeriesIDSetIterator(e.seriesIDSet)
	}
	return &rawSeriesIDIterator{n: e.series.n, data: e.series.data}
}

// blockMeasurementIterator iterates over a list measurements in a block.
type blockMeasurementIterator struct {
	elem MeasurementBlockElem
	data []byte
}

// Next returns the next measurement. Returns nil when iterator is complete.
func (itr *blockMeasurementIterator) Next() MeasurementElem {
	// Return nil when we run out of data.
	if len(itr.data) == 0 {
		return nil
	}

	// Unmarshal the element at the current position.
	itr.elem.UnmarshalBinary(itr.data)

	// Move the data forward past the record.
	itr.data = itr.data[itr.elem.size:]

	return &itr.elem
}

// rawSeriesIterator iterates over a list of raw series data.
type rawSeriesIDIterator struct {
	prev uint64
	n    uint64
	data []byte
}

func (itr *rawSeriesIDIterator) Close() error { return nil }

// Next returns the next decoded series.
func (itr *rawSeriesIDIterator) Next() (tsdb.SeriesIDElem, error) {
	if len(itr.data) == 0 {
		return tsdb.SeriesIDElem{}, nil
	}

	delta, n, err := uvarint(itr.data)
	if err != nil {
		return tsdb.SeriesIDElem{}, err
	}
	itr.data = itr.data[n:]

	seriesID := itr.prev + uint64(delta)
	itr.prev = seriesID
	return tsdb.SeriesIDElem{SeriesID: tsdb.NewSeriesID(seriesID)}, nil
}

func (itr *rawSeriesIDIterator) SeriesIDSet() *tsdb.SeriesIDSet {
	ss := tsdb.NewSeriesIDSet()
	for data, prev := itr.data, uint64(0); len(data) > 0; {
		delta, n, err := uvarint(data)
		if err != nil {
			break
		}
		data = data[n:]

		seriesID := prev + uint64(delta)
		prev = seriesID
		ss.AddNoLock(tsdb.NewSeriesID(seriesID))
	}
	return ss
}

// MeasurementBlockTrailer represents meta data at the end of a MeasurementBlock.
type MeasurementBlockTrailer struct {
	Version int // Encoding version

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

// ReadMeasurementBlockTrailer returns the block trailer from data.
func ReadMeasurementBlockTrailer(data []byte) (MeasurementBlockTrailer, error) {
	var t MeasurementBlockTrailer

	// Read version (which is located in the last two bytes of the trailer).
	t.Version = int(binary.BigEndian.Uint16(data[len(data)-2:]))
	if t.Version != MeasurementBlockVersion {
		return t, ErrUnsupportedIndexFileVersion
	}

	// Slice trailer data.
	buf := data[len(data)-MeasurementTrailerSize:]

	// Read data section info.
	t.Data.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.Data.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read measurement block info.
	t.HashIndex.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.HashIndex.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// We would advance past old sketch info, but that's unused now.
	_ = buf
	// buf = buf[4*8:]

	return t, nil
}

// WriteTo writes the trailer to w.
func (t *MeasurementBlockTrailer) WriteTo(w io.Writer) (n int64, err error) {
	// Write data section info.
	if err := writeUint64To(w, uint64(t.Data.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.Data.Size), &n); err != nil {
		return n, err
	}

	// Write hash index section info.
	if err := writeUint64To(w, uint64(t.HashIndex.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.HashIndex.Size), &n); err != nil {
		return n, err
	}

	// Write legacy sketch info.
	for i := 0; i < 4; i++ {
		if err := writeUint64To(w, 0, &n); err != nil {
			return n, err
		}
	}

	// Write measurement block version.
	if err := writeUint16To(w, MeasurementBlockVersion, &n); err != nil {
		return n, err
	}

	return n, nil
}

// MeasurementBlockElem represents an internal measurement element.
type MeasurementBlockElem struct {
	flag byte   // flag
	name []byte // measurement name

	tagBlock struct {
		offset int64
		size   int64
	}

	series struct {
		n    uint64 // series count
		data []byte // serialized series data
	}

	seriesIDSet *tsdb.SeriesIDSet

	// size in bytes, set after unmarshaling.
	size int
}

// Name returns the measurement name.
func (e *MeasurementBlockElem) Name() []byte { return e.name }

// Deleted returns true if the tombstone flag is set.
func (e *MeasurementBlockElem) Deleted() bool {
	return (e.flag & MeasurementTombstoneFlag) != 0
}

// TagBlockOffset returns the offset of the measurement's tag block.
func (e *MeasurementBlockElem) TagBlockOffset() int64 { return e.tagBlock.offset }

// TagBlockSize returns the size of the measurement's tag block.
func (e *MeasurementBlockElem) TagBlockSize() int64 { return e.tagBlock.size }

// SeriesData returns the raw series data.
func (e *MeasurementBlockElem) SeriesData() []byte { return e.series.data }

// SeriesN returns the number of series associated with the measurement.
func (e *MeasurementBlockElem) SeriesN() uint64 { return e.series.n }

// SeriesID returns series ID at an index.
func (e *MeasurementBlockElem) SeriesID(i int) uint64 {
	return binary.BigEndian.Uint64(e.series.data[i*SeriesIDSize:])
}

func (e *MeasurementBlockElem) HasSeries() bool { return e.series.n > 0 }

// SeriesIDs returns a list of decoded series ids.
//
// NOTE: This should be used for testing and diagnostics purposes only.
// It requires loading the entire list of series in-memory.
func (e *MeasurementBlockElem) SeriesIDs() []tsdb.SeriesID {
	a := make([]tsdb.SeriesID, 0, e.series.n)
	e.ForEachSeriesID(func(id tsdb.SeriesID) error {
		a = append(a, id)
		return nil
	})
	return a
}

func (e *MeasurementBlockElem) ForEachSeriesID(fn func(tsdb.SeriesID) error) error {
	// Read from roaring, if available.
	if e.seriesIDSet != nil {
		itr := e.seriesIDSet.Iterator()
		for itr.HasNext() {
			if err := fn(tsdb.NewSeriesID(uint64(itr.Next()))); err != nil {
				return err
			}
		}
	}

	// Read from uvarint encoded data, if available.
	var prev uint64
	for data := e.series.data; len(data) > 0; {
		delta, n, err := uvarint(data)
		if err != nil {
			return err
		}
		data = data[n:]

		seriesID := prev + uint64(delta)
		if err = fn(tsdb.NewSeriesID(seriesID)); err != nil {
			return err
		}
		prev = seriesID
	}
	return nil
}

// Size returns the size of the element.
func (e *MeasurementBlockElem) Size() int { return e.size }

// UnmarshalBinary unmarshals data into e.
func (e *MeasurementBlockElem) UnmarshalBinary(data []byte) error {
	start := len(data)

	// Parse flag data.
	e.flag, data = data[0], data[1:]

	// Parse tag block offset.
	e.tagBlock.offset, data = int64(binary.BigEndian.Uint64(data)), data[8:]
	e.tagBlock.size, data = int64(binary.BigEndian.Uint64(data)), data[8:]

	// Parse name.
	sz, n, err := uvarint(data)
	if err != nil {
		return err
	}
	e.name, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse series count.
	v, n, err := uvarint(data)
	if err != nil {
		return err
	}
	e.series.n, data = uint64(v), data[n:]

	// Parse series data size.
	sz, n, err = uvarint(data)
	if err != nil {
		return err
	}
	data = data[n:]

	// Parse series data (original uvarint encoded or roaring bitmap).
	if e.flag&MeasurementSeriesIDSetFlag == 0 {
		e.series.data, data = data[:sz], data[sz:]
	} else {
		// data = memalign(data)
		e.seriesIDSet = tsdb.NewSeriesIDSet()
		if err = e.seriesIDSet.UnmarshalBinaryUnsafe(data[:sz]); err != nil {
			return err
		}
		data = data[sz:]
	}

	// Save length of elem.
	e.size = start - len(data)

	return nil
}

// MeasurementBlockWriter writes a measurement block.
type MeasurementBlockWriter struct {
	buf bytes.Buffer
	mms map[string]measurement
}

// NewMeasurementBlockWriter returns a new MeasurementBlockWriter.
func NewMeasurementBlockWriter() *MeasurementBlockWriter {
	return &MeasurementBlockWriter{
		mms: make(map[string]measurement),
	}
}

// Add adds a measurement with series and tag set offset/size.
func (mw *MeasurementBlockWriter) Add(name []byte, deleted bool, offset, size int64, seriesIDs []tsdb.SeriesID) {
	mm := mw.mms[string(name)]
	mm.deleted = deleted
	mm.tagBlock.offset = offset
	mm.tagBlock.size = size

	if mm.seriesIDSet == nil {
		mm.seriesIDSet = tsdb.NewSeriesIDSet()
	}
	for _, seriesID := range seriesIDs {
		mm.seriesIDSet.AddNoLock(seriesID)
	}

	mw.mms[string(name)] = mm
}

// WriteTo encodes the measurements to w.
func (mw *MeasurementBlockWriter) WriteTo(w io.Writer) (n int64, err error) {
	var t MeasurementBlockTrailer

	// Sort names.
	names := make([]string, 0, len(mw.mms))
	for name := range mw.mms {
		names = append(names, name)
	}
	sort.Strings(names)

	// Begin data section.
	t.Data.Offset = n

	// Write padding byte so no offsets are zero.
	if err := writeUint8To(w, 0, &n); err != nil {
		return n, err
	}

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
	t.Data.Size = n - t.Data.Offset

	// Build key hash map
	m := rhh.NewHashMap(rhh.Options{
		Capacity:   int64(len(names)),
		LoadFactor: LoadFactor,
	})
	for name := range mw.mms {
		mm := mw.mms[name]
		m.Put([]byte(name), &mm)
	}

	t.HashIndex.Offset = n

	// Encode hash map length.
	if err := writeUint64To(w, uint64(m.Cap()), &n); err != nil {
		return n, err
	}

	// Encode hash map offset entries.
	for i := int64(0); i < m.Cap(); i++ {
		_, v := m.Elem(i)

		var offset int64
		if mm, ok := v.(*measurement); ok {
			offset = mm.offset
		}

		if err := writeUint64To(w, uint64(offset), &n); err != nil {
			return n, err
		}
	}
	t.HashIndex.Size = n - t.HashIndex.Offset

	// Write trailer.
	nn, err := t.WriteTo(w)
	n += nn
	return n, err
}

// writeMeasurementTo encodes a single measurement entry into w.
func (mw *MeasurementBlockWriter) writeMeasurementTo(w io.Writer, name []byte, mm *measurement, n *int64) error {
	// Write flag & tag block offset.
	if err := writeUint8To(w, mm.flag(), n); err != nil {
		return err
	}
	if err := writeUint64To(w, uint64(mm.tagBlock.offset), n); err != nil {
		return err
	} else if err := writeUint64To(w, uint64(mm.tagBlock.size), n); err != nil {
		return err
	}

	// Write measurement name.
	if err := writeUvarintTo(w, uint64(len(name)), n); err != nil {
		return err
	}
	if err := writeTo(w, name, n); err != nil {
		return err
	}

	// Write series data to buffer.
	mw.buf.Reset()
	if _, err := mm.seriesIDSet.WriteTo(&mw.buf); err != nil {
		return err
	}

	// Write series count.
	if err := writeUvarintTo(w, mm.seriesIDSet.Cardinality(), n); err != nil {
		return err
	}

	// Write data size & buffer.
	if err := writeUvarintTo(w, uint64(mw.buf.Len()), n); err != nil {
		return err
	}

	// Word align bitmap data.
	// if offset := (*n) % 8; offset != 0 {
	// 	if err := writeTo(w, make([]byte, 8-offset), n); err != nil {
	// 		return err
	// 	}
	// }

	nn, err := mw.buf.WriteTo(w)
	*n += nn
	return err
}

type measurement struct {
	deleted  bool
	tagBlock struct {
		offset int64
		size   int64
	}
	seriesIDSet *tsdb.SeriesIDSet
	offset      int64
}

func (mm measurement) flag() byte {
	flag := byte(MeasurementSeriesIDSetFlag)
	if mm.deleted {
		flag |= MeasurementTombstoneFlag
	}
	return flag
}
