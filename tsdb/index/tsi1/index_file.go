package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/mmap"
)

// IndexFileVersion is the current TSI1 index file version.
const IndexFileVersion = 1

// FileSignature represents a magic number at the header of the index file.
const FileSignature = "TSI1"

// IndexFile field size constants.
const (
	// IndexFile trailer fields
	IndexFileVersionSize       = 2
	SeriesBlockOffsetSize      = 8
	SeriesBlockSizeSize        = 8
	MeasurementBlockOffsetSize = 8
	MeasurementBlockSizeSize   = 8

	IndexFileTrailerSize = IndexFileVersionSize +
		SeriesBlockOffsetSize +
		SeriesBlockSizeSize +
		MeasurementBlockOffsetSize +
		MeasurementBlockSizeSize
)

// IndexFile errors.
var (
	ErrInvalidIndexFile            = errors.New("invalid index file")
	ErrUnsupportedIndexFileVersion = errors.New("unsupported index file version")
)

// IndexFile represents a collection of measurement, tag, and series data.
type IndexFile struct {
	data []byte

	// Components
	sblk  SeriesBlock
	tblks map[string]*TagBlock // tag blocks by measurement name
	mblk  MeasurementBlock

	// Path to data file.
	Path string
}

// NewIndexFile returns a new instance of IndexFile.
func NewIndexFile() *IndexFile {
	return &IndexFile{}
}

// Open memory maps the data file at the file's path.
func (f *IndexFile) Open() error {
	data, err := mmap.Map(f.Path)
	if err != nil {
		return err
	}
	return f.UnmarshalBinary(data)
}

// Close unmaps the data file.
func (f *IndexFile) Close() error {
	f.sblk = SeriesBlock{}
	f.tblks = nil
	f.mblk = MeasurementBlock{}
	return mmap.Unmap(f.data)
}

// UnmarshalBinary opens an index from data.
// The byte slice is retained so it must be kept open.
func (f *IndexFile) UnmarshalBinary(data []byte) error {
	// Ensure magic number exists at the beginning.
	if len(data) < len(FileSignature) {
		return io.ErrShortBuffer
	} else if !bytes.Equal(data[:len(FileSignature)], []byte(FileSignature)) {
		return ErrInvalidIndexFile
	}

	// Read index file trailer.
	t, err := ReadIndexFileTrailer(data)
	if err != nil {
		return err
	}

	// Slice measurement block data.
	buf := data[t.MeasurementBlock.Offset:]
	buf = buf[:t.MeasurementBlock.Size]

	// Unmarshal measurement block.
	if err := f.mblk.UnmarshalBinary(buf); err != nil {
		return err
	}

	// Unmarshal each tag block.
	f.tblks = make(map[string]*TagBlock)
	itr := f.mblk.Iterator()
	for m := itr.Next(); m != nil; m = itr.Next() {
		e := m.(*MeasurementBlockElem)

		// Slice measurement block data.
		buf := data[e.tagBlock.offset:]
		buf = buf[:e.tagBlock.size]

		// Unmarshal measurement block.
		var tblk TagBlock
		if err := tblk.UnmarshalBinary(buf); err != nil {
			return err
		}
		f.tblks[string(e.name)] = &tblk
	}

	// Slice series list data.
	buf = data[t.SeriesBlock.Offset:]
	buf = buf[:t.SeriesBlock.Size]

	// Unmarshal series list.
	if err := f.sblk.UnmarshalBinary(buf); err != nil {
		return err
	}

	// Save reference to entire data block.
	f.data = data

	return nil
}

// Measurement returns a measurement element.
func (f *IndexFile) Measurement(name []byte) MeasurementElem {
	e, ok := f.mblk.Elem(name)
	if !ok {
		return nil
	}
	return &e
}

// TagKeySeriesIterator returns a series iterator for a tag key and a flag
// indicating if a tombstone exists on the measurement or key.
func (f *IndexFile) TagKeySeriesIterator(name, key []byte) (itr SeriesIterator, deleted bool) {
	// Find measurement.
	mm, ok := f.mblk.Elem(name)
	if !ok {
		return nil, deleted
	} else if mm.Deleted() {
		deleted = true
	}

	// Find key element.
	ke := f.tblks[string(name)].TagKeyElem(key)
	if ke == nil {
		return nil, deleted
	} else if ke.Deleted() {
		deleted = true
	}

	// Merge all value series iterators together.
	vitr := ke.TagValueIterator()
	var itrs []SeriesIterator
	for ve := vitr.Next(); ve != nil; ve = vitr.Next() {
		sitr := &rawSeriesIDIterator{data: ve.(*TagBlockValueElem).series.data}
		itrs = append(itrs, newSeriesDecodeIterator(&f.sblk, sitr))
	}

	return MergeSeriesIterators(itrs...), deleted
}

// TagValueSeriesIterator returns a series iterator for a tag value and a flag
// indicating if a tombstone exists on the measurement, key, or value.
func (f *IndexFile) TagValueSeriesIterator(name, key, value []byte) (itr SeriesIterator, deleted bool) {
	// Find measurement.
	mm, ok := f.mblk.Elem(name)
	if !ok {
		return nil, deleted
	} else if mm.Deleted() {
		deleted = true
	}

	// Find value element.
	ve, del := f.tblks[string(name)].TagValueElem(key, value)
	if del {
		deleted = true
	}
	if ve.Value() == nil {
		return nil, deleted
	} else if ve.Deleted() {
		deleted = true
	}

	// Create an iterator over value's series.
	sitr := newSeriesDecodeIterator(&f.sblk, &rawSeriesIDIterator{data: ve.(*TagBlockValueElem).series.data})
	return sitr, deleted
}

// TagKey returns a tag key and flag indicating if tombstoned by measurement.
func (f *IndexFile) TagKey(name, key []byte) (e TagKeyElem, deleted bool) {
	// Find measurement.
	mm, ok := f.mblk.Elem(name)
	if !ok {
		return nil, deleted
	} else if mm.Deleted() {
		deleted = true
	}

	// Find key element.
	ke := f.tblks[string(name)].TagKeyElem(key)
	if ke == nil {
		return nil, deleted
	}
	return ke, deleted
}

/*
// TagValue returns a tag key and flag indicating if tombstoned by measurement or key.
func (f *IndexFile) TagValue(name, key, value []byte) (e TagKeyValue, deleted bool) {
	// Find measurement.
	mm, ok := f.mblk.Elem(name)
	if !ok {
		return nil, deleted
	} else if mm.Deleted() {
		deleted = true
	}

	// Find key element.
	ve, del := f.tblks[string(name)].TagValueElem(key, value)
	if del {
		deleted = true
	}
	if ve.value == nil {
		return nil, deleted
	}
	return &ve, deleted
}
*/

// Series returns the series and a flag indicating if the series has been
// tombstoned by the measurement.
func (f *IndexFile) Series(name []byte, tags models.Tags) (e SeriesElem, deleted bool) {
	// Find measurement.
	me, ok := f.mblk.Elem(name)
	if !ok {
		return nil, false
	} else if me.Deleted() {
		deleted = true
	}

	// Return series element in series block.
	return f.sblk.Series(name, tags), deleted
}

// TagValueElem returns an element for a measurement/tag/value.
func (f *IndexFile) TagValueElem(name, key, value []byte) (e TagValueElem, deleted bool) {
	tblk, ok := f.tblks[string(name)]
	if !ok {
		return nil, false
	}
	return tblk.TagValueElem(key, value)
}

// MeasurementIterator returns an iterator over all measurements.
func (f *IndexFile) MeasurementIterator() MeasurementIterator {
	return f.mblk.Iterator()
}

// TagKeyIterator returns an iterator over all tag keys for a measurement.
func (f *IndexFile) TagKeyIterator(name []byte) TagKeyIterator {
	blk := f.tblks[string(name)]
	if blk == nil {
		return nil
	}
	return blk.TagKeyIterator()
}

// MeasurementSeriesIterator returns an iterator over a measurement's series.
func (f *IndexFile) MeasurementSeriesIterator(name []byte) SeriesIterator {
	return &seriesDecodeIterator{
		itr:  f.mblk.seriesIDIterator(name),
		sblk: &f.sblk,
	}
}

// SeriesIterator returns an iterator over all series.
func (f *IndexFile) SeriesIterator() SeriesIterator {
	return f.sblk.SeriesIterator()
}

// ReadIndexFileTrailer returns the index file trailer from data.
func ReadIndexFileTrailer(data []byte) (IndexFileTrailer, error) {
	var t IndexFileTrailer

	// Read version.
	t.Version = int(binary.BigEndian.Uint16(data[len(data)-IndexFileVersionSize:]))
	if t.Version != IndexFileVersion {
		return t, ErrUnsupportedIndexFileVersion
	}

	// Slice trailer data.
	buf := data[len(data)-IndexFileTrailerSize:]

	// Read series list info.
	t.SeriesBlock.Offset = int64(binary.BigEndian.Uint64(buf[0:SeriesBlockOffsetSize]))
	buf = buf[SeriesBlockOffsetSize:]
	t.SeriesBlock.Size = int64(binary.BigEndian.Uint64(buf[0:SeriesBlockSizeSize]))
	buf = buf[SeriesBlockSizeSize:]

	// Read measurement block info.
	t.MeasurementBlock.Offset = int64(binary.BigEndian.Uint64(buf[0:MeasurementBlockOffsetSize]))
	buf = buf[MeasurementBlockOffsetSize:]
	t.MeasurementBlock.Size = int64(binary.BigEndian.Uint64(buf[0:MeasurementBlockSizeSize]))
	buf = buf[MeasurementBlockSizeSize:]

	return t, nil
}

// IndexFileTrailer represents meta data written to the end of the index file.
type IndexFileTrailer struct {
	Version     int
	SeriesBlock struct {
		Offset int64
		Size   int64
	}
	MeasurementBlock struct {
		Offset int64
		Size   int64
	}
}

// WriteTo writes the trailer to w.
func (t *IndexFileTrailer) WriteTo(w io.Writer) (n int64, err error) {
	// Write series list info.
	if err := writeUint64To(w, uint64(t.SeriesBlock.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.SeriesBlock.Size), &n); err != nil {
		return n, err
	}

	// Write measurement block info.
	if err := writeUint64To(w, uint64(t.MeasurementBlock.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.MeasurementBlock.Size), &n); err != nil {
		return n, err
	}

	// Write index file encoding version.
	if err := writeUint16To(w, IndexFileVersion, &n); err != nil {
		return n, err
	}

	return n, nil
}
