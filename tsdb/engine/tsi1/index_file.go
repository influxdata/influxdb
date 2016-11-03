package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
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
	sblk SeriesBlock
	mblk MeasurementBlock
}

// UnmarshalBinary opens an index from data.
// The byte slice is retained so it must be kept open.
func (i *IndexFile) UnmarshalBinary(data []byte) error {
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
	if err := i.mblk.UnmarshalBinary(buf); err != nil {
		return err
	}

	// Slice series list data.
	buf = data[t.SeriesBlock.Offset:]
	buf = buf[:t.SeriesBlock.Size]

	// Unmarshal series list.
	if err := i.sblk.UnmarshalBinary(buf); err != nil {
		return err
	}

	// Save reference to entire data block.
	i.data = data

	return nil
}

// Close closes the index file.
func (i *IndexFile) Close() error {
	i.sblk = SeriesBlock{}
	i.mblk = MeasurementBlock{}
	return nil
}

// TagValueElem returns a list of series ids for a measurement/tag/value.
func (i *IndexFile) TagValueElem(name, key, value []byte) (TagBlockValueElem, error) {
	// Find measurement.
	e, ok := i.mblk.Elem(name)
	if !ok {
		return TagBlockValueElem{}, nil
	}

	// Find tag block.
	tblk, err := i.tagBlock(&e)
	if err != nil {
		return TagBlockValueElem{}, err
	}
	return tblk.TagValueElem(key, value), nil
}

// tagBlock returns a tag block for a measurement.
func (i *IndexFile) tagBlock(e *MeasurementBlockElem) (TagBlock, error) {
	// Slice data.
	buf := i.data[e.tagBlock.offset:]
	buf = buf[:e.tagBlock.size]

	// Unmarshal block.
	var blk TagBlock
	if err := blk.UnmarshalBinary(buf); err != nil {
		return TagBlock{}, err
	}
	return blk, nil
}

// MeasurementIterator returns an iterator over all measurements.
func (i *IndexFile) MeasurementIterator() MeasurementIterator {
	return i.mblk.Iterator()
}

// TagKeyIterator returns an iterator over all tag keys for a measurement.
func (i *IndexFile) TagKeyIterator(name []byte) (TagKeyIterator, error) {
	// Create an internal iterator.
	bitr, err := i.tagBlockKeyIterator(name)
	if err != nil {
		return nil, err
	}

	// Decode into an externally accessible iterator.
	itr := newTagKeyDecodeIterator(&i.sblk)
	itr.itr = bitr
	return &itr, nil
}

// tagBlockKeyIterator returns an internal iterator over all tag keys for a measurement.
func (i *IndexFile) tagBlockKeyIterator(name []byte) (tagBlockKeyIterator, error) {
	// Find measurement element.
	e, ok := i.mblk.Elem(name)
	if !ok {
		return tagBlockKeyIterator{}, nil
	}

	// Fetch tag block.
	blk, err := i.tagBlock(&e)
	if err != nil {
		return tagBlockKeyIterator{}, err
	}
	return blk.tagKeyIterator(), nil
}

// MeasurementSeriesIterator returns an iterator over a measurement's series.
func (i *IndexFile) MeasurementSeriesIterator(name []byte) SeriesIterator {
	return &seriesDecodeIterator{
		itr:  i.mblk.seriesIDIterator(name),
		sblk: &i.sblk,
	}
}

// SeriesIterator returns an iterator over all series.
func (i *IndexFile) SeriesIterator() SeriesIterator {
	return i.sblk.SeriesIterator()
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
