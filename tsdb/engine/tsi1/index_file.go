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
	SeriesListOffsetSize       = 8
	SeriesListSizeSize         = 8
	MeasurementBlockOffsetSize = 8
	MeasurementBlockSizeSize   = 8

	IndexFileTrailerSize = IndexFileVersionSize +
		SeriesListOffsetSize +
		SeriesListSizeSize +
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
	slist SeriesList
	mblk  MeasurementBlock
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
	buf = data[t.SeriesList.Offset:]
	buf = buf[:t.SeriesList.Size]

	// Unmarshal series list.
	if err := i.slist.UnmarshalBinary(buf); err != nil {
		return err
	}

	// Save reference to entire data block.
	i.data = data

	return nil
}

// Close closes the index file.
func (i *IndexFile) Close() error {
	i.slist = SeriesList{}
	i.mblk = MeasurementBlock{}
	return nil
}

// TagValueElem returns a list of series ids for a measurement/tag/value.
func (i *IndexFile) TagValueElem(name, key, value []byte) (TagSetValueElem, error) {
	// Find measurement.
	e, ok := i.mblk.Elem(name)
	if !ok {
		return TagSetValueElem{}, nil
	}

	// Find tag set block.
	tblk, err := i.tagSetBlock(&e)
	if err != nil {
		return TagSetValueElem{}, err
	}
	return tblk.TagValueElem(key, value), nil
}

// tagSetBlock returns a tag set block for a measurement.
func (i *IndexFile) tagSetBlock(e *MeasurementBlockElem) (TagSet, error) {
	// Slice tag set data.
	buf := i.data[e.TagSet.Offset:]
	buf = buf[:e.TagSet.Size]

	// Unmarshal block.
	var blk TagSet
	if err := blk.UnmarshalBinary(buf); err != nil {
		return TagSet{}, err
	}
	return blk, nil
}

// MeasurementIterator returns an iterator over all measurements.
func (i *IndexFile) MeasurementIterator() MeasurementIterator {
	return i.mblk.Iterator()
}

// MeasurementSeriesIterator returns an iterator over a measurement's series.
func (i *IndexFile) MeasurementSeriesIterator(name []byte) SeriesIterator {
	// Find measurement element.
	e, ok := i.mblk.Elem(name)
	if !ok {
		return &seriesIterator{}
	}

	// Return iterator.
	return &rawSeriesIterator{
		n:          e.Series.N,
		data:       e.Series.Data,
		seriesList: &i.slist,
	}
}

// rawSeriesIterator iterates over a list of raw data.
type rawSeriesIterator struct {
	i, n uint32 // index & total count
	data []byte // raw data

	// series list used for decoding
	seriesList *SeriesList

	// reusable buffer
	e SeriesElem
}

// Next returns the next decoded series. Uses name & tags as reusable buffers.
// Returns nils when the iterator is complete.
func (itr *rawSeriesIterator) Next() *SeriesElem {
	// Return nil if we've reached the end.
	if itr.i == itr.n {
		return nil
	}

	// Move forward and retrieved offset.
	offset := binary.BigEndian.Uint32(itr.data[itr.i*SeriesIDSize:])

	// Read from series list into buffers.
	itr.seriesList.DecodeSeriesAt(offset, &itr.e.Name, &itr.e.Tags, &itr.e.Deleted)

	// Move iterator forward.
	itr.i++

	return &itr.e
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
	t.SeriesList.Offset = int64(binary.BigEndian.Uint64(buf[0:SeriesListOffsetSize]))
	buf = buf[SeriesListOffsetSize:]
	t.SeriesList.Size = int64(binary.BigEndian.Uint64(buf[0:SeriesListSizeSize]))
	buf = buf[SeriesListSizeSize:]

	// Read measurement block info.
	t.MeasurementBlock.Offset = int64(binary.BigEndian.Uint64(buf[0:MeasurementBlockOffsetSize]))
	buf = buf[MeasurementBlockOffsetSize:]
	t.MeasurementBlock.Size = int64(binary.BigEndian.Uint64(buf[0:MeasurementBlockSizeSize]))
	buf = buf[MeasurementBlockSizeSize:]

	return t, nil
}

// IndexFileTrailer represents meta data written to the end of the index file.
type IndexFileTrailer struct {
	Version    int
	SeriesList struct {
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
	if err := writeUint64To(w, uint64(t.SeriesList.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.SeriesList.Size), &n); err != nil {
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
