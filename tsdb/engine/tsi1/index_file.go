package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"

	"github.com/influxdata/influxdb/models"
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
func (i *IndexFile) TagValueElem(name, key, value []byte) (TagValueElem, error) {
	// Find measurement.
	e, ok := i.mblk.Elem(name)
	if !ok {
		return TagValueElem{}, nil
	}

	// Find tag set block.
	tblk, err := i.tagSetBlock(&e)
	if err != nil {
		return TagValueElem{}, err
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
	return &seriesIterator{
		n:          e.Series.N,
		data:       e.Series.Data,
		seriesList: &i.slist,
	}
}

// seriesIterator iterates over a list of raw data.
type seriesIterator struct {
	i, n uint32
	data []byte

	seriesList *SeriesList
}

// Next returns the next decoded series. Uses name & tags as reusable buffers.
// Returns nils when the iterator is complete.
func (itr *seriesIterator) Next(name *[]byte, tags *models.Tags, deleted *bool) {
	// Return nil if we've reached the end.
	if itr.i == itr.n {
		*name, *tags = nil, nil
		return
	}

	// Move forward and retrieved offset.
	offset := binary.BigEndian.Uint32(itr.data[itr.i*SeriesIDSize:])

	// Read from series list into buffers.
	itr.seriesList.DecodeSeriesAt(offset, name, tags, deleted)

	// Move iterator forward.
	itr.i++
}

// IndexFiles represents a layered set of index files.
type IndexFiles []*IndexFile

// IndexFileWriter represents a naive implementation of an index file builder.
type IndexFileWriter struct {
	series indexFileSeries
	mms    indexFileMeasurements
}

// NewIndexFileWriter returns a new instance of IndexFileWriter.
func NewIndexFileWriter() *IndexFileWriter {
	return &IndexFileWriter{
		mms: make(indexFileMeasurements),
	}
}

// Add adds a series to the index file.
func (iw *IndexFileWriter) Add(name []byte, tags models.Tags) {
	// Add to series list.
	iw.series = append(iw.series, indexFileSerie{name: name, tags: tags})

	// Find or create measurement.
	mm, ok := iw.mms[string(name)]
	if !ok {
		mm.name = name
		mm.tagset = make(indexFileTagset)
		iw.mms[string(name)] = mm
	}

	// Add tagset.
	for _, tag := range tags {
		t, ok := mm.tagset[string(tag.Key)]
		if !ok {
			t.name = tag.Key
			t.values = make(indexFileValues)
			mm.tagset[string(tag.Key)] = t
		}

		v, ok := t.values[string(tag.Value)]
		if !ok {
			v.name = tag.Value
			t.values[string(tag.Value)] = v
		}
	}
}

// WriteTo writes the index file to w.
func (iw *IndexFileWriter) WriteTo(w io.Writer) (n int64, err error) {
	var t IndexFileTrailer

	// Write magic number.
	if err := writeTo(w, []byte(FileSignature), &n); err != nil {
		return n, err
	}

	// Write series list.
	t.SeriesList.Offset = n
	if err := iw.writeSeriesListTo(w, &n); err != nil {
		return n, err
	}
	t.SeriesList.Size = n - t.SeriesList.Offset

	// Sort measurement names.
	names := iw.mms.Names()

	// Write tagset blocks in measurement order.
	if err := iw.writeTagsetsTo(w, names, &n); err != nil {
		return n, err
	}

	// Write measurement block.
	t.MeasurementBlock.Offset = n
	if err := iw.writeMeasurementBlockTo(w, names, &n); err != nil {
		return n, err
	}
	t.MeasurementBlock.Size = n - t.MeasurementBlock.Offset

	// Write trailer.
	if err := iw.writeTrailerTo(w, t, &n); err != nil {
		return n, err
	}

	return n, nil
}

func (iw *IndexFileWriter) writeSeriesListTo(w io.Writer, n *int64) error {
	// Ensure series are sorted.
	sort.Sort(iw.series)

	// Write all series.
	sw := NewSeriesListWriter()
	for _, serie := range iw.series {
		if err := sw.Add(serie.name, serie.tags); err != nil {
			return err
		}
	}

	// Flush series list.
	nn, err := sw.WriteTo(w)
	*n += nn
	if err != nil {
		return err
	}

	// Add series to each measurement and key/value.
	for i := range iw.series {
		serie := &iw.series[i]

		// Lookup series offset.
		serie.offset = sw.Offset(serie.name, serie.tags)
		if serie.offset == 0 {
			panic("series not found")
		}

		// Add series id to measurement, tag key, and tag value.
		mm := iw.mms[string(serie.name)]
		mm.seriesIDs = append(mm.seriesIDs, serie.offset)
		iw.mms[string(serie.name)] = mm

		// Add series id to each tag value.
		for _, tag := range serie.tags {
			t := mm.tagset[string(tag.Key)]

			v := t.values[string(tag.Value)]
			v.seriesIDs = append(v.seriesIDs, serie.offset)
			t.values[string(tag.Value)] = v
		}
	}

	return nil
}

func (iw *IndexFileWriter) writeTagsetsTo(w io.Writer, names []string, n *int64) error {
	for _, name := range names {
		if err := iw.writeTagsetTo(w, name, n); err != nil {
			return err
		}
	}
	return nil
}

// writeTagsetTo writes a single tagset to w and saves the tagset offset.
func (iw *IndexFileWriter) writeTagsetTo(w io.Writer, name string, n *int64) error {
	mm := iw.mms[name]

	tsw := NewTagSetWriter()
	for _, tag := range mm.tagset {
		// Mark tag deleted.
		if tag.deleted {
			tsw.AddTag(tag.name, true)
			continue
		}

		// Add each value.
		for _, value := range tag.values {
			sort.Sort(uint32Slice(value.seriesIDs))
			tsw.AddTagValue(tag.name, value.name, value.deleted, value.seriesIDs)
		}
	}

	// Save tagset offset to measurement.
	mm.offset = *n

	// Write tagset to writer.
	nn, err := tsw.WriteTo(w)
	*n += nn
	if err != nil {
		return err
	}

	// Save tagset offset to measurement.
	mm.size = *n - mm.offset

	iw.mms[name] = mm

	return nil
}

func (iw *IndexFileWriter) writeMeasurementBlockTo(w io.Writer, names []string, n *int64) error {
	mw := NewMeasurementBlockWriter()

	// Add measurement data.
	for _, mm := range iw.mms {
		mw.Add(mm.name, mm.offset, mm.size, mm.seriesIDs)
	}

	// Write data to writer.
	nn, err := mw.WriteTo(w)
	*n += nn
	if err != nil {
		return err
	}

	return nil
}

// writeTrailerTo writes the index file trailer to w.
func (iw *IndexFileWriter) writeTrailerTo(w io.Writer, t IndexFileTrailer, n *int64) error {
	// Write series list info.
	if err := writeUint64To(w, uint64(t.SeriesList.Offset), n); err != nil {
		return err
	} else if err := writeUint64To(w, uint64(t.SeriesList.Size), n); err != nil {
		return err
	}

	// Write measurement block info.
	if err := writeUint64To(w, uint64(t.MeasurementBlock.Offset), n); err != nil {
		return err
	} else if err := writeUint64To(w, uint64(t.MeasurementBlock.Size), n); err != nil {
		return err
	}

	// Write index file encoding version.
	if err := writeUint16To(w, IndexFileVersion, n); err != nil {
		return err
	}

	return nil
}

type indexFileSerie struct {
	name    []byte
	tags    models.Tags
	deleted bool
	offset  uint32
}

type indexFileSeries []indexFileSerie

func (a indexFileSeries) Len() int      { return len(a) }
func (a indexFileSeries) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a indexFileSeries) Less(i, j int) bool {
	if cmp := bytes.Compare(a[i].name, a[j].name); cmp != 0 {
		return cmp == -1
	}
	return models.CompareTags(a[i].tags, a[j].tags) == -1
}

type indexFileMeasurement struct {
	name      []byte
	deleted   bool
	tagset    indexFileTagset
	offset    int64 // tagset offset
	size      int64 // tagset size
	seriesIDs []uint32
}

type indexFileMeasurements map[string]indexFileMeasurement

// Names returns a sorted list of measurement names.
func (m indexFileMeasurements) Names() []string {
	a := make([]string, 0, len(m))
	for name := range m {
		a = append(a, name)
	}
	sort.Strings(a)
	return a
}

type indexFileTag struct {
	name    []byte
	deleted bool
	values  indexFileValues
}

type indexFileTagset map[string]indexFileTag

type indexFileValue struct {
	name      []byte
	deleted   bool
	seriesIDs []uint32
}

type indexFileValues map[string]indexFileValue

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

type uint32Slice []uint32

func (a uint32Slice) Len() int           { return len(a) }
func (a uint32Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint32Slice) Less(i, j int) bool { return a[i] < a[j] }
