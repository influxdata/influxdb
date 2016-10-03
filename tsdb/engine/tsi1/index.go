package tsi1

import (
	"encoding/binary"
	"errors"
	"io"
	"sort"

	"github.com/influxdata/influxdb/models"
)

// IndexVersion is the current TSI1 index version.
const IndexVersion = 1

// Index field size constants.
const (
	// Index trailer fields
	IndexVersionSize           = 2
	SeriesListOffsetSize       = 8
	SeriesListSizeSize         = 8
	MeasurementBlockOffsetSize = 8
	MeasurementBlockSizeSize   = 8

	IndexTrailerSize = IndexVersionSize +
		SeriesListOffsetSize +
		SeriesListSizeSize +
		MeasurementBlockOffsetSize +
		MeasurementBlockSizeSize
)

// Index errors.
var (
	ErrUnsupportedIndexVersion = errors.New("unsupported index version")
)

// Index represents a collection of measurement, tag, and series data.
type Index struct {
	data []byte

	// Components
	slist SeriesList
	mblk  MeasurementBlock
}

// UnmarshalBinary opens an index from data.
// The byte slice is retained so it must be kept open.
func (i *Index) UnmarshalBinary(data []byte) error {
	// Read index trailer.
	t, err := ReadIndexTrailer(data)
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

	Hexdump(buf)

	// Unmarshal series list.
	if err := i.slist.UnmarshalBinary(buf); err != nil {
		return err
	}

	return nil
}

// Close closes the index file.
func (i *Index) Close() error {
	i.slist = SeriesList{}
	i.mblk = MeasurementBlock{}
	return nil
}

// Indices represents a layered set of indices.
type Indices []*Index

// IndexWriter represents a naive implementation of an index builder.
type IndexWriter struct {
	series indexSeries
	mms    indexMeasurements
}

// NewIndexWriter returns a new instance of IndexWriter.
func NewIndexWriter() *IndexWriter {
	return &IndexWriter{
		mms: make(indexMeasurements),
	}
}

// Add adds a series to the index.
func (iw *IndexWriter) Add(name string, tags models.Tags) {
	// Add to series list.
	iw.series = append(iw.series, indexSerie{name: name, tags: tags})

	// Find or create measurement.
	mm, ok := iw.mms[name]
	if !ok {
		mm.name = []byte(name)
		mm.tagset = make(indexTagset)
		iw.mms[name] = mm
	}

	// Add tagset.
	for _, tag := range tags {
		t, ok := mm.tagset[string(tag.Key)]
		if !ok {
			t.name = tag.Key
			t.values = make(indexValues)
			mm.tagset[string(tag.Key)] = t
		}

		v, ok := t.values[string(tag.Value)]
		if !ok {
			v.name = tag.Value
			t.values[string(tag.Value)] = v
		}
	}
}

// WriteTo writes the index to w.
func (iw *IndexWriter) WriteTo(w io.Writer) (n int64, err error) {
	var t IndexTrailer

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

func (iw *IndexWriter) writeSeriesListTo(w io.Writer, n *int64) error {
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
		mm := iw.mms[serie.name]
		mm.seriesIDs = append(mm.seriesIDs, serie.offset)
		iw.mms[serie.name] = mm

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

func (iw *IndexWriter) writeTagsetsTo(w io.Writer, names []string, n *int64) error {
	for _, name := range names {
		if err := iw.writeTagsetTo(w, name, n); err != nil {
			return err
		}
	}
	return nil
}

// writeTagsetTo writes a single tagset to w and saves the tagset offset.
func (iw *IndexWriter) writeTagsetTo(w io.Writer, name string, n *int64) error {
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

	// Write tagset to writer.
	nn, err := tsw.WriteTo(w)
	*n += nn
	if err != nil {
		return err
	}

	// Save tagset offset to measurement.
	mm.offset = uint64(*n)

	return nil
}

func (iw *IndexWriter) writeMeasurementBlockTo(w io.Writer, names []string, n *int64) error {
	mw := NewMeasurementBlockWriter()

	// Add measurement data.
	for _, mm := range iw.mms {
		mw.Add(mm.name, mm.offset, mm.seriesIDs)
	}

	// Write data to writer.
	nn, err := mw.WriteTo(w)
	*n += nn
	if err != nil {
		return err
	}

	return nil
}

// writeTrailerTo writes the index trailer to w.
func (iw *IndexWriter) writeTrailerTo(w io.Writer, t IndexTrailer, n *int64) error {
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

	// Write index encoding version.
	if err := writeUint16To(w, IndexVersion, n); err != nil {
		return err
	}

	return nil
}

type indexSerie struct {
	name    string
	tags    models.Tags
	deleted bool
	offset  uint32
}

type indexSeries []indexSerie

func (a indexSeries) Len() int      { return len(a) }
func (a indexSeries) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a indexSeries) Less(i, j int) bool {
	if a[i].name != a[j].name {
		return a[i].name < a[j].name
	}
	return models.CompareTags(a[i].tags, a[j].tags) == -1
}

type indexMeasurement struct {
	name      []byte
	deleted   bool
	tagset    indexTagset
	offset    uint64
	seriesIDs []uint32
}

type indexMeasurements map[string]indexMeasurement

// Names returns a sorted list of measurement names.
func (m indexMeasurements) Names() []string {
	a := make([]string, 0, len(m))
	for name := range m {
		a = append(a, name)
	}
	sort.Strings(a)
	return a
}

type indexTag struct {
	name    []byte
	deleted bool
	values  indexValues
}

type indexTagset map[string]indexTag

type indexValue struct {
	name      []byte
	deleted   bool
	seriesIDs []uint32
}

type indexValues map[string]indexValue

// ReadIndexTrailer returns the index trailer from data.
func ReadIndexTrailer(data []byte) (IndexTrailer, error) {
	var t IndexTrailer

	// Read version.
	t.Version = int(binary.BigEndian.Uint16(data[len(data)-IndexVersionSize:]))
	if t.Version != IndexVersion {
		return t, ErrUnsupportedIndexVersion
	}

	// Slice trailer data.
	buf := data[len(data)-IndexTrailerSize:]

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

// IndexTrailer represents meta data written to the end of the index.
type IndexTrailer struct {
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
