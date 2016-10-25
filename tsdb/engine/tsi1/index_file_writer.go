package tsi1

/*
import (
	"bytes"
	"io"
	"sort"

	"github.com/influxdata/influxdb/models"
)

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

// AddSeries adds a series to the index file.
func (iw *IndexFileWriter) AddSeries(name []byte, tags models.Tags) {
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

// DeleteSeries removes a series from the writer.
func (iw *IndexFileWriter) DeleteSeries(name []byte, tags models.Tags) {
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
*/
