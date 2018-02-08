package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/pkg/mmap"
	"github.com/influxdata/influxdb/tsdb"
)

// IndexFileVersion is the current TSI1 index file version.
const IndexFileVersion = 1

// FileSignature represents a magic number at the header of the index file.
const FileSignature = "TSI1"

// IndexFile field size constants.
const (
	// IndexFile trailer fields
	IndexFileVersionSize = 2

	// IndexFileTrailerSize is the size of the trailer. Currently 82 bytes.
	IndexFileTrailerSize = IndexFileVersionSize +
		8 + 8 + // measurement block offset + size
		8 + 8 + // series id set offset + size
		8 + 8 + // tombstone series id set offset + size
		8 + 8 + // series sketch offset + size
		8 + 8 + // tombstone series sketch offset + size
		0
)

// IndexFile errors.
var (
	ErrInvalidIndexFile            = errors.New("invalid index file")
	ErrUnsupportedIndexFileVersion = errors.New("unsupported index file version")
)

// IndexFile represents a collection of measurement, tag, and series data.
type IndexFile struct {
	wg   sync.WaitGroup // ref count
	data []byte

	// Components
	sfile *tsdb.SeriesFile
	tblks map[string]*TagBlock // tag blocks by measurement name
	mblk  MeasurementBlock

	// Raw series set data.
	seriesIDSetData          []byte
	tombstoneSeriesIDSetData []byte

	// Series sketches
	sketch, tSketch estimator.Sketch

	// Sortable identifier & filepath to the log file.
	level int
	id    int

	// Compaction tracking.
	mu         sync.RWMutex
	compacting bool

	// Path to data file.
	path string
}

// NewIndexFile returns a new instance of IndexFile.
func NewIndexFile(sfile *tsdb.SeriesFile) *IndexFile {
	return &IndexFile{
		sfile:   sfile,
		sketch:  hll.NewDefaultPlus(),
		tSketch: hll.NewDefaultPlus(),
	}
}

// Open memory maps the data file at the file's path.
func (f *IndexFile) Open() error {
	defer func() {
		if err := recover(); err != nil {
			err = fmt.Errorf("[Index file: %s] %v", f.path, err)
			panic(err)
		}
	}()

	// Extract identifier from path name.
	f.id, f.level = ParseFilename(f.Path())

	data, err := mmap.Map(f.Path(), 0)
	if err != nil {
		return err
	}

	return f.UnmarshalBinary(data)
}

// Close unmaps the data file.
func (f *IndexFile) Close() error {
	// Wait until all references are released.
	f.wg.Wait()

	f.sfile = nil
	f.tblks = nil
	f.mblk = MeasurementBlock{}
	return mmap.Unmap(f.data)
}

// ID returns the file sequence identifier.
func (f *IndexFile) ID() int { return f.id }

// Path returns the file path.
func (f *IndexFile) Path() string { return f.path }

// SetPath sets the file's path.
func (f *IndexFile) SetPath(path string) { f.path = path }

// Level returns the compaction level for the file.
func (f *IndexFile) Level() int { return f.level }

// Retain adds a reference count to the file.
func (f *IndexFile) Retain() { f.wg.Add(1) }

// Release removes a reference count from the file.
func (f *IndexFile) Release() { f.wg.Done() }

// Size returns the size of the index file, in bytes.
func (f *IndexFile) Size() int64 { return int64(len(f.data)) }

// Compacting returns true if the file is being compacted.
func (f *IndexFile) Compacting() bool {
	f.mu.RLock()
	v := f.compacting
	f.mu.RUnlock()
	return v
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

	// Slice series sketch data.
	buf := data[t.SeriesSketch.Offset : t.SeriesSketch.Offset+t.SeriesSketch.Size]
	if err := f.sketch.UnmarshalBinary(buf); err != nil {
		return err
	}

	buf = data[t.TombstoneSeriesSketch.Offset : t.TombstoneSeriesSketch.Offset+t.TombstoneSeriesSketch.Size]
	if err := f.tSketch.UnmarshalBinary(buf); err != nil {
		return err
	}

	// Slice series set data.
	f.seriesIDSetData = data[t.SeriesIDSet.Offset : t.SeriesIDSet.Offset+t.SeriesIDSet.Size]
	f.tombstoneSeriesIDSetData = data[t.TombstoneSeriesIDSet.Offset : t.TombstoneSeriesIDSet.Offset+t.TombstoneSeriesIDSet.Size]

	// Slice measurement block data.
	buf = data[t.MeasurementBlock.Offset:]
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

	// Save reference to entire data block.
	f.data = data

	return nil
}

func (f *IndexFile) SeriesIDSet() (*tsdb.SeriesIDSet, error) {
	ss := tsdb.NewSeriesIDSet()
	if err := ss.UnmarshalBinary(f.seriesIDSetData); err != nil {
		return nil, err
	}
	return ss, nil
}

func (f *IndexFile) TombstoneSeriesIDSet() (*tsdb.SeriesIDSet, error) {
	ss := tsdb.NewSeriesIDSet()
	if err := ss.UnmarshalBinary(f.tombstoneSeriesIDSetData); err != nil {
		return nil, err
	}
	return ss, nil
}

// Measurement returns a measurement element.
func (f *IndexFile) Measurement(name []byte) MeasurementElem {
	e, ok := f.mblk.Elem(name)
	if !ok {
		return nil
	}
	return &e
}

// MeasurementN returns the number of measurements in the file.
func (f *IndexFile) MeasurementN() (n uint64) {
	mitr := f.mblk.Iterator()
	for me := mitr.Next(); me != nil; me = mitr.Next() {
		n++
	}
	return n
}

// MeasurementHasSeries returns true if a measurement has any non-tombstoned series.
func (f *IndexFile) MeasurementHasSeries(ss *tsdb.SeriesIDSet, name []byte) (ok bool) {
	e, ok := f.mblk.Elem(name)
	if !ok {
		return false
	}

	var exists bool
	e.ForEachSeriesID(func(id uint64) error {
		if ss.Contains(id) {
			exists = true
			return errors.New("done")
		}
		return nil
	})
	return exists
}

// TagValueIterator returns a value iterator for a tag key and a flag
// indicating if a tombstone exists on the measurement or key.
func (f *IndexFile) TagValueIterator(name, key []byte) TagValueIterator {
	tblk := f.tblks[string(name)]
	if tblk == nil {
		return nil
	}

	// Find key element.
	ke := tblk.TagKeyElem(key)
	if ke == nil {
		return nil
	}

	// Merge all value series iterators together.
	return ke.TagValueIterator()
}

// TagKeySeriesIDIterator returns a series iterator for a tag key and a flag
// indicating if a tombstone exists on the measurement or key.
func (f *IndexFile) TagKeySeriesIDIterator(name, key []byte) tsdb.SeriesIDIterator {
	tblk := f.tblks[string(name)]
	if tblk == nil {
		return nil
	}

	// Find key element.
	ke := tblk.TagKeyElem(key)
	if ke == nil {
		return nil
	}

	// Merge all value series iterators together.
	vitr := ke.TagValueIterator()
	var itrs []tsdb.SeriesIDIterator
	for ve := vitr.Next(); ve != nil; ve = vitr.Next() {
		sitr := &rawSeriesIDIterator{data: ve.(*TagBlockValueElem).series.data}
		itrs = append(itrs, sitr)
	}

	return tsdb.MergeSeriesIDIterators(itrs...)
}

// TagValueSeriesIDIterator returns a series iterator for a tag value and a flag
// indicating if a tombstone exists on the measurement, key, or value.
func (f *IndexFile) TagValueSeriesIDIterator(name, key, value []byte) tsdb.SeriesIDIterator {
	tblk := f.tblks[string(name)]
	if tblk == nil {
		return nil
	}

	// Find value element.
	n, data := tblk.TagValueSeriesData(key, value)
	if n == 0 {
		return nil
	}

	// Create an iterator over value's series.
	return &rawSeriesIDIterator{n: n, data: data}
}

// TagKey returns a tag key.
func (f *IndexFile) TagKey(name, key []byte) TagKeyElem {
	tblk := f.tblks[string(name)]
	if tblk == nil {
		return nil
	}
	return tblk.TagKeyElem(key)
}

// TagValue returns a tag value.
func (f *IndexFile) TagValue(name, key, value []byte) TagValueElem {
	tblk := f.tblks[string(name)]
	if tblk == nil {
		return nil
	}
	return tblk.TagValueElem(key, value)
}

// HasSeries returns flags indicating if the series exists and if it is tombstoned.
func (f *IndexFile) HasSeries(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool) {
	return f.sfile.HasSeries(name, tags, buf), false // TODO(benbjohnson): series tombstone
}

// TagValueElem returns an element for a measurement/tag/value.
func (f *IndexFile) TagValueElem(name, key, value []byte) TagValueElem {
	tblk, ok := f.tblks[string(name)]
	if !ok {
		return nil
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

// MeasurementSeriesIDIterator returns an iterator over a measurement's series.
func (f *IndexFile) MeasurementSeriesIDIterator(name []byte) tsdb.SeriesIDIterator {
	return f.mblk.SeriesIDIterator(name)
}

// MergeMeasurementsSketches merges the index file's measurements sketches into
// the provided sketches.
func (f *IndexFile) MergeMeasurementsSketches(s, t estimator.Sketch) error {
	if err := s.Merge(f.mblk.sketch); err != nil {
		return err
	}
	return t.Merge(f.mblk.tSketch)
}

// MergeSeriesSketches merges the index file's series sketches into the provided
// sketches.
func (f *IndexFile) MergeSeriesSketches(s, t estimator.Sketch) error {
	if err := s.Merge(f.sketch); err != nil {
		return err
	}
	return t.Merge(f.tSketch)
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

	// Read measurement block info.
	t.MeasurementBlock.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.MeasurementBlock.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read series id set info.
	t.SeriesIDSet.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.SeriesIDSet.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read series tombstone id set info.
	t.TombstoneSeriesIDSet.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.TombstoneSeriesIDSet.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read series sketch set info.
	t.SeriesSketch.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.SeriesSketch.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read series tombstone sketch info.
	t.TombstoneSeriesSketch.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.TombstoneSeriesSketch.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	if len(buf) != 2 { // Version field still in buffer.
		return t, fmt.Errorf("unread %d bytes left unread in trailer", len(buf)-2)
	}
	return t, nil
}

// IndexFileTrailer represents meta data written to the end of the index file.
type IndexFileTrailer struct {
	Version int

	MeasurementBlock struct {
		Offset int64
		Size   int64
	}

	SeriesIDSet struct {
		Offset int64
		Size   int64
	}

	TombstoneSeriesIDSet struct {
		Offset int64
		Size   int64
	}

	SeriesSketch struct {
		Offset int64
		Size   int64
	}

	TombstoneSeriesSketch struct {
		Offset int64
		Size   int64
	}
}

// WriteTo writes the trailer to w.
func (t *IndexFileTrailer) WriteTo(w io.Writer) (n int64, err error) {
	// Write measurement block info.
	if err := writeUint64To(w, uint64(t.MeasurementBlock.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.MeasurementBlock.Size), &n); err != nil {
		return n, err
	}

	// Write series id set info.
	if err := writeUint64To(w, uint64(t.SeriesIDSet.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.SeriesIDSet.Size), &n); err != nil {
		return n, err
	}

	// Write tombstone series id set info.
	if err := writeUint64To(w, uint64(t.TombstoneSeriesIDSet.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.TombstoneSeriesIDSet.Size), &n); err != nil {
		return n, err
	}

	// Write series sketch info.
	if err := writeUint64To(w, uint64(t.SeriesSketch.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.SeriesSketch.Size), &n); err != nil {
		return n, err
	}

	// Write series tombstone sketch info.
	if err := writeUint64To(w, uint64(t.TombstoneSeriesSketch.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.TombstoneSeriesSketch.Size), &n); err != nil {
		return n, err
	}

	// Write index file encoding version.
	if err := writeUint16To(w, IndexFileVersion, &n); err != nil {
		return n, err
	}

	return n, nil
}

// FormatIndexFileName generates an index filename for the given index.
func FormatIndexFileName(id, level int) string {
	return fmt.Sprintf("L%d-%08d%s", level, id, IndexFileExt)
}
