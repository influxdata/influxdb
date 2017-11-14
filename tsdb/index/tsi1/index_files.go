package tsi1

import (
	"bufio"
	"io"
	"os"
	"sort"
	"time"

	"github.com/influxdata/influxdb/pkg/bytesutil"
	"github.com/influxdata/influxdb/tsdb"
)

// IndexFiles represents a layered set of index files.
type IndexFiles []*IndexFile

// IDs returns the ids for all index files.
func (p IndexFiles) IDs() []int {
	a := make([]int, len(p))
	for i, f := range p {
		a[i] = f.ID()
	}
	return a
}

// Retain adds a reference count to all files.
func (p IndexFiles) Retain() {
	for _, f := range p {
		f.Retain()
	}
}

// Release removes a reference count from all files.
func (p IndexFiles) Release() {
	for _, f := range p {
		f.Release()
	}
}

// Files returns p as a list of File objects.
func (p IndexFiles) Files() []File {
	other := make([]File, len(p))
	for i, f := range p {
		other[i] = f
	}
	return other
}

// MeasurementNames returns a sorted list of all measurement names for all files.
func (p *IndexFiles) MeasurementNames() [][]byte {
	itr := p.MeasurementIterator()
	if itr == nil {
		return nil
	}

	var names [][]byte
	for e := itr.Next(); e != nil; e = itr.Next() {
		names = append(names, bytesutil.Clone(e.Name()))
	}
	sort.Sort(byteSlices(names))
	return names
}

// MeasurementIterator returns an iterator that merges measurements across all files.
func (p IndexFiles) MeasurementIterator() MeasurementIterator {
	a := make([]MeasurementIterator, 0, len(p))
	for i := range p {
		itr := p[i].MeasurementIterator()
		if itr == nil {
			continue
		}
		a = append(a, itr)
	}
	return MergeMeasurementIterators(a...)
}

// TagKeyIterator returns an iterator that merges tag keys across all files.
func (p *IndexFiles) TagKeyIterator(name []byte) (TagKeyIterator, error) {
	a := make([]TagKeyIterator, 0, len(*p))
	for _, f := range *p {
		itr := f.TagKeyIterator(name)
		if itr == nil {
			continue
		}
		a = append(a, itr)
	}
	return MergeTagKeyIterators(a...), nil
}

// MeasurementSeriesIDIterator returns an iterator that merges series across all files.
func (p IndexFiles) MeasurementSeriesIDIterator(name []byte) tsdb.SeriesIDIterator {
	a := make([]tsdb.SeriesIDIterator, 0, len(p))
	for _, f := range p {
		itr := f.MeasurementSeriesIDIterator(name)
		if itr == nil {
			continue
		}
		a = append(a, itr)
	}
	return MergeSeriesIDIterators(a...)
}

// TagValueSeriesIDIterator returns an iterator that merges series across all files.
func (p IndexFiles) TagValueSeriesIDIterator(name, key, value []byte) tsdb.SeriesIDIterator {
	a := make([]tsdb.SeriesIDIterator, 0, len(p))

	for i := range p {
		itr := p[i].TagValueSeriesIDIterator(name, key, value)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeSeriesIDIterators(a...)
}

// CompactTo merges all index files and writes them to w.
func (p IndexFiles) CompactTo(w io.Writer, sfile *SeriesFile, m, k uint64) (n int64, err error) {
	var t IndexFileTrailer

	// Wrap writer in buffered I/O.
	bw := bufio.NewWriter(w)

	// Setup context object to track shared data for this compaction.
	var info indexCompactInfo
	info.tagSets = make(map[string]indexTagSetPos)

	// Write magic number.
	if err := writeTo(bw, []byte(FileSignature), &n); err != nil {
		return n, err
	}

	// Flush buffer before re-mapping.
	if err := bw.Flush(); err != nil {
		return n, err
	}

	// Write tagset blocks in measurement order.
	if err := p.writeTagsetsTo(bw, &info, &n); err != nil {
		return n, err
	}

	// Write measurement block.
	t.MeasurementBlock.Offset = n
	if err := p.writeMeasurementBlockTo(bw, &info, &n); err != nil {
		return n, err
	}
	t.MeasurementBlock.Size = n - t.MeasurementBlock.Offset

	// Write trailer.
	nn, err := t.WriteTo(bw)
	n += nn
	if err != nil {
		return n, err
	}

	// Flush file.
	if err := bw.Flush(); err != nil {
		return n, err
	}

	return n, nil
}

func (p IndexFiles) writeTagsetsTo(w io.Writer, info *indexCompactInfo, n *int64) error {
	mitr := p.MeasurementIterator()
	if mitr == nil {
		return nil
	}

	for m := mitr.Next(); m != nil; m = mitr.Next() {
		if err := p.writeTagsetTo(w, m.Name(), info, n); err != nil {
			return err
		}
	}
	return nil
}

// writeTagsetTo writes a single tagset to w and saves the tagset offset.
func (p IndexFiles) writeTagsetTo(w io.Writer, name []byte, info *indexCompactInfo, n *int64) error {
	var seriesIDs []uint64

	kitr, err := p.TagKeyIterator(name)
	if err != nil {
		return err
	}

	enc := NewTagBlockEncoder(w)
	for ke := kitr.Next(); ke != nil; ke = kitr.Next() {
		// Encode key.
		if err := enc.EncodeKey(ke.Key(), ke.Deleted()); err != nil {
			return err
		}

		// Iterate over tag values.
		vitr := ke.TagValueIterator()
		for ve := vitr.Next(); ve != nil; ve = vitr.Next() {
			seriesIDs = seriesIDs[:0]

			// Merge all series together.
			sitr := p.TagValueSeriesIDIterator(name, ke.Key(), ve.Value())
			for se := sitr.Next(); se.SeriesID != 0; se = sitr.Next() {
				seriesIDs = append(seriesIDs, se.SeriesID)
			}

			// Encode value.
			if err := enc.EncodeValue(ve.Value(), ve.Deleted(), seriesIDs); err != nil {
				return err
			}
		}
	}

	// Save tagset offset to measurement.
	pos := info.tagSets[string(name)]
	pos.offset = *n

	// Flush data to writer.
	err = enc.Close()
	*n += enc.N()
	if err != nil {
		return err
	}

	// Save tagset size to measurement.
	pos.size = *n - pos.offset

	info.tagSets[string(name)] = pos

	return nil
}

func (p IndexFiles) writeMeasurementBlockTo(w io.Writer, info *indexCompactInfo, n *int64) error {
	mw := NewMeasurementBlockWriter()

	// Add measurement data & compute sketches.
	mitr := p.MeasurementIterator()
	if mitr != nil {
		for m := mitr.Next(); m != nil; m = mitr.Next() {
			name := m.Name()

			// Look-up series ids.
			itr := p.MeasurementSeriesIDIterator(name)
			var seriesIDs []uint64
			for e := itr.Next(); e.SeriesID != 0; e = itr.Next() {
				seriesIDs = append(seriesIDs, e.SeriesID)
			}
			sort.Sort(uint64Slice(seriesIDs))

			// Add measurement to writer.
			pos := info.tagSets[string(name)]
			mw.Add(name, m.Deleted(), pos.offset, pos.size, seriesIDs)
		}
	}

	// Flush data to writer.
	nn, err := mw.WriteTo(w)
	*n += nn
	return err
}

// Stat returns the max index file size and the total file size for all index files.
func (p IndexFiles) Stat() (*IndexFilesInfo, error) {
	var info IndexFilesInfo
	for _, f := range p {
		fi, err := os.Stat(f.Path())
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return nil, err
		}

		if fi.Size() > info.MaxSize {
			info.MaxSize = fi.Size()
		}
		if fi.ModTime().After(info.ModTime) {
			info.ModTime = fi.ModTime()
		}

		info.Size += fi.Size()
	}
	return &info, nil
}

type IndexFilesInfo struct {
	MaxSize int64     // largest file size
	Size    int64     // total file size
	ModTime time.Time // last modified
}

// indexCompactInfo is a context object used for tracking position information
// during the compaction of index files.
type indexCompactInfo struct {
	sfile *SeriesFile

	// Tracks offset/size for each measurement's tagset.
	tagSets map[string]indexTagSetPos
}

// indexTagSetPos stores the offset/size of tagsets.
type indexTagSetPos struct {
	offset int64
	size   int64
}
