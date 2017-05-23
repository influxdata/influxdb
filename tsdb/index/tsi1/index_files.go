package tsi1

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/pkg/mmap"
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
	var names [][]byte
	for e := itr.Next(); e != nil; e = itr.Next() {
		names = append(names, copyBytes(e.Name()))
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

// SeriesIterator returns an iterator that merges series across all files.
func (p IndexFiles) SeriesIterator() SeriesIterator {
	a := make([]SeriesIterator, 0, len(p))
	for _, f := range p {
		itr := f.SeriesIterator()
		if itr == nil {
			continue
		}
		a = append(a, itr)
	}
	return MergeSeriesIterators(a...)
}

// MeasurementSeriesIterator returns an iterator that merges series across all files.
func (p IndexFiles) MeasurementSeriesIterator(name []byte) SeriesIterator {
	a := make([]SeriesIterator, 0, len(p))
	for _, f := range p {
		itr := f.MeasurementSeriesIterator(name)
		if itr == nil {
			continue
		}
		a = append(a, itr)
	}
	return MergeSeriesIterators(a...)
}

// TagValueSeriesIterator returns an iterator that merges series across all files.
func (p IndexFiles) TagValueSeriesIterator(name, key, value []byte) SeriesIterator {
	a := make([]SeriesIterator, 0, len(p))
	for i := range p {
		itr := p[i].TagValueSeriesIterator(name, key, value)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeSeriesIterators(a...)
}

// CompactTo merges all index files and writes them to w.
func (p IndexFiles) CompactTo(w io.Writer, m, k uint64) (n int64, err error) {
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

	// Write combined series list.
	t.SeriesBlock.Offset = n
	if err := p.writeSeriesBlockTo(bw, m, k, &info, &n); err != nil {
		return n, err
	}
	t.SeriesBlock.Size = n - t.SeriesBlock.Offset

	// Flush buffer before re-mapping.
	if err := bw.Flush(); err != nil {
		return n, err
	}

	// Open series block as memory-mapped data.
	sblk, data, err := mapIndexFileSeriesBlock(w)
	if data != nil {
		defer mmap.Unmap(data)
	}
	if err != nil {
		return n, err
	}
	info.sblk = sblk

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

func (p IndexFiles) writeSeriesBlockTo(w io.Writer, m, k uint64, info *indexCompactInfo, n *int64) error {
	// Estimate series cardinality.
	sketch := hll.NewDefaultPlus()
	for _, f := range p {
		if err := f.MergeSeriesSketches(sketch, sketch); err != nil {
			return err
		}
	}

	itr := p.SeriesIterator()
	enc := NewSeriesBlockEncoder(w, uint32(sketch.Count()), m, k)

	// Write all series.
	for e := itr.Next(); e != nil; e = itr.Next() {
		if err := enc.Encode(e.Name(), e.Tags(), e.Deleted()); err != nil {
			return err
		}
	}

	// Close and flush block.
	err := enc.Close()
	*n += int64(enc.N())
	if err != nil {
		return err
	}

	return nil
}

func (p IndexFiles) writeTagsetsTo(w io.Writer, info *indexCompactInfo, n *int64) error {
	mitr := p.MeasurementIterator()
	for m := mitr.Next(); m != nil; m = mitr.Next() {
		if err := p.writeTagsetTo(w, m.Name(), info, n); err != nil {
			return err
		}
	}
	return nil
}

// writeTagsetTo writes a single tagset to w and saves the tagset offset.
func (p IndexFiles) writeTagsetTo(w io.Writer, name []byte, info *indexCompactInfo, n *int64) error {
	var seriesKey []byte

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
			// Merge all series together.
			sitr := p.TagValueSeriesIterator(name, ke.Key(), ve.Value())
			var seriesIDs []uint32
			for se := sitr.Next(); se != nil; se = sitr.Next() {
				seriesID, _ := info.sblk.Offset(se.Name(), se.Tags(), seriesKey[:0])
				if seriesID == 0 {
					return fmt.Errorf("expected series id: %s/%s", se.Name(), se.Tags().String())
				}
				seriesIDs = append(seriesIDs, seriesID)
			}
			sort.Sort(uint32Slice(seriesIDs))

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
	var seriesKey []byte
	mw := NewMeasurementBlockWriter()

	// Add measurement data & compute sketches.
	mitr := p.MeasurementIterator()
	for m := mitr.Next(); m != nil; m = mitr.Next() {
		name := m.Name()

		// Look-up series ids.
		itr := p.MeasurementSeriesIterator(name)
		var seriesIDs []uint32
		for e := itr.Next(); e != nil; e = itr.Next() {
			seriesID, _ := info.sblk.Offset(e.Name(), e.Tags(), seriesKey[:0])
			if seriesID == 0 {
				panic(fmt.Sprintf("expected series id: %s %s", e.Name(), e.Tags().String()))
			}
			seriesIDs = append(seriesIDs, seriesID)
		}
		sort.Sort(uint32Slice(seriesIDs))

		// Add measurement to writer.
		pos := info.tagSets[string(name)]
		mw.Add(name, m.Deleted(), pos.offset, pos.size, seriesIDs)
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
	// Memory-mapped series block.
	// Available after the series block has been written.
	sblk *SeriesBlock

	// Tracks offset/size for each measurement's tagset.
	tagSets map[string]indexTagSetPos
}

// indexTagSetPos stores the offset/size of tagsets.
type indexTagSetPos struct {
	offset int64
	size   int64
}
