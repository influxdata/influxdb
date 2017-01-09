package tsi1

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb/pkg/estimator/hll"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/mmap"
)

// Log errors.
var (
	ErrLogEntryChecksumMismatch = errors.New("log entry checksum mismatch")
)

// Log entry flag constants.
const (
	LogEntrySeriesTombstoneFlag      = 0x01
	LogEntryMeasurementTombstoneFlag = 0x02
	LogEntryTagKeyTombstoneFlag      = 0x04
	LogEntryTagValueTombstoneFlag    = 0x08
)

// LogFile represents an on-disk write-ahead log file.
type LogFile struct {
	mu   sync.RWMutex
	wg   sync.WaitGroup // ref count
	data []byte         // mmap
	file *os.File       // writer
	w    *bufio.Writer  // buffered writer
	buf  []byte         // marshaling buffer

	size    int64     // tracks current file size
	modTime time.Time // tracks last time write occurred

	mSketch, mTSketch estimator.Sketch // Measurement sketches
	sSketch, sTSketch estimator.Sketch // Series sketche

	// In-memory index.
	mms logMeasurements

	// Filepath to the log file.
	Path string
}

// NewLogFile returns a new instance of LogFile.
func NewLogFile() *LogFile {
	return &LogFile{
		mms:      make(logMeasurements),
		mSketch:  hll.NewDefaultPlus(),
		mTSketch: hll.NewDefaultPlus(),
		sSketch:  hll.NewDefaultPlus(),
		sTSketch: hll.NewDefaultPlus(),
	}
}

// Open reads the log from a file and validates all the checksums.
func (f *LogFile) Open() error {
	if err := f.open(); err != nil {
		f.Close()
		return err
	}
	return nil
}

func (f *LogFile) open() error {
	// Open file for appending.
	file, err := os.OpenFile(f.Path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	f.file = file
	f.w = bufio.NewWriter(f.file)

	// Finish opening if file is empty.
	fi, err := file.Stat()
	if err != nil {
		return err
	} else if fi.Size() == 0 {
		return nil
	}
	f.size = fi.Size()
	f.modTime = fi.ModTime()

	// Open a read-only memory map of the existing data.
	data, err := mmap.Map(f.Path)
	if err != nil {
		return err
	}
	f.data = data

	// Read log entries from mmap.
	for buf := f.data; len(buf) > 0; {
		// Read next entry.
		var e LogEntry
		if err := e.UnmarshalBinary(buf); err != nil {
			return err
		}

		// Execute entry against in-memory index.
		f.execEntry(&e)

		// Move buffer forward.
		buf = buf[e.Size:]
	}

	return nil
}

// Close shuts down the file handle and mmap.
func (f *LogFile) Close() error {
	// Wait until the file has no more references.
	f.wg.Wait()

	if f.w != nil {
		f.w.Flush()
		f.w = nil
	}

	if f.file != nil {
		f.file.Close()
		f.file = nil
	}

	if f.data != nil {
		mmap.Unmap(f.data)
	}

	f.mms = make(logMeasurements)

	return nil
}

// Retain adds a reference count to the file.
func (f *LogFile) Retain() { f.wg.Add(1) }

// Release removes a reference count from the file.
func (f *LogFile) Release() { f.wg.Done() }

// Stat returns size and last modification time of the file.
func (f *LogFile) Stat() (int64, time.Time) {
	f.mu.Lock()
	size, modTime := f.size, f.modTime
	f.mu.Unlock()
	return size, modTime
}

// Measurement returns a measurement element.
func (f *LogFile) Measurement(name []byte) MeasurementElem {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}
	return mm
}

// MeasurementNames returns an ordered list of measurement names.
func (f *LogFile) MeasurementNames() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.measurementNames()
}

func (f *LogFile) measurementNames() []string {
	a := make([]string, 0, len(f.mms))
	for name := range f.mms {
		a = append(a, name)
	}
	return a
}

// DeleteMeasurement adds a tombstone for a measurement to the log file.
func (f *LogFile) DeleteMeasurement(name []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	e := LogEntry{Flag: LogEntryMeasurementTombstoneFlag, Name: name}
	if err := f.appendEntry(&e); err != nil {
		return err
	}
	f.execEntry(&e)
	return nil
}

// TagKeySeriesIterator returns a series iterator for a tag key.
func (f *LogFile) TagKeySeriesIterator(name, key []byte) SeriesIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}

	tk, ok := mm.tagSet[string(key)]
	if !ok {
		return nil
	}

	// Combine iterators across all tag keys.
	itrs := make([]SeriesIterator, 0, len(tk.tagValues))
	for _, tv := range tk.tagValues {
		if len(tv.series) == 0 {
			continue
		}
		itrs = append(itrs, newLogSeriesIterator(tv.series))
	}

	return MergeSeriesIterators(itrs...)
}

// TagKeyIterator returns a value iterator for a measurement.
func (f *LogFile) TagKeyIterator(name []byte) TagKeyIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}

	a := make([]logTagKey, 0, len(mm.tagSet))
	for _, k := range mm.tagSet {
		a = append(a, k)
	}
	return newLogTagKeyIterator(a)
}

// TagValue returns a tag value element.
func (f *LogFile) TagValue(name, key, value []byte) TagValueElem {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}

	tk, ok := mm.tagSet[string(key)]
	if !ok {
		return nil
	}

	tv, ok := tk.tagValues[string(value)]
	if !ok {
		return nil
	}

	return &tv
}

// TagValueIterator returns a value iterator for a tag key.
func (f *LogFile) TagValueIterator(name, key []byte) TagValueIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}

	tk, ok := mm.tagSet[string(key)]
	if !ok {
		return nil
	}
	return tk.TagValueIterator()
}

// DeleteTagKey adds a tombstone for a tag key to the log file.
func (f *LogFile) DeleteTagKey(name, key []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	e := LogEntry{Flag: LogEntryTagKeyTombstoneFlag, Name: name, Tags: models.Tags{{Key: key}}}
	if err := f.appendEntry(&e); err != nil {
		return err
	}
	f.execEntry(&e)
	return nil
}

// TagValueSeriesIterator returns a series iterator for a tag value.
func (f *LogFile) TagValueSeriesIterator(name, key, value []byte) SeriesIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}

	tk, ok := mm.tagSet[string(key)]
	if !ok {
		return nil
	}

	tv, ok := tk.tagValues[string(value)]
	if !ok {
		return nil
	} else if len(tv.series) == 0 {
		return nil
	}

	return newLogSeriesIterator(tv.series)
}

// DeleteTagValue adds a tombstone for a tag value to the log file.
func (f *LogFile) DeleteTagValue(name, key, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	e := LogEntry{Flag: LogEntryTagValueTombstoneFlag, Name: name, Tags: models.Tags{{Key: key, Value: value}}}
	if err := f.appendEntry(&e); err != nil {
		return err
	}
	f.execEntry(&e)
	return nil
}

// AddSeriesList adds a list of series to the log file in bulk.
func (f *LogFile) AddSeriesList(names [][]byte, tagsSlice []models.Tags) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i := range names {
		e := LogEntry{Name: names[i], Tags: tagsSlice[i]}
		if err := f.appendEntry(&e); err != nil {
			return err
		}
		f.execEntry(&e)
	}
	return nil
}

// AddSeries adds a series to the log file.
func (f *LogFile) AddSeries(name []byte, tags models.Tags) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	e := LogEntry{Name: name, Tags: tags}
	if err := f.appendEntry(&e); err != nil {
		return err
	}
	f.execEntry(&e)
	return nil
}

// DeleteSeries adds a tombstone for a series to the log file.
func (f *LogFile) DeleteSeries(name []byte, tags models.Tags) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	e := LogEntry{Flag: LogEntrySeriesTombstoneFlag, Name: name, Tags: tags}
	if err := f.appendEntry(&e); err != nil {
		return err
	}
	f.execEntry(&e)
	return nil
}

// SeriesN returns the total number of series in the file.
func (f *LogFile) SeriesN() (n uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for _, mm := range f.mms {
		n += uint64(len(mm.series))
	}
	return n
}

// HasSeries returns flags indicating if the series exists and if it is tombstoned.
func (f *LogFile) HasSeries(name []byte, tags models.Tags) (exists, tombstoned bool) {
	e := f.Series(name, tags)
	if e == nil {
		return false, false
	}
	return true, e.Deleted()
}

// Series returns a series by name/tags.
func (f *LogFile) Series(name []byte, tags models.Tags) SeriesElem {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}

	serie := mm.series[string(AppendSeriesKey(make([]byte, 0, 256), name, tags))]
	if serie == nil {
		return nil
	}
	return serie
}

// appendEntry adds a log entry to the end of the file.
func (f *LogFile) appendEntry(e *LogEntry) error {
	// Marshal entry to the local buffer.
	f.buf = appendLogEntry(f.buf[:0], e)

	// Save the size of the record.
	e.Size = len(f.buf)

	// Write record to file.
	n, err := f.w.Write(f.buf)
	if err != nil {
		// Move position backwards over partial entry.
		// Log should be reopened if seeking cannot be completed.
		if n > 0 {
			f.w.Reset(f.file)
			if _, err := f.file.Seek(int64(-n), os.SEEK_CUR); err != nil {
				f.Close()
			}
		}
		return err
	}

	// Update in-memory file size & modification time.
	f.size += int64(n)
	f.modTime = time.Now()

	return nil
}

// execEntry executes a log entry against the in-memory index.
// This is done after appending and on replay of the log.
func (f *LogFile) execEntry(e *LogEntry) {
	switch e.Flag {
	case LogEntryMeasurementTombstoneFlag:
		f.execDeleteMeasurementEntry(e)
	case LogEntryTagKeyTombstoneFlag:
		f.execDeleteTagKeyEntry(e)
	case LogEntryTagValueTombstoneFlag:
		f.execDeleteTagValueEntry(e)
	default:
		f.execSeriesEntry(e)
	}
}

func (f *LogFile) execDeleteMeasurementEntry(e *LogEntry) {
	mm := f.measurement(e.Name)
	mm.deleted = true
	mm.tagSet = make(map[string]logTagKey)
	mm.series = make(map[string]*logSerie)

	// Update measurement tombstone sketch.
	f.mTSketch.Add(e.Name)
}

func (f *LogFile) execDeleteTagKeyEntry(e *LogEntry) {
	key := e.Tags[0].Key

	mm := f.measurement(e.Name)
	ts := mm.createTagSetIfNotExists(key)

	ts.deleted = true

	mm.tagSet[string(key)] = ts
}

func (f *LogFile) execDeleteTagValueEntry(e *LogEntry) {
	key, value := e.Tags[0].Key, e.Tags[0].Value

	mm := f.measurement(e.Name)
	ts := mm.createTagSetIfNotExists(key)
	tv := ts.createTagValueIfNotExists(value)

	tv.deleted = true

	ts.tagValues[string(value)] = tv
	mm.tagSet[string(key)] = ts
}

func (f *LogFile) execSeriesEntry(e *LogEntry) {
	// Check if series is deleted.
	deleted := (e.Flag & LogEntrySeriesTombstoneFlag) != 0

	// Fetch measurement.
	mm := f.measurement(e.Name)

	// Undelete measurement if it's been tombstoned previously.
	if !deleted && mm.deleted {
		mm.deleted = false
	}

	// Generate key & series, if not exists.
	key := AppendSeriesKey(make([]byte, 0, 256), e.Name, e.Tags)
	serie := mm.series[string(key)]
	if serie == nil {
		serie = &logSerie{name: e.Name, tags: e.Tags, deleted: deleted}
		mm.series[string(key)] = serie
	} else if deleted {
		serie.deleted = true
		mm.series[string(key)] = serie
	}

	// Save tags.
	for _, t := range e.Tags {
		ts := mm.createTagSetIfNotExists(t.Key)
		tv := ts.createTagValueIfNotExists(t.Value)

		tv.series[string(key)] = serie

		ts.tagValues[string(t.Value)] = tv
		mm.tagSet[string(t.Key)] = ts
	}

	// Update the sketches...
	if deleted {
		// TODO(edd) decrement series count...
		f.sTSketch.Add(key) // Deleting series so update tombstone sketch.
		return
	}

	// TODO(edd) increment series count....
	f.sSketch.Add(key)    // Add series to sketch.
	f.mSketch.Add(e.Name) // Add measurement to sketch as this may be the fist series for the measurement.
}

// SeriesIterator returns an iterator over all series in the log file.
func (f *LogFile) SeriesIterator() SeriesIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Sort measurement names determine total series count.
	var n int
	names := make([][]byte, 0, len(f.mms))
	for _, mm := range f.mms {
		names = append(names, mm.name)
		n += len(mm.series)
	}
	sort.Sort(byteSlices(names))

	// Combine series across all measurements.
	series := make(logSeries, 0, n)
	for _, name := range names {
		for _, serie := range f.mms[string(name)].series {
			series = append(series, *serie)
		}
	}
	sort.Sort(series)

	if len(series) == 0 {
		return nil
	}
	return &logSeriesIterator{series: series}
}

// measurement returns a measurement by name.
func (f *LogFile) measurement(name []byte) *logMeasurement {
	mm := f.mms[string(name)]
	if mm == nil {
		mm = &logMeasurement{
			name:   name,
			tagSet: make(map[string]logTagKey),
			series: make(map[string]*logSerie),
		}
		f.mms[string(name)] = mm
	}
	if mm.series == nil {
		panic("NO SERIES? " + string(mm.name))
	}
	return mm
}

// MeasurementIterator returns an iterator over all the measurements in the file.
func (f *LogFile) MeasurementIterator() MeasurementIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var itr logMeasurementIterator
	for _, mm := range f.mms {
		itr.mms = append(itr.mms, *mm)
	}
	sort.Sort(logMeasurementSlice(itr.mms))
	return &itr
}

// MeasurementSeriesIterator returns an iterator over all series for a measurement.
func (f *LogFile) MeasurementSeriesIterator(name []byte) SeriesIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm := f.mms[string(name)]
	if mm == nil || len(mm.series) == 0 {
		return nil
	}
	return newLogSeriesIterator(mm.series)
}

// WriteTo compacts the log file and writes it to w.
func (f *LogFile) WriteTo(w io.Writer) (n int64, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Wrap in bufferred writer.
	bw := bufio.NewWriter(w)

	// Reset compaction fields.
	var t IndexFileTrailer
	f.reset()

	// Write magic number.
	if err := writeTo(bw, []byte(FileSignature), &n); err != nil {
		return n, err
	}

	// Write series list.
	t.SeriesBlock.Offset = n
	if err := f.writeSeriesBlockTo(bw, &n); err != nil {
		return n, err
	}
	t.SeriesBlock.Size = n - t.SeriesBlock.Offset

	// Sort measurement names.
	names := f.mms.names()

	// Write tagset blocks in measurement order.
	if err := f.writeTagsetsTo(bw, names, &n); err != nil {
		return n, err
	}

	// Write measurement block.
	t.MeasurementBlock.Offset = n
	if err := f.writeMeasurementBlockTo(bw, names, &n); err != nil {
		return n, err
	}
	t.MeasurementBlock.Size = n - t.MeasurementBlock.Offset

	// Write trailer.
	nn, err := t.WriteTo(bw)
	n += nn
	if err != nil {
		return n, err
	}

	// Flush buffer.
	if err := bw.Flush(); err != nil {
		return n, err
	}

	return n, nil
}

func (f *LogFile) writeSeriesBlockTo(w io.Writer, n *int64) error {
	// Write all series.
	sw := NewSeriesBlockWriter()

	// Retreve measurement names in order.
	names := f.measurementNames()

	// Add series from measurements.
	for _, name := range names {
		mm := f.mms[name]
		for _, serie := range mm.series {
			if err := sw.Add(serie.name, serie.tags); err != nil {
				return err
			}
		}
	}

	// As the log file is created it's possible that series were added, removed
	// and then added again. Since sketches cannot have values removed from them
	// the series would be in both the series and tombstoned series sketches. So
	// that a series only appears in one of the sketches we rebuild some fresh
	// sketches for the compaction to a TSI file.
	//
	// We update these sketches below as we iterate through the series in this
	// log file.
	sw.Sketch, sw.TSketch = hll.NewDefaultPlus(), hll.NewDefaultPlus()

	// Flush series list.
	nn, err := sw.WriteTo(w)
	*n += nn
	if err != nil {
		return err
	}

	// Add series to each measurement and key/value.
	buf := make([]byte, 0, 1024)
	for _, name := range names {
		mm := f.mms[name]

		for i := range mm.series {
			serie := mm.series[i]

			// Lookup series offset.
			serie.offset = sw.Offset(serie.name, serie.tags)
			if serie.offset == 0 {
				panic("series not found: " + string(serie.name) + " " + serie.tags.String())
			}

			// Add series id to measurement, tag key, and tag value.
			mm.seriesIDs = append(mm.seriesIDs, serie.offset)

			// Add series id to each tag value.
			for _, tag := range serie.tags {
				t := mm.tagSet[string(tag.Key)]

				v := t.tagValues[string(tag.Value)]
				v.seriesIDs = append(v.seriesIDs, serie.offset)
				t.tagValues[string(tag.Value)] = v
			}

			key := AppendSeriesKey(buf[:0], serie.name, serie.tags)
			if serie.Deleted() {
				sw.TSketch.Add(key)
			} else {
				sw.Sketch.Add(key)
			}
		}
	}

	// Set log file sketches to updated versions.
	f.sSketch, f.sTSketch = sw.Sketch, sw.TSketch
	return nil
}

func (f *LogFile) writeTagsetsTo(w io.Writer, names []string, n *int64) error {
	for _, name := range names {
		if err := f.writeTagsetTo(w, name, n); err != nil {
			return err
		}
	}
	return nil
}

// writeTagsetTo writes a single tagset to w and saves the tagset offset.
func (f *LogFile) writeTagsetTo(w io.Writer, name string, n *int64) error {
	mm := f.mms[name]

	tw := NewTagBlockWriter()
	for _, tag := range mm.tagSet {
		// Mark tag deleted.
		if tag.deleted {
			tw.DeleteTag(tag.name)
			continue
		}

		// Add each value.
		for _, value := range tag.tagValues {
			sort.Sort(uint32Slice(value.seriesIDs))
			tw.AddTagValue(tag.name, value.name, value.deleted, value.seriesIDs)
		}
	}

	// Save tagset offset to measurement.
	mm.offset = *n

	// Write tagset to writer.
	nn, err := tw.WriteTo(w)
	*n += nn
	if err != nil {
		return err
	}

	// Save tagset offset to measurement.
	mm.size = *n - mm.offset

	return nil
}

func (f *LogFile) writeMeasurementBlockTo(w io.Writer, names []string, n *int64) error {
	mw := NewMeasurementBlockWriter()

	// As the log file is created it's possible that measurements were added,
	// removed and then added again. Since sketches cannot have values removed
	// from them, the measurement would be in both the measurement and
	// tombstoned measurement sketches. So that a measurement only appears in
	// one of the sketches, we rebuild some fresh sketches for the compaction to
	// a TSI file.
	//
	// We update these sketches below as we iterate through the measurements in
	// this log file.
	mw.Sketch, mw.TSketch = hll.NewDefaultPlus(), hll.NewDefaultPlus()

	// Add measurement data.
	for _, name := range names {
		mm := f.mms[name]

		sort.Sort(uint32Slice(mm.seriesIDs))
		mw.Add(mm.name, mm.offset, mm.size, mm.seriesIDs)
		if mm.Deleted() {
			mw.TSketch.Add(mm.Name())
		} else {
			mw.Sketch.Add(mm.Name())
		}
	}

	// Write data to writer.
	nn, err := mw.WriteTo(w)
	*n += nn
	if err != nil {
		return err
	}

	// Set the updated sketches
	f.mSketch, f.mTSketch = mw.Sketch, mw.TSketch
	return nil
}

// reset clears all the compaction fields on the in-memory index.
func (f *LogFile) reset() {
	for _, mm := range f.mms {
		for i := range mm.series {
			mm.series[i].offset = 0
		}

		mm.offset, mm.size, mm.seriesIDs = 0, 0, nil
		for key, tagSet := range mm.tagSet {
			for value, tagValue := range tagSet.tagValues {
				tagValue.seriesIDs = nil
				tagSet.tagValues[value] = tagValue
			}
			mm.tagSet[key] = tagSet
		}
	}
}

// MergeSeriesSketches merges the series sketches within a mutex.
func (f *LogFile) MergeSeriesSketches(sketch, tsketch estimator.Sketch) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if err := sketch.Merge(f.sSketch); err != nil {
		return err
	}
	if err := tsketch.Merge(f.sTSketch); err != nil {
		return err
	}
	return nil
}

// LogEntry represents a single log entry in the write-ahead log.
type LogEntry struct {
	Flag     byte        // flag
	Name     []byte      // measurement name
	Tags     models.Tags // tagset
	Checksum uint32      // checksum of flag/name/tags.
	Size     int         // total size of record, in bytes.
}

// UnmarshalBinary unmarshals data into e.
func (e *LogEntry) UnmarshalBinary(data []byte) error {
	orig := data
	start := len(data)

	// Parse flag data.
	e.Flag, data = data[0], data[1:]

	// Parse name.
	sz, n := binary.Uvarint(data)
	e.Name, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse tag count.
	tagN, n := binary.Uvarint(data)
	data = data[n:]

	// Parse tags.
	tags := make(models.Tags, tagN)
	for i := range tags {
		tag := &tags[i]

		// Parse key.
		sz, n := binary.Uvarint(data)
		tag.Key, data = data[n:n+int(sz)], data[n+int(sz):]

		// Parse value.
		sz, n = binary.Uvarint(data)
		tag.Value, data = data[n:n+int(sz)], data[n+int(sz):]
	}
	e.Tags = tags

	// Compute checksum.
	chk := crc32.ChecksumIEEE(orig[:start-len(data)])

	// Parse checksum.
	e.Checksum, data = binary.BigEndian.Uint32(data[:4]), data[4:]

	// Verify checksum.
	if chk != e.Checksum {
		return ErrLogEntryChecksumMismatch
	}

	// Save length of elem.
	e.Size = start - len(data)

	return nil
}

// appendLogEntry appends to dst and returns the new buffer.
// This updates the checksum on the entry.
func appendLogEntry(dst []byte, e *LogEntry) []byte {
	var buf [binary.MaxVarintLen64]byte
	start := len(dst)

	// Append flag.
	dst = append(dst, e.Flag)

	// Append name.
	n := binary.PutUvarint(buf[:], uint64(len(e.Name)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, e.Name...)

	// Append tag count.
	n = binary.PutUvarint(buf[:], uint64(len(e.Tags)))
	dst = append(dst, buf[:n]...)

	// Append key/value pairs.
	for i := range e.Tags {
		t := &e.Tags[i]

		// Append key.
		n := binary.PutUvarint(buf[:], uint64(len(t.Key)))
		dst = append(dst, buf[:n]...)
		dst = append(dst, t.Key...)

		// Append value.
		n = binary.PutUvarint(buf[:], uint64(len(t.Value)))
		dst = append(dst, buf[:n]...)
		dst = append(dst, t.Value...)
	}

	// Calculate checksum.
	e.Checksum = crc32.ChecksumIEEE(dst[start:])

	// Append checksum.
	binary.BigEndian.PutUint32(buf[:4], e.Checksum)
	dst = append(dst, buf[:4]...)

	return dst
}

type logSerie struct {
	name    []byte
	tags    models.Tags
	deleted bool
	offset  uint32
}

func (s *logSerie) Name() []byte        { return s.name }
func (s *logSerie) Tags() models.Tags   { return s.tags }
func (s *logSerie) Deleted() bool       { return s.deleted }
func (s *logSerie) Expr() influxql.Expr { return nil }

type logSeries []logSerie

func (a logSeries) Len() int      { return len(a) }
func (a logSeries) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a logSeries) Less(i, j int) bool {
	if cmp := bytes.Compare(a[i].name, a[j].name); cmp != 0 {
		return cmp == -1
	}
	return models.CompareTags(a[i].tags, a[j].tags) == -1
}

// logMeasurements represents a map of measurement names to measurements.
type logMeasurements map[string]*logMeasurement

// names returns a sorted list of measurement names.
func (m logMeasurements) names() []string {
	a := make([]string, 0, len(m))
	for name := range m {
		a = append(a, name)
	}
	sort.Strings(a)
	return a
}

type logMeasurement struct {
	name    []byte
	tagSet  map[string]logTagKey
	deleted bool
	series  map[string]*logSerie

	// Compaction fields.
	offset    int64    // tagset offset
	size      int64    // tagset size
	seriesIDs []uint32 // series offsets
}

func (m *logMeasurement) Name() []byte  { return m.name }
func (m *logMeasurement) Deleted() bool { return m.deleted }

func (m *logMeasurement) createTagSetIfNotExists(key []byte) logTagKey {
	ts, ok := m.tagSet[string(key)]
	if !ok {
		ts = logTagKey{name: key, tagValues: make(map[string]logTagValue)}
	}
	return ts
}

// logMeasurementSlice is a sortable list of log measurements.
type logMeasurementSlice []logMeasurement

func (a logMeasurementSlice) Len() int           { return len(a) }
func (a logMeasurementSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a logMeasurementSlice) Less(i, j int) bool { return bytes.Compare(a[i].name, a[j].name) == -1 }

// logMeasurementIterator represents an iterator over a slice of measurements.
type logMeasurementIterator struct {
	mms []logMeasurement
}

// Next returns the next element in the iterator.
func (itr *logMeasurementIterator) Next() (e MeasurementElem) {
	if len(itr.mms) == 0 {
		return nil
	}
	e, itr.mms = &itr.mms[0], itr.mms[1:]
	return e
}

type logTagKey struct {
	name      []byte
	deleted   bool
	tagValues map[string]logTagValue
}

func (tk *logTagKey) Key() []byte   { return tk.name }
func (tk *logTagKey) Deleted() bool { return tk.deleted }

func (tk *logTagKey) TagValueIterator() TagValueIterator {
	a := make([]logTagValue, 0, len(tk.tagValues))
	for _, v := range tk.tagValues {
		a = append(a, v)
	}
	return newLogTagValueIterator(a)
}

func (tk *logTagKey) createTagValueIfNotExists(value []byte) logTagValue {
	tv, ok := tk.tagValues[string(value)]
	if !ok {
		tv = logTagValue{name: value, series: make(map[string]*logSerie)}
	}
	return tv
}

// logTagKey is a sortable list of log tag keys.
type logTagKeySlice []logTagKey

func (a logTagKeySlice) Len() int           { return len(a) }
func (a logTagKeySlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a logTagKeySlice) Less(i, j int) bool { return bytes.Compare(a[i].name, a[j].name) == -1 }

type logTagValue struct {
	name    []byte
	deleted bool
	series  map[string]*logSerie

	// Compaction fields.
	seriesIDs []uint32
}

func (tv *logTagValue) Value() []byte { return tv.name }
func (tv *logTagValue) Deleted() bool { return tv.deleted }

// logTagValue is a sortable list of log tag values.
type logTagValueSlice []logTagValue

func (a logTagValueSlice) Len() int           { return len(a) }
func (a logTagValueSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a logTagValueSlice) Less(i, j int) bool { return bytes.Compare(a[i].name, a[j].name) == -1 }

// logTagKeyIterator represents an iterator over a slice of tag keys.
type logTagKeyIterator struct {
	a []logTagKey
}

// newLogTagKeyIterator returns a new instance of logTagKeyIterator.
func newLogTagKeyIterator(a []logTagKey) *logTagKeyIterator {
	sort.Sort(logTagKeySlice(a))
	return &logTagKeyIterator{a: a}
}

// Next returns the next element in the iterator.
func (itr *logTagKeyIterator) Next() (e TagKeyElem) {
	if len(itr.a) == 0 {
		return nil
	}
	e, itr.a = &itr.a[0], itr.a[1:]
	return e
}

// logTagValueIterator represents an iterator over a slice of tag values.
type logTagValueIterator struct {
	a []logTagValue
}

// newLogTagValueIterator returns a new instance of logTagValueIterator.
func newLogTagValueIterator(a []logTagValue) *logTagValueIterator {
	sort.Sort(logTagValueSlice(a))
	return &logTagValueIterator{a: a}
}

// Next returns the next element in the iterator.
func (itr *logTagValueIterator) Next() (e TagValueElem) {
	if len(itr.a) == 0 {
		return nil
	}
	e, itr.a = &itr.a[0], itr.a[1:]
	return e
}

// logSeriesIterator represents an iterator over a slice of series.
type logSeriesIterator struct {
	series logSeries
}

// newLogSeriesIterator returns a new instance of logSeriesIterator.
// All series are copied to the iterator.
func newLogSeriesIterator(m map[string]*logSerie) *logSeriesIterator {
	if len(m) == 0 {
		return nil
	}

	itr := logSeriesIterator{series: make(logSeries, 0, len(m))}
	for _, serie := range m {
		itr.series = append(itr.series, *serie)
	}
	sort.Sort(itr.series)
	return &itr
}

// Next returns the next element in the iterator.
func (itr *logSeriesIterator) Next() (e SeriesElem) {
	if len(itr.series) == 0 {
		return nil
	}
	e, itr.series = &itr.series[0], itr.series[1:]
	return e
}

// FormatLogFileName generates a log filename for the given index.
func FormatLogFileName(i int) string {
	return fmt.Sprintf("%08d%s", i, LogFileExt)
}
