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
	"github.com/influxdata/influxdb/tsdb"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/mmap"
	"github.com/influxdata/influxql"
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
	id   int            // file sequence identifier
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
	path string
}

// NewLogFile returns a new instance of LogFile.
func NewLogFile(path string) *LogFile {
	return &LogFile{
		path:     path,
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
	f.id, _ = ParseFilename(f.path)

	// Open file for appending.
	file, err := os.OpenFile(f.Path(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
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
	data, err := mmap.Map(f.Path())
	if err != nil {
		return err
	}
	f.data = data

	// Read log entries from mmap.
	var n int64
	for buf := f.data; len(buf) > 0; {
		// Read next entry. Truncate partial writes.
		var e LogEntry
		if err := e.UnmarshalBinary(buf); err == io.ErrShortBuffer {
			if err := file.Truncate(n); err != nil {
				return err
			} else if _, err := file.Seek(0, io.SeekEnd); err != nil {
				return err
			}
			break
		} else if err != nil {
			return err
		}

		// Execute entry against in-memory index.
		f.execEntry(&e)

		// Move buffer forward.
		n += int64(e.Size)
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

// Flush flushes buffered data to disk.
func (f *LogFile) Flush() error {
	if f.w != nil {
		return f.w.Flush()
	}
	return nil
}

// ID returns the file sequence identifier.
func (f *LogFile) ID() int { return f.id }

// Path returns the file path.
func (f *LogFile) Path() string { return f.path }

// SetPath sets the log file's path.
func (f *LogFile) SetPath(path string) { f.path = path }

// Level returns the log level of the file.
func (f *LogFile) Level() int { return 0 }

// Filter returns the bloom filter for the file.
func (f *LogFile) Filter() *bloom.Filter { return nil }

// Retain adds a reference count to the file.
func (f *LogFile) Retain() { f.wg.Add(1) }

// Release removes a reference count from the file.
func (f *LogFile) Release() { f.wg.Done() }

// Stat returns size and last modification time of the file.
func (f *LogFile) Stat() (int64, time.Time) {
	f.mu.RLock()
	size, modTime := f.size, f.modTime
	f.mu.RUnlock()
	return size, modTime
}

// Size returns the size of the file, in bytes.
func (f *LogFile) Size() int64 {
	f.mu.RLock()
	v := f.size
	f.mu.RUnlock()
	return v
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
	sort.Strings(a)
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
func (f *LogFile) TagKeySeriesIterator(name, key []byte) tsdb.SeriesIterator {
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
	itrs := make([]tsdb.SeriesIterator, 0, len(tk.tagValues))
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

// TagKey returns a tag key element.
func (f *LogFile) TagKey(name, key []byte) TagKeyElem {
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

	return &tk
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
func (f *LogFile) TagValueSeriesIterator(name, key, value []byte) tsdb.SeriesIterator {
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

// MeasurementN returns the total number of measurements.
func (f *LogFile) MeasurementN() (n uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return uint64(len(f.mms))
}

// TagKeyN returns the total number of keys.
func (f *LogFile) TagKeyN() (n uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, mm := range f.mms {
		n += uint64(len(mm.tagSet))
	}
	return n
}

// TagValueN returns the total number of values.
func (f *LogFile) TagValueN() (n uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, mm := range f.mms {
		for _, k := range mm.tagSet {
			n += uint64(len(k.tagValues))
		}
	}
	return n
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
	// Determine total size of names, keys, values.
	var n int
	for i := range names {
		n += len(names[i])

		tags := tagsSlice[i]
		for j := range tags {
			n += len(tags[j].Key) + len(tags[j].Value)
		}
	}

	// Allocate names, keys, & values in one block.
	buf := make([]byte, n)

	// Clone all entries.
	entries := make([]LogEntry, len(names))
	for i := range names {
		copy(buf, names[i])
		clonedName := buf[:len(names[i])]
		buf = buf[len(names[i]):]

		// Clone tag set.
		var clonedTags models.Tags
		if len(tagsSlice[i]) > 0 {
			clonedTags = make(models.Tags, len(tagsSlice[i]))
			for j, tags := range tagsSlice[i] {
				copy(buf, tags.Key)
				key := buf[:len(tags.Key)]
				buf = buf[len(tags.Key):]

				copy(buf, tags.Value)
				value := buf[:len(tags.Value)]
				buf = buf[len(tags.Value):]

				clonedTags[j] = models.Tag{Key: key, Value: value}
			}
		}

		entries[i] = LogEntry{Name: clonedName, Tags: clonedTags}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	for i := range entries {
		if err := f.appendEntry(&entries[i]); err != nil {
			return err
		}
		f.execEntry(&entries[i])
	}
	return nil
}

// AddSeries adds a series to the log file.
func (f *LogFile) AddSeries(name []byte, tags models.Tags) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// The name and tags are clone to prevent a memory leak
	newName := make([]byte, len(name))
	copy(newName, name)

	e := LogEntry{Name: newName, Tags: tags.Clone()}
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
func (f *LogFile) HasSeries(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool) {
	e := f.SeriesWithBuffer(name, tags, buf)
	if e == nil {
		return false, false
	}
	return true, e.Deleted()
}

// FilterNamesTags filters out any series which already exist. It modifies the
// provided slices of names and tags.
func (f *LogFile) FilterNamesTags(names [][]byte, tagsSlice []models.Tags) ([][]byte, []models.Tags) {
	buf := make([]byte, 1024)
	f.mu.RLock()
	defer f.mu.RUnlock()

	newNames, newTagsSlice := names[:0], tagsSlice[:0]
	for i := 0; i < len(names); i++ {
		name := names[i]
		tags := tagsSlice[i]

		mm := f.mms[string(name)]
		if mm == nil {
			newNames = append(newNames, name)
			newTagsSlice = append(newTagsSlice, tags)
			continue
		}

		key := AppendSeriesKey(buf[:0], name, tags)
		s := mm.series[string(key)]
		if s == nil || s.Deleted() {
			newNames = append(newNames, name)
			newTagsSlice = append(newTagsSlice, tags)
		}
	}
	return newNames, newTagsSlice
}

// Series returns a series by name/tags.
func (f *LogFile) Series(name []byte, tags models.Tags) tsdb.SeriesElem {
	return f.SeriesWithBuffer(name, tags, nil)
}

// SeriesWithBuffer returns a series by name/tags.
func (f *LogFile) SeriesWithBuffer(name []byte, tags models.Tags, buf []byte) tsdb.SeriesElem {
	key := AppendSeriesKey(buf[:0], name, tags)

	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return nil
	}

	s := mm.series[string(key)]
	if s == nil {
		return nil
	}
	return s
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
	mm := f.createMeasurementIfNotExists(e.Name)
	mm.deleted = true
	mm.tagSet = make(map[string]logTagKey)
	mm.series = make(map[string]*logSerie)

	// Update measurement tombstone sketch.
	f.mTSketch.Add(e.Name)
}

func (f *LogFile) execDeleteTagKeyEntry(e *LogEntry) {
	key := e.Tags[0].Key

	mm := f.createMeasurementIfNotExists(e.Name)
	ts := mm.createTagSetIfNotExists(key)

	ts.deleted = true

	mm.tagSet[string(key)] = ts
}

func (f *LogFile) execDeleteTagValueEntry(e *LogEntry) {
	key, value := e.Tags[0].Key, e.Tags[0].Value

	mm := f.createMeasurementIfNotExists(e.Name)
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
	mm := f.createMeasurementIfNotExists(e.Name)

	// Undelete measurement if it's been tombstoned previously.
	if !deleted && mm.deleted {
		mm.deleted = false
	}

	// Generate key & series, if not exists.
	key := AppendSeriesKey(nil, e.Name, e.Tags)
	serie := mm.createSeriesIfNotExists(key, e.Name, e.Tags, deleted)

	// Save tags.
	for _, t := range e.Tags {
		ts := mm.createTagSetIfNotExists(t.Key)
		tv := ts.createTagValueIfNotExists(t.Value)

		// Add a reference to the series on the tag value.
		tv.series[string(key)] = serie

		ts.tagValues[string(t.Value)] = tv
		mm.tagSet[string(t.Key)] = ts
	}

	// Update the sketches.
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
func (f *LogFile) SeriesIterator() tsdb.SeriesIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Determine total series count across all measurements.
	var n int
	mSeriesIdx := make([]int, len(f.mms))
	mSeries := make([][]logSerie, 0, len(f.mms))
	for _, mm := range f.mms {
		n += len(mm.series)
		a := make([]logSerie, 0, len(mm.series))
		for _, s := range mm.series {
			a = append(a, *s)
		}
		sort.Sort(logSeries(a))
		mSeries = append(mSeries, a)
	}

	// Combine series across all measurements by merging the already sorted
	// series lists.
	sBuffer := make([]*logSerie, len(f.mms))
	series := make(logSeries, 0, n)
	var (
		minSerie    *logSerie
		minSerieIdx int
	)

	for s := 0; s < cap(series); s++ {
		for i := 0; i < len(sBuffer); i++ {
			// Are there still serie to pull from this measurement?
			if mSeriesIdx[i] < len(mSeries[i]) && sBuffer[i] == nil {
				// Fill the buffer slot for this measurement.
				sBuffer[i] = &mSeries[i][mSeriesIdx[i]]
				mSeriesIdx[i]++
			}

			// Does this measurement have the smallest current serie out of
			// all those in the buffer?
			if minSerie == nil || (sBuffer[i] != nil && sBuffer[i].Compare(minSerie.name, minSerie.tags) < 0) {
				minSerie, minSerieIdx = sBuffer[i], i
			}
		}
		series, minSerie, sBuffer[minSerieIdx] = append(series, *minSerie), nil, nil
	}

	if len(series) == 0 {
		return nil
	}
	return &logSeriesIterator{series: series}
}

// createMeasurementIfNotExists returns a measurement by name.
func (f *LogFile) createMeasurementIfNotExists(name []byte) *logMeasurement {
	mm := f.mms[string(name)]
	if mm == nil {
		mm = &logMeasurement{
			name:   name,
			tagSet: make(map[string]logTagKey),
			series: make(map[string]*logSerie),
		}
		f.mms[string(name)] = mm
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
func (f *LogFile) MeasurementSeriesIterator(name []byte) tsdb.SeriesIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm := f.mms[string(name)]
	if mm == nil || len(mm.series) == 0 {
		return nil
	}
	return newLogSeriesIterator(mm.series)
}

// CompactTo compacts the log file and writes it to w.
func (f *LogFile) CompactTo(w io.Writer, m, k uint64) (n int64, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Wrap in bufferred writer.
	bw := bufio.NewWriter(w)

	// Setup compaction offset tracking data.
	var t IndexFileTrailer
	info := newLogFileCompactInfo()

	// Write magic number.
	if err := writeTo(bw, []byte(FileSignature), &n); err != nil {
		return n, err
	}

	// Retreve measurement names in order.
	names := f.measurementNames()

	// Write series list.
	t.SeriesBlock.Offset = n
	if err := f.writeSeriesBlockTo(bw, names, m, k, info, &n); err != nil {
		return n, err
	}
	t.SeriesBlock.Size = n - t.SeriesBlock.Offset

	// Flush buffer & mmap series block.
	if err := bw.Flush(); err != nil {
		return n, err
	}

	// Update series offsets.
	// NOTE: Pass the raw writer so we can mmap.
	if err := f.updateSeriesOffsets(w, names, info); err != nil {
		return n, err
	}

	// Write tagset blocks in measurement order.
	if err := f.writeTagsetsTo(bw, names, info, &n); err != nil {
		return n, err
	}

	// Write measurement block.
	t.MeasurementBlock.Offset = n
	if err := f.writeMeasurementBlockTo(bw, names, info, &n); err != nil {
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

func (f *LogFile) writeSeriesBlockTo(w io.Writer, names []string, m, k uint64, info *logFileCompactInfo, n *int64) error {
	// Determine series count.
	var seriesN uint32
	for _, mm := range f.mms {
		seriesN += uint32(len(mm.series))
	}

	// Write all series.
	enc := NewSeriesBlockEncoder(w, seriesN, m, k)

	// Add series from measurements.
	for _, name := range names {
		mm := f.mms[name]

		// Sort series.
		keys := make([][]byte, 0, len(mm.series))
		for k := range mm.series {
			keys = append(keys, []byte(k))
		}
		sort.Sort(seriesKeys(keys))

		for _, key := range keys {
			serie := mm.series[string(key)]
			if err := enc.Encode(serie.name, serie.tags, serie.deleted); err != nil {
				return err
			}
		}
	}

	// Close and flush series block.
	err := enc.Close()
	*n += int64(enc.N())
	if err != nil {
		return err
	}

	return nil
}

func (f *LogFile) updateSeriesOffsets(w io.Writer, names []string, info *logFileCompactInfo) error {
	// Open series block.
	sblk, data, err := mapIndexFileSeriesBlock(w)
	if data != nil {
		defer mmap.Unmap(data)
	}
	if err != nil {
		return err
	}

	// Add series to each measurement and key/value.
	var seriesKey []byte
	for _, name := range names {
		mm := f.mms[name]
		mmInfo := info.createMeasurementInfoIfNotExists(name)
		mmInfo.seriesIDs = make([]uint32, 0, len(mm.series))

		for _, serie := range mm.series {
			// Lookup series offset.
			offset, _ := sblk.Offset(serie.name, serie.tags, seriesKey[:0])
			if offset == 0 {
				panic("series not found: " + string(serie.name) + " " + serie.tags.String())
			}

			// Add series id to measurement, tag key, and tag value.
			mmInfo.seriesIDs = append(mmInfo.seriesIDs, offset)

			// Add series id to each tag value.
			for _, tag := range serie.tags {
				tagSetInfo := mmInfo.createTagSetInfoIfNotExists(tag.Key)
				tagValueInfo := tagSetInfo.createTagValueInfoIfNotExists(tag.Value)
				tagValueInfo.seriesIDs = append(tagValueInfo.seriesIDs, offset)
			}
		}
	}

	return nil
}

func (f *LogFile) writeTagsetsTo(w io.Writer, names []string, info *logFileCompactInfo, n *int64) error {
	for _, name := range names {
		if err := f.writeTagsetTo(w, name, info, n); err != nil {
			return err
		}
	}
	return nil
}

// writeTagsetTo writes a single tagset to w and saves the tagset offset.
func (f *LogFile) writeTagsetTo(w io.Writer, name string, info *logFileCompactInfo, n *int64) error {
	mm := f.mms[name]
	mmInfo := info.mms[name]

	enc := NewTagBlockEncoder(w)
	for _, k := range mm.keys() {
		tag := mm.tagSet[k]

		// Encode tag. Skip values if tag is deleted.
		if err := enc.EncodeKey(tag.name, tag.deleted); err != nil {
			return err
		} else if tag.deleted {
			continue
		}

		// Lookup compaction info.
		tagSetInfo := mmInfo.tagSet[k]
		assert(tagSetInfo != nil, "tag set info not found")

		// Sort tag values.
		values := make([]string, 0, len(tag.tagValues))
		for v := range tag.tagValues {
			values = append(values, v)
		}
		sort.Strings(values)

		// Add each value.
		for _, v := range values {
			value := tag.tagValues[v]
			tagValueInfo := tagSetInfo.tagValues[v]
			sort.Sort(uint32Slice(tagValueInfo.seriesIDs))

			if err := enc.EncodeValue(value.name, value.deleted, tagValueInfo.seriesIDs); err != nil {
				return err
			}
		}
	}

	// Save tagset offset to measurement.
	mmInfo.offset = *n

	// Flush tag block.
	err := enc.Close()
	*n += enc.N()
	if err != nil {
		return err
	}

	// Save tagset offset to measurement.
	mmInfo.size = *n - mmInfo.offset

	return nil
}

func (f *LogFile) writeMeasurementBlockTo(w io.Writer, names []string, info *logFileCompactInfo, n *int64) error {
	mw := NewMeasurementBlockWriter()

	// Add measurement data.
	for _, name := range names {
		mm := f.mms[name]
		mmInfo := info.mms[name]
		assert(mmInfo != nil, "measurement info not found")

		sort.Sort(uint32Slice(mmInfo.seriesIDs))
		mw.Add(mm.name, mm.deleted, mmInfo.offset, mmInfo.size, mmInfo.seriesIDs)
	}

	// Flush data to writer.
	nn, err := mw.WriteTo(w)
	*n += nn
	return err
}

// logFileCompactInfo is a context object to track compaction position info.
type logFileCompactInfo struct {
	mms map[string]*logFileMeasurementCompactInfo
}

// newLogFileCompactInfo returns a new instance of logFileCompactInfo.
func newLogFileCompactInfo() *logFileCompactInfo {
	return &logFileCompactInfo{
		mms: make(map[string]*logFileMeasurementCompactInfo),
	}
}

func (info *logFileCompactInfo) createMeasurementInfoIfNotExists(name string) *logFileMeasurementCompactInfo {
	mmInfo := info.mms[name]
	if mmInfo == nil {
		mmInfo = &logFileMeasurementCompactInfo{
			tagSet: make(map[string]*logFileTagSetCompactInfo),
		}
		info.mms[name] = mmInfo
	}
	return mmInfo
}

type logFileMeasurementCompactInfo struct {
	offset    int64
	size      int64
	seriesIDs []uint32

	tagSet map[string]*logFileTagSetCompactInfo
}

func (info *logFileMeasurementCompactInfo) createTagSetInfoIfNotExists(key []byte) *logFileTagSetCompactInfo {
	tagSetInfo := info.tagSet[string(key)]
	if tagSetInfo == nil {
		tagSetInfo = &logFileTagSetCompactInfo{tagValues: make(map[string]*logFileTagValueCompactInfo)}
		info.tagSet[string(key)] = tagSetInfo
	}
	return tagSetInfo
}

type logFileTagSetCompactInfo struct {
	tagValues map[string]*logFileTagValueCompactInfo
}

func (info *logFileTagSetCompactInfo) createTagValueInfoIfNotExists(value []byte) *logFileTagValueCompactInfo {
	tagValueInfo := info.tagValues[string(value)]
	if tagValueInfo == nil {
		tagValueInfo = &logFileTagValueCompactInfo{}
		info.tagValues[string(value)] = tagValueInfo
	}
	return tagValueInfo
}

type logFileTagValueCompactInfo struct {
	seriesIDs []uint32
}

// MergeSeriesSketches merges the series sketches belonging to this LogFile
// into the provided sketches.
//
// MergeSeriesSketches is safe for concurrent use by multiple goroutines.
func (f *LogFile) MergeSeriesSketches(sketch, tsketch estimator.Sketch) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if err := sketch.Merge(f.sSketch); err != nil {
		return err
	}
	return tsketch.Merge(f.sTSketch)
}

// MergeMeasurementsSketches merges the measurement sketches belonging to this
// LogFile into the provided sketches.
//
// MergeMeasurementsSketches is safe for concurrent use by multiple goroutines.
func (f *LogFile) MergeMeasurementsSketches(sketch, tsketch estimator.Sketch) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if err := sketch.Merge(f.mSketch); err != nil {
		return err
	}
	return tsketch.Merge(f.mTSketch)
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
	if len(data) < 1 {
		return io.ErrShortBuffer
	}
	e.Flag, data = data[0], data[1:]

	// Parse name length.
	if len(data) < 1 {
		return io.ErrShortBuffer
	}
	sz, n := binary.Uvarint(data)

	// Read name data.
	if len(data) < n+int(sz) {
		return io.ErrShortBuffer
	}
	e.Name, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse tag count.
	if len(data) < 1 {
		return io.ErrShortBuffer
	}
	tagN, n := binary.Uvarint(data)
	data = data[n:]

	// Parse tags.
	tags := make(models.Tags, tagN)
	for i := range tags {
		tag := &tags[i]

		// Parse key length.
		if len(data) < 1 {
			return io.ErrShortBuffer
		}
		sz, n := binary.Uvarint(data)

		// Read key data.
		if len(data) < n+int(sz) {
			return io.ErrShortBuffer
		}
		tag.Key, data = data[n:n+int(sz)], data[n+int(sz):]

		// Parse value.
		if len(data) < 1 {
			return io.ErrShortBuffer
		}
		sz, n = binary.Uvarint(data)

		// Read value data.
		if len(data) < n+int(sz) {
			return io.ErrShortBuffer
		}
		tag.Value, data = data[n:n+int(sz)], data[n+int(sz):]
	}
	e.Tags = tags

	// Compute checksum.
	chk := crc32.ChecksumIEEE(orig[:start-len(data)])

	// Parse checksum.
	if len(data) < 4 {
		return io.ErrShortBuffer
	}
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
}

func (s *logSerie) String() string {
	return fmt.Sprintf("key: %s tags: %v", s.name, s.tags)
}

func (s *logSerie) Name() []byte        { return s.name }
func (s *logSerie) Tags() models.Tags   { return s.tags }
func (s *logSerie) Deleted() bool       { return s.deleted }
func (s *logSerie) Expr() influxql.Expr { return nil }
func (s *logSerie) Compare(name []byte, tags models.Tags) int {
	if cmp := bytes.Compare(s.name, name); cmp != 0 {
		return cmp
	}
	return models.CompareTags(s.tags, tags)
}

type logSeries []logSerie

func (a logSeries) Len() int      { return len(a) }
func (a logSeries) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a logSeries) Less(i, j int) bool {
	return a[i].Compare(a[j].name, a[j].tags) == -1
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

// createSeriesIfNotExists creates or returns an existing series on the measurement.
func (m *logMeasurement) createSeriesIfNotExists(key []byte, name []byte, tags models.Tags, deleted bool) *logSerie {
	s := m.series[string(key)]
	if s == nil {
		s = &logSerie{name: name, tags: tags, deleted: deleted}
		m.series[string(key)] = s
	} else {
		s.deleted = deleted
	}
	return s
}

// keys returns a sorted list of tag keys.
func (m *logMeasurement) keys() []string {
	a := make([]string, 0, len(m.tagSet))
	for k := range m.tagSet {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
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
	for _, s := range m {
		itr.series = append(itr.series, *s)
	}
	sort.Sort(itr.series)

	return &itr
}

// Next returns the next element in the iterator.
func (itr *logSeriesIterator) Next() (e tsdb.SeriesElem) {
	if len(itr.series) == 0 {
		return nil
	}
	e, itr.series = &itr.series[0], itr.series[1:]
	return e
}

// FormatLogFileName generates a log filename for the given index.
func FormatLogFileName(id int) string {
	return fmt.Sprintf("L0-%08d%s", id, LogFileExt)
}
