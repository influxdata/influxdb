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
	"github.com/influxdata/influxdb/pkg/bloom"
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
	id   int            // file sequence identifier
	data []byte         // mmap
	file *os.File       // writer
	w    *bufio.Writer  // buffered writer
	buf  []byte         // marshaling buffer

	sfile   *SeriesFile // series lookup
	size    int64       // tracks current file size
	modTime time.Time   // tracks last time write occurred

	mSketch, mTSketch estimator.Sketch // Measurement sketches
	sSketch, sTSketch estimator.Sketch // Series sketche

	// In-memory index.
	mms logMeasurements

	// Filepath to the log file.
	path string
}

// NewLogFile returns a new instance of LogFile.
func NewLogFile(sfile *SeriesFile, path string) *LogFile {
	return &LogFile{
		sfile:    sfile,
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
	data, err := mmap.Map(f.Path(), 0)
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

// TagKeySeriesIDIterator returns a series iterator for a tag key.
func (f *LogFile) TagKeySeriesIDIterator(name, key []byte) SeriesIDIterator {
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
	itrs := make([]SeriesIDIterator, 0, len(tk.tagValues))
	for _, tv := range tk.tagValues {
		if len(tv.series) == 0 {
			continue
		}
		itrs = append(itrs, newLogSeriesIDIterator(tv.series))
	}

	return MergeSeriesIDIterators(itrs...)
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

	e := LogEntry{Flag: LogEntryTagKeyTombstoneFlag, Name: name, Key: key}
	if err := f.appendEntry(&e); err != nil {
		return err
	}
	f.execEntry(&e)
	return nil
}

// TagValueSeriesIDIterator returns a series iterator for a tag value.
func (f *LogFile) TagValueSeriesIDIterator(name, key, value []byte) SeriesIDIterator {
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

	return newLogSeriesIDIterator(tv.series)
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

	e := LogEntry{Flag: LogEntryTagValueTombstoneFlag, Name: name, Key: key, Value: value}
	if err := f.appendEntry(&e); err != nil {
		return err
	}
	f.execEntry(&e)
	return nil
}

// AddSeriesList adds a list of series to the log file in bulk.
func (f *LogFile) AddSeriesList(names [][]byte, tagsSlice []models.Tags) error {
	buf := make([]byte, 2048)

	entries := make([]LogEntry, len(names))
	for i := range names {
		seriesID, err := f.sfile.CreateSeriesIfNotExists(names[i], tagsSlice[i], buf[:0])
		if err != nil {
			return err
		}
		entries[i] = LogEntry{SeriesID: seriesID}
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
	seriesID, err := f.sfile.CreateSeriesIfNotExists(name, tags, nil)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	e := LogEntry{SeriesID: seriesID}
	if err := f.appendEntry(&e); err != nil {
		return err
	}
	f.execEntry(&e)
	return nil
}

// DeleteSeries adds a tombstone for a series to the log file.
func (f *LogFile) DeleteSeriesID(seriesID uint32) error {
	if seriesID == 0 {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	e := LogEntry{Flag: LogEntrySeriesTombstoneFlag, SeriesID: seriesID}
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

/*
// HasSeries returns flags indicating if the series exists and if it is tombstoned.
func (f *LogFile) HasSeries(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool) {
	e := f.SeriesWithBuffer(name, tags, buf)
	if e.SeriesID == 0 {
		return false, false
	}
	return true, e.Deleted
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
*/

/*
// Series returns a series by name/tags.
func (f *LogFile) Series(name []byte, tags models.Tags) SeriesIDElem {
	return f.SeriesWithBuffer(name, tags, nil)
}

// SeriesWithBuffer returns a series by name/tags.
func (f *LogFile) SeriesWithBuffer(name []byte, tags models.Tags, buf []byte) SeriesIDElem {
	key := AppendSeriesKey(buf[:0], name, tags)

	f.mu.RLock()
	defer f.mu.RUnlock()

	mm, ok := f.mms[string(name)]
	if !ok {
		return SeriesIDElem{}
	}

	s := mm.series[string(key)]
	if s == nil {
		return SeriesIDElem{}
	}
	return s
}
*/

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
	mm.series = make(map[uint32]bool)

	// Update measurement tombstone sketch.
	f.mTSketch.Add(e.Name)
}

func (f *LogFile) execDeleteTagKeyEntry(e *LogEntry) {
	mm := f.createMeasurementIfNotExists(e.Name)
	ts := mm.createTagSetIfNotExists(e.Key)

	ts.deleted = true

	mm.tagSet[string(e.Key)] = ts
}

func (f *LogFile) execDeleteTagValueEntry(e *LogEntry) {
	mm := f.createMeasurementIfNotExists(e.Name)
	ts := mm.createTagSetIfNotExists(e.Key)
	tv := ts.createTagValueIfNotExists(e.Value)

	tv.deleted = true

	ts.tagValues[string(e.Value)] = tv
	mm.tagSet[string(e.Key)] = ts
}

func (f *LogFile) execSeriesEntry(e *LogEntry) {
	// Check if series is deleted.
	deleted := (e.Flag & LogEntrySeriesTombstoneFlag) != 0

	seriesKey := f.sfile.SeriesKey(e.SeriesID)
	assert(seriesKey != nil, "series key not found")

	// Read key size.
	_, remainder := ReadSeriesKeyLen(seriesKey)

	// Read measurement name.
	name, remainder := ReadSeriesKeyMeasurement(remainder)
	mm := f.createMeasurementIfNotExists(name)

	// Undelete measurement if it's been tombstoned previously.
	if !deleted && mm.deleted {
		mm.deleted = false
	}

	// Mark series id tombstone.
	mm.series[e.SeriesID] = deleted

	// Read tag count.
	tagN, remainder := ReadSeriesKeyTagN(remainder)

	// Save tags.
	var k, v []byte
	for i := 0; i < tagN; i++ {
		k, v, remainder = ReadSeriesKeyTag(remainder)
		ts := mm.createTagSetIfNotExists(k)
		tv := ts.createTagValueIfNotExists(v)

		// Add a reference to the series on the tag value.
		tv.series[e.SeriesID] = deleted

		ts.tagValues[string(v)] = tv
		mm.tagSet[string(k)] = ts
	}

	// Update the sketches.
	if deleted {
		// TODO(edd) decrement series count...
		f.sTSketch.Add(seriesKey) // Deleting series so update tombstone sketch.
		return
	}

	// TODO(edd) increment series count....
	f.sSketch.Add(seriesKey) // Add series to sketch.
	f.mSketch.Add(name)      // Add measurement to sketch as this may be the fist series for the measurement.
}

// SeriesIDIterator returns an iterator over all series in the log file.
func (f *LogFile) SeriesIDIterator() SeriesIDIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Determine total series count across all measurements.
	var n int
	mSeriesIdx := make([]int, len(f.mms))
	mSeries := make([][]SeriesIDElem, 0, len(f.mms))
	for _, mm := range f.mms {
		n += len(mm.series)
		a := make([]SeriesIDElem, 0, len(mm.series))
		for seriesID, deleted := range mm.series {
			a = append(a, SeriesIDElem{SeriesID: seriesID, Deleted: deleted})
		}
		sort.Sort(SeriesIDElems(a))
		mSeries = append(mSeries, a)
	}

	// Combine series across all measurements by merging the already sorted
	// series lists.
	sBuffer := make([]SeriesIDElem, len(f.mms))
	series := make([]SeriesIDElem, 0, n)
	var minElem SeriesIDElem
	var minElemIdx int

	for s := 0; s < cap(series); s++ {
		for i := 0; i < len(sBuffer); i++ {
			// Are there still serie to pull from this measurement?
			if mSeriesIdx[i] < len(mSeries[i]) && sBuffer[i].SeriesID == 0 {
				// Fill the buffer slot for this measurement.
				sBuffer[i] = mSeries[i][mSeriesIdx[i]]
				mSeriesIdx[i]++
			}

			// Does this measurement have the smallest current serie out of
			// all those in the buffer?
			if minElem.SeriesID == 0 || (sBuffer[i].SeriesID != 0 && sBuffer[i].SeriesID < minElem.SeriesID) {
				minElem, minElemIdx = sBuffer[i], i
			}
		}
		series, minElem.SeriesID, sBuffer[minElemIdx].SeriesID = append(series, minElem), 0, 0
	}

	if len(series) == 0 {
		return nil
	}
	return &logSeriesIDIterator{series: series}
}

// createMeasurementIfNotExists returns a measurement by name.
func (f *LogFile) createMeasurementIfNotExists(name []byte) *logMeasurement {
	mm := f.mms[string(name)]
	if mm == nil {
		mm = &logMeasurement{
			name:   name,
			tagSet: make(map[string]logTagKey),
			series: make(map[uint32]bool),
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

// MeasurementSeriesIDIterator returns an iterator over all series for a measurement.
func (f *LogFile) MeasurementSeriesIDIterator(name []byte) SeriesIDIterator {
	f.mu.RLock()
	defer f.mu.RUnlock()

	mm := f.mms[string(name)]
	if mm == nil || len(mm.series) == 0 {
		return nil
	}
	return newLogSeriesIDIterator(mm.series)
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

	// Flush buffer & mmap series block.
	if err := bw.Flush(); err != nil {
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

	enc := NewTagBlockEncoder(w)
	for _, k := range mm.keys() {
		tag := mm.tagSet[k]

		// Encode tag. Skip values if tag is deleted.
		if err := enc.EncodeKey(tag.name, tag.deleted); err != nil {
			return err
		} else if tag.deleted {
			continue
		}

		// Add each value.
		for _, value := range tag.tagValues {
			if err := enc.EncodeValue(value.name, value.deleted, value.seriesIDs()); err != nil {
				return err
			}
		}
	}

	// Save tagset offset to measurement.
	offset := *n

	// Flush tag block.
	err := enc.Close()
	*n += enc.N()
	if err != nil {
		return err
	}

	// Save tagset offset to measurement.
	size := *n - offset

	info.mms[name] = &logFileMeasurementCompactInfo{offset: offset, size: size}

	return nil
}

func (f *LogFile) writeMeasurementBlockTo(w io.Writer, names []string, info *logFileCompactInfo, n *int64) error {
	mw := NewMeasurementBlockWriter()

	// Add measurement data.
	for _, name := range names {
		mm := f.mms[name]
		mmInfo := info.mms[name]
		assert(mmInfo != nil, "measurement info not found")

		// sort.Sort(uint32Slice(mm.seriesIDs))
		mw.Add(mm.name, mm.deleted, mmInfo.offset, mmInfo.size, mm.seriesIDs())
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

type logFileMeasurementCompactInfo struct {
	offset int64
	size   int64
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
	Flag     byte   // flag
	SeriesID uint32 // series id
	Name     []byte // measurement name
	Key      []byte // tag key
	Value    []byte // tag value
	Checksum uint32 // checksum of flag/name/tags.
	Size     int    // total size of record, in bytes.
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

	// Parse series id.
	if len(data) < 1 {
		return io.ErrShortBuffer
	}
	seriesID, n := binary.Uvarint(data)
	e.SeriesID, data = uint32(seriesID), data[n:]

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

	// Parse key length.
	if len(data) < 1 {
		return io.ErrShortBuffer
	}
	sz, n = binary.Uvarint(data)

	// Read key data.
	if len(data) < n+int(sz) {
		return io.ErrShortBuffer
	}
	e.Key, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse value length.
	if len(data) < 1 {
		return io.ErrShortBuffer
	}
	sz, n = binary.Uvarint(data)

	// Read value data.
	if len(data) < n+int(sz) {
		return io.ErrShortBuffer
	}
	e.Value, data = data[n:n+int(sz)], data[n+int(sz):]

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

	// Append series id.
	n := binary.PutUvarint(buf[:], uint64(e.SeriesID))
	dst = append(dst, buf[:n]...)

	// Append name.
	n = binary.PutUvarint(buf[:], uint64(len(e.Name)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, e.Name...)

	// Append key.
	n = binary.PutUvarint(buf[:], uint64(len(e.Key)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, e.Key...)

	// Append value.
	n = binary.PutUvarint(buf[:], uint64(len(e.Value)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, e.Value...)

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
	series  map[uint32]bool
}

func (mm *logMeasurement) seriesIDs() []uint32 {
	a := make([]uint32, 0, len(mm.series))
	for seriesID := range mm.series {
		a = append(a, seriesID)
	}
	sort.Sort(uint32Slice(a))
	return a
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
		tv = logTagValue{name: value, series: make(map[uint32]bool)}
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
	series  map[uint32]bool
}

func (tv *logTagValue) seriesIDs() []uint32 {
	a := make([]uint32, 0, len(tv.series))
	for seriesID := range tv.series {
		a = append(a, seriesID)
	}
	sort.Sort(uint32Slice(a))
	return a
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

// logSeriesIDIterator represents an iterator over a slice of series.
type logSeriesIDIterator struct {
	series []SeriesIDElem
}

// newLogSeriesIDIterator returns a new instance of logSeriesIDIterator.
// All series are copied to the iterator.
func newLogSeriesIDIterator(m map[uint32]bool) *logSeriesIDIterator {
	if len(m) == 0 {
		return nil
	}

	itr := logSeriesIDIterator{series: make([]SeriesIDElem, 0, len(m))}
	for seriesID, deleted := range m {
		itr.series = append(itr.series, SeriesIDElem{SeriesID: seriesID, Deleted: deleted})
	}
	sort.Sort(SeriesIDElems(itr.series))

	return &itr
}

// Next returns the next element in the iterator.
func (itr *logSeriesIDIterator) Next() SeriesIDElem {
	if len(itr.series) == 0 {
		return SeriesIDElem{}
	}
	elem := itr.series[0]
	itr.series = itr.series[1:]
	return elem
}

// FormatLogFileName generates a log filename for the given index.
func FormatLogFileName(id int) string {
	return fmt.Sprintf("L0-%08d%s", id, LogFileExt)
}
