package tsm1

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb"

	"github.com/golang/snappy"
)

const (
	// DefaultSegmentSize of 2MB is the size at which segment files will be rolled over
	DefaultSegmentSize = 2 * 1024 * 1024

	// FileExtension is the file extension we expect for wal segments
	WALFileExtension = "wal"

	WALFilePrefix = "_"

	writeBufLen = 32 << 10 // 32kb
)

// flushType indiciates why a flush and compaction are being run so the partition can
// do the appropriate type of compaction
type flushType int

const (
	// noFlush indicates that no flush or compaction are necesssary at this time
	noFlush flushType = iota
	// memoryFlush indicates that we should look for the series using the most
	// memory to flush out and compact all others
	memoryFlush
	// idleFlush indicates that we should flush all series in the parition,
	// delete all segment files and hold off on opening a new one
	idleFlush
	// startupFlush indicates that we're flushing because the database is starting up
	startupFlush
)

// walEntry is a byte written to a wal segment file that indicates what the following compressed block contains
type walEntryType byte

const (
	pointsEntry walEntryType = 0x01
	fieldsEntry walEntryType = 0x02
	seriesEntry walEntryType = 0x03
	deleteEntry walEntryType = 0x04
)

var ErrWALClosed = fmt.Errorf("WAL closed")

type Log struct {
	path string

	// write variables
	writeLock          sync.Mutex
	currentSegmentID   int
	currentSegmentFile *os.File
	currentSegmentSize int

	// cache and flush variables
	closing                chan struct{}
	cacheLock              sync.RWMutex
	lastWriteTime          time.Time
	flushRunning           bool
	cache                  map[string]Values
	cacheDirtySort         map[string]bool   // this map should be small, only for dirty vals
	flushCache             map[string]Values // temporary map while flushing
	memorySize             int
	measurementFieldsCache map[string]*tsdb.MeasurementFields
	seriesToCreateCache    []*tsdb.SeriesCreate

	// LogOutput is the writer used by the logger.
	LogOutput io.Writer
	logger    *log.Logger

	// FlushColdInterval is the period of time after which a partition will do a
	// full flush and compaction if it has been cold for writes.
	FlushColdInterval time.Duration

	// SegmentSize is the file size at which a segment file will be rotated
	SegmentSize int

	// FlushMemorySizeThreshold specifies when the log should be forced to be flushed
	FlushMemorySizeThreshold int

	// MaxMemorySizeThreshold specifies the limit at which writes to the WAL should be rejected
	MaxMemorySizeThreshold int

	// IndexWriter is the database series will be flushed to
	IndexWriter IndexWriter

	// LoggingEnabled specifies if detailed logs should be output
	LoggingEnabled bool

	// SkipCache specifies if the wal should immediately write to the index instead of
	// caching data in memory. False by default so we buffer in memory before flushing to index.
	SkipCache bool

	// SkipDurability specifies if the wal should not write the wal entries to disk.
	// False by default which means all writes are durable even when cached before flushing to index.
	SkipDurability bool
}

// IndexWriter is an interface for the indexed database the WAL flushes data to
type IndexWriter interface {
	Write(valuesByKey map[string]Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error
	MarkDeletes(keys []string)
	MarkMeasurementDelete(name string)
}

func NewLog(path string) *Log {
	return &Log{
		path: path,

		// these options should be overriden by any options in the config
		LogOutput:                os.Stderr,
		FlushColdInterval:        tsdb.DefaultFlushColdInterval,
		SegmentSize:              DefaultSegmentSize,
		FlushMemorySizeThreshold: tsdb.DefaultFlushMemorySizeThreshold,
		MaxMemorySizeThreshold:   tsdb.DefaultMaxMemorySizeThreshold,
		logger:                   log.New(os.Stderr, "[tsm1wal] ", log.LstdFlags),
		closing:                  make(chan struct{}),
	}
}

// Path returns the path the log was initialized with.
func (l *Log) Path() string { return l.path }

// Open opens and initializes the Log. Will recover from previous unclosed shutdowns
func (l *Log) Open() error {
	if l.LoggingEnabled {
		l.logger.Printf("tsm1 WAL starting with %d flush memory size threshold and %d max memory size threshold\n", l.FlushMemorySizeThreshold, l.MaxMemorySizeThreshold)
		l.logger.Printf("tsm1 WAL writing to %s\n", l.path)
	}
	if err := os.MkdirAll(l.path, 0777); err != nil {
		return err
	}

	l.cache = make(map[string]Values)
	l.cacheDirtySort = make(map[string]bool)
	l.measurementFieldsCache = make(map[string]*tsdb.MeasurementFields)
	l.closing = make(chan struct{})

	// flush out any WAL entries that are there from before
	if err := l.readAndFlushWAL(); err != nil {
		return err
	}

	return nil
}

// Cursor will return a cursor object to Seek and iterate with Next for the WAL cache for the given.
// This should only ever be called by the engine cursor method, which will always give it
// exactly one field.
func (l *Log) Cursor(series string, fields []string, dec *tsdb.FieldCodec, ascending bool) tsdb.Cursor {
	l.cacheLock.RLock()
	defer l.cacheLock.RUnlock()

	if len(fields) != 1 {
		panic("wal cursor should only ever be called with 1 field")
	}
	ck := SeriesFieldKey(series, fields[0])
	values := l.cache[ck]

	// if we're in the middle of a flush, combine the previous cache
	// with this one for the cursor
	if l.flushCache != nil {
		if fc, ok := l.flushCache[ck]; ok {
			c := make([]Value, len(fc), len(fc)+len(values))
			copy(c, fc)
			c = append(c, values...)

			return newWALCursor(Values(c).Deduplicate(), ascending)
		}
	}

	if l.cacheDirtySort[ck] {
		values = Values(values).Deduplicate()
	}

	// build a copy so writes afterwards don't change the result set
	a := make([]Value, len(values))
	copy(a, values)
	return newWALCursor(a, ascending)
}

func (l *Log) WritePoints(points []models.Point, fields map[string]*tsdb.MeasurementFields, series []*tsdb.SeriesCreate) error {
	// add everything to the cache, or return an error if we've hit our max memory
	if err := l.addToCache(points, fields, series, true); err != nil {
		return err
	}

	// make the write durable if specified
	if !l.SkipDurability {
		// write the points
		pointStrings := make([]string, len(points))
		for i, p := range points {
			pointStrings[i] = p.String()
		}
		data := strings.Join(pointStrings, "\n")
		compressed := snappy.Encode(nil, []byte(data))

		if err := l.writeToLog(pointsEntry, compressed); err != nil {
			return err
		}

		// write the new fields
		if len(fields) > 0 {
			data, err := json.Marshal(fields)
			if err != nil {
				return err
			}
			compressed = snappy.Encode(compressed, data)
			if err := l.writeToLog(fieldsEntry, compressed); err != nil {
				return err
			}
		}

		// write the new series
		if len(series) > 0 {
			data, err := json.Marshal(series)
			if err != nil {
				return err
			}
			compressed = snappy.Encode(compressed, data)
			if err := l.writeToLog(seriesEntry, compressed); err != nil {
				return err
			}
		}
	}

	// usually skipping the cache is only for testing purposes and this was the easiest
	// way to represent the logic (to cache and then immediately flush)
	if l.SkipCache {
		if err := l.flush(idleFlush); err != nil {
			return err
		}
	}

	return nil
}

// addToCache will add the points, measurements, and fields to the cache and return true if successful. They will be queryable
// immediately after return and will be flushed at the next flush cycle. Before adding to the cache we check if we're over the
// max memory threshold. If we are we request a flush in a new goroutine and return an error, indicating we didn't add the values
// to the cache and that writes should return a failure.
func (l *Log) addToCache(points []models.Point, fields map[string]*tsdb.MeasurementFields, series []*tsdb.SeriesCreate, checkMemory bool) error {
	l.cacheLock.Lock()
	defer l.cacheLock.Unlock()

	// Make sure the log has not been closed
	select {
	case <-l.closing:
		return ErrWALClosed
	default:
	}

	// if we should check memory and we're over the threshold, mark a flush as running and kick one off in a goroutine
	if checkMemory && l.memorySize > l.FlushMemorySizeThreshold {
		if !l.flushRunning {
			l.flushRunning = true
			go func() {
				if err := l.flush(memoryFlush); err != nil {
					l.logger.Printf("addToCache: failed to flush: %v", err)
				}
			}()
		}
		if l.memorySize > l.MaxMemorySizeThreshold {
			return fmt.Errorf("WAL backed up flushing to index, hit max memory")
		}
	}

	for _, p := range points {
		for name, value := range p.Fields() {
			k := SeriesFieldKey(string(p.Key()), name)
			v := NewValue(p.Time(), value)
			cacheValues := l.cache[k]

			// only mark it as dirty if it isn't already
			if _, ok := l.cacheDirtySort[k]; !ok && len(cacheValues) > 0 {
				dirty := cacheValues[len(cacheValues)-1].Time().UnixNano() >= v.Time().UnixNano()
				if dirty {
					l.cacheDirtySort[k] = true
				}
			}
			l.memorySize += v.Size()
			l.cache[k] = append(cacheValues, v)
		}
	}

	for k, v := range fields {
		l.measurementFieldsCache[k] = v
	}
	l.seriesToCreateCache = append(l.seriesToCreateCache, series...)
	l.lastWriteTime = time.Now()

	return nil
}

func (l *Log) LastWriteTime() time.Time {
	l.cacheLock.RLock()
	defer l.cacheLock.RUnlock()
	return l.lastWriteTime
}

// readAndFlushWAL is called on open and will read the segment files in, flushing whenever
// the memory gets over the limit. Once all files have been read it will flush and remove the files
func (l *Log) readAndFlushWAL() error {
	files, err := l.segmentFileNames()
	if err != nil {
		return err
	}

	// read all the segment files and cache them, flushing along the way if we
	// hit memory limits
	for _, fn := range files {
		if err := l.readFileToCache(fn); err != nil {
			return err
		}

		if l.memorySize > l.MaxMemorySizeThreshold {
			if err := l.flush(memoryFlush); err != nil {
				return err
			}
		}
	}

	// now flush and remove all the old files
	if err := l.flush(startupFlush); err != nil {
		return err
	}

	return nil
}

func (l *Log) readFileToCache(fileName string) error {
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, writeBufLen)
	data := make([]byte, writeBufLen)
	for {
		// read the type and the length of the entry
		_, err := io.ReadFull(f, buf[0:5])
		if err == io.EOF {
			return nil
		} else if err != nil {
			l.logger.Printf("error reading segment file %s: %s", fileName, err.Error())
			return err
		}
		entryType := buf[0]
		length := btou32(buf[1:5])

		// read the compressed block and decompress it
		if int(length) > len(buf) {
			buf = make([]byte, length)
		}
		_, err = io.ReadFull(f, buf[0:length])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			l.logger.Printf("hit end of file while reading compressed wal entry from %s", fileName)
			return nil
		} else if err != nil {
			return err
		}
		data, err = snappy.Decode(data, buf[0:length])
		if err != nil {
			l.logger.Printf("error decoding compressed entry from %s: %s", fileName, err.Error())
			return nil
		}

		// and marshal it and send it to the cache
		switch walEntryType(entryType) {
		case pointsEntry:
			points, err := models.ParsePoints(data)
			if err != nil {
				l.logger.Printf("failed to parse points: %v", err)
				return err
			}
			l.addToCache(points, nil, nil, false)
		case fieldsEntry:
			fields := make(map[string]*tsdb.MeasurementFields)
			if err := json.Unmarshal(data, &fields); err != nil {
				return err
			}
			l.addToCache(nil, fields, nil, false)
		case seriesEntry:
			var series []*tsdb.SeriesCreate
			if err := json.Unmarshal(data, &series); err != nil {
				return err
			}
			l.addToCache(nil, nil, series, false)
		case deleteEntry:
			d := &deleteData{}
			if err := json.Unmarshal(data, &d); err != nil {
				return err
			}
			l.IndexWriter.MarkDeletes(d.Keys)
			l.IndexWriter.MarkMeasurementDelete(d.MeasurementName)
			l.deleteKeysFromCache(d.Keys)
			if d.MeasurementName != "" {
				l.deleteMeasurementFromCache(d.MeasurementName)
			}
		}
	}
}

func (l *Log) writeToLog(writeType walEntryType, data []byte) error {
	l.writeLock.Lock()
	defer l.writeLock.Unlock()

	// Make sure the log has not been closed
	select {
	case <-l.closing:
		return ErrWALClosed
	default:
	}

	if l.currentSegmentFile == nil || l.currentSegmentSize > DefaultSegmentSize {
		if err := l.newSegmentFile(); err != nil {
			// A drop database or RP call could trigger this error if writes were in-flight
			// when the drop statement executes.
			return fmt.Errorf("error opening new segment file for wal: %s", err.Error())
		}
	}

	// The panics here are an intentional choice. Based on reports from users
	// it's better to fail hard if the database can't take writes. Then they'll
	// get alerted and fix whatever is broken. Remove these and face Paul's wrath.
	if _, err := l.currentSegmentFile.Write([]byte{byte(writeType)}); err != nil {
		panic(fmt.Sprintf("error writing type to wal: %s", err.Error()))
	}
	if _, err := l.currentSegmentFile.Write(u32tob(uint32(len(data)))); err != nil {
		panic(fmt.Sprintf("error writing len to wal: %s", err.Error()))
	}
	if _, err := l.currentSegmentFile.Write(data); err != nil {
		panic(fmt.Sprintf("error writing data to wal: %s", err.Error()))
	}

	l.currentSegmentSize += 5 + len(data)

	return l.currentSegmentFile.Sync()
}

// Flush will force a flush of the WAL to the index
func (l *Log) Flush() error {
	return l.flush(idleFlush)
}

func (l *Log) DeleteMeasurement(measurement string, keys []string) error {
	d := &deleteData{MeasurementName: measurement, Keys: keys}
	err := l.writeDeleteEntry(d)
	if err != nil {
		return err
	}

	l.deleteKeysFromCache(keys)
	l.deleteMeasurementFromCache(measurement)

	return nil
}

func (l *Log) deleteMeasurementFromCache(name string) {
	l.cacheLock.Lock()
	defer l.cacheLock.Unlock()
	delete(l.measurementFieldsCache, name)
}

func (l *Log) writeDeleteEntry(d *deleteData) error {
	js, err := json.Marshal(d)
	if err != nil {
		return err
	}
	data := snappy.Encode(nil, js)
	return l.writeToLog(deleteEntry, data)
}

func (l *Log) DeleteSeries(keys []string) error {
	l.deleteKeysFromCache(keys)

	return l.writeDeleteEntry(&deleteData{Keys: keys})
}

func (l *Log) deleteKeysFromCache(keys []string) {
	seriesKeys := make(map[string]bool)
	for _, k := range keys {
		series, _ := seriesAndFieldFromCompositeKey(k)
		seriesKeys[series] = true
	}

	l.cacheLock.Lock()
	defer l.cacheLock.Unlock()

	for _, k := range keys {
		delete(l.cache, k)
	}

	// now remove any of these that are marked for creation
	var seriesCreate []*tsdb.SeriesCreate
	for _, sc := range l.seriesToCreateCache {
		if _, ok := seriesKeys[sc.Series.Key]; !ok {
			seriesCreate = append(seriesCreate, sc)
		}
	}
	l.seriesToCreateCache = seriesCreate
}

// Close will finish any flush that is currently in process and close file handles
func (l *Log) Close() error {
	l.cacheLock.Lock()
	l.writeLock.Lock()
	defer l.cacheLock.Unlock()
	defer l.writeLock.Unlock()

	// If cache is nil, then we're not open.  This avoids a double-close in tests.
	if l.cache != nil {
		// Close, but don't set to nil so future goroutines can still be signaled
		close(l.closing)
	}

	l.cache = nil
	l.measurementFieldsCache = nil
	l.seriesToCreateCache = nil

	if l.currentSegmentFile != nil {
		l.currentSegmentFile.Close()
		l.currentSegmentFile = nil
	}

	return nil
}

// flush writes all wal data in memory to the index
func (l *Log) flush(flush flushType) error {
	// Make sure the log has not been closed
	select {
	case <-l.closing:
		return ErrWALClosed
	default:
	}

	// only flush if there isn't one already running. Memory flushes are only triggered
	// by writes, which will mark the flush as running, so we can ignore it.
	l.cacheLock.Lock()

	if l.flushRunning && flush != memoryFlush {
		l.cacheLock.Unlock()
		return nil
	}

	// mark the flush as running and ensure that it gets marked as not running when we return
	l.flushRunning = true
	defer func() {
		l.cacheLock.Lock()
		l.flushRunning = false
		l.cacheLock.Unlock()
	}()

	// only hold the lock while we rotate the segment file
	l.writeLock.Lock()
	lastFileID := l.currentSegmentID
	// if it's an idle flush, don't open a new segment file
	if flush == idleFlush {
		if l.currentSegmentFile != nil {
			if err := l.currentSegmentFile.Close(); err != nil {
				l.cacheLock.Unlock()
				l.writeLock.Unlock()
				return fmt.Errorf("error closing current segment: %v", err)
			}
			l.currentSegmentFile = nil
			l.currentSegmentSize = 0
		}
	} else {
		if err := l.newSegmentFile(); err != nil {
			l.cacheLock.Unlock()
			l.writeLock.Unlock()
			return fmt.Errorf("error creating new wal file: %v", err)
		}
	}
	l.writeLock.Unlock()

	// copy the cache items to new maps so we can empty them out
	l.flushCache = make(map[string]Values)
	valueCount := 0
	for key, v := range l.cache {
		l.flushCache[key] = v
		valueCount += len(v)
	}
	l.cache = make(map[string]Values)
	for k := range l.cacheDirtySort {
		l.flushCache[k] = l.flushCache[k].Deduplicate()
	}
	l.cacheDirtySort = make(map[string]bool)

	flushSize := l.memorySize

	// reset the memory being used by the cache
	l.memorySize = 0

	// reset the measurements for flushing
	mfc := l.measurementFieldsCache
	l.measurementFieldsCache = make(map[string]*tsdb.MeasurementFields)

	// reset the series for flushing
	scc := l.seriesToCreateCache
	l.seriesToCreateCache = nil

	l.cacheLock.Unlock()

	// exit if there's nothing to flush to the index
	if len(l.flushCache) == 0 && len(mfc) == 0 && len(scc) == 0 && flush != startupFlush {
		return nil
	}

	if l.LoggingEnabled {
		ftype := "idle"
		if flush == memoryFlush {
			ftype = "memory"
		} else if flush == startupFlush {
			ftype = "startup"
		}
		l.logger.Printf("%s flush of %s with %d keys and %d total values of %d bytes\n", ftype, l.path, len(l.flushCache), valueCount, flushSize)
	}

	startTime := time.Now()
	if err := l.IndexWriter.Write(l.flushCache, mfc, scc); err != nil {
		l.logger.Printf("failed to flush to index: %v", err)
		return err
	}
	if l.LoggingEnabled {
		l.logger.Printf("%s flush to index took %s\n", l.path, time.Since(startTime))
	}

	l.cacheLock.Lock()
	l.flushCache = nil
	l.cacheLock.Unlock()

	// remove all the old segment files
	fileNames, err := l.segmentFileNames()
	if err != nil {
		return err
	}
	for _, fn := range fileNames {
		id, err := idFromFileName(fn)
		if err != nil {
			return err
		}
		if id <= lastFileID {
			err := os.RemoveAll(fn)
			if err != nil {
				return fmt.Errorf("failed to remove: %v: %v", fn, err)
			}
		}
	}

	return nil
}

// segmentFileNames will return all files that are WAL segment files in sorted order by ascending ID
func (l *Log) segmentFileNames() ([]string, error) {
	names, err := filepath.Glob(filepath.Join(l.path, fmt.Sprintf("%s*.%s", WALFilePrefix, WALFileExtension)))
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// newSegmentFile will close the current segment file and open a new one, updating bookkeeping info on the log
func (l *Log) newSegmentFile() error {
	l.currentSegmentID++
	if l.currentSegmentFile != nil {
		if err := l.currentSegmentFile.Close(); err != nil {
			return err
		}
	}

	fileName := filepath.Join(l.path, fmt.Sprintf("%s%05d.%s", WALFilePrefix, l.currentSegmentID, WALFileExtension))
	ff, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	l.currentSegmentSize = 0
	l.currentSegmentFile = ff

	return nil
}

// shouldFlush will return the flushType specifying whether we should flush. memoryFlush
// is never returned from this function since those can only be triggered by writes
func (l *Log) shouldFlush() flushType {
	l.cacheLock.RLock()
	defer l.cacheLock.RUnlock()

	if l.flushRunning {
		return noFlush
	}

	if len(l.cache) == 0 {
		return noFlush
	}

	if time.Since(l.lastWriteTime) > l.FlushColdInterval {
		return idleFlush
	}

	return noFlush
}

// cursor is a unidirectional iterator for a given entry in the cache
type walCursor struct {
	cache     Values
	position  int
	ascending bool
}

func newWALCursor(cache Values, ascending bool) *walCursor {
	// position is set such that a call to Next will successfully advance
	// to the next postion and return the value.
	c := &walCursor{cache: cache, ascending: ascending, position: -1}
	if !ascending {
		c.position = len(c.cache)
	}
	return c
}

func (c *walCursor) Ascending() bool { return c.ascending }

// Seek will point the cursor to the given time (or key)
func (c *walCursor) SeekTo(seek int64) (int64, interface{}) {
	// Seek cache index
	c.position = sort.Search(len(c.cache), func(i int) bool {
		return c.cache[i].Time().UnixNano() >= seek
	})

	// If seek is not in the cache, return the last value in the cache
	if !c.ascending && c.position >= len(c.cache) {
		c.position = len(c.cache) - 1
	}

	// Make sure our position points to something in the cache
	if c.position < 0 || c.position >= len(c.cache) {
		return tsdb.EOF, nil
	}

	v := c.cache[c.position]

	return v.Time().UnixNano(), v.Value()
}

// Next moves the cursor to the next key/value. will return nil if at the end
func (c *walCursor) Next() (int64, interface{}) {
	var v Value
	if c.ascending {
		v = c.nextForward()
	} else {
		v = c.nextReverse()
	}

	return v.Time().UnixNano(), v.Value()
}

// nextForward advances the cursor forward returning the next value
func (c *walCursor) nextForward() Value {
	c.position++

	if c.position >= len(c.cache) {
		return &EmptyValue{}
	}

	return c.cache[c.position]
}

// nextReverse advances the cursor backwards returning the next value
func (c *walCursor) nextReverse() Value {
	c.position--

	if c.position < 0 {
		return &EmptyValue{}
	}

	return c.cache[c.position]
}

// deleteData holds the information for a delete entry
type deleteData struct {
	// MeasurementName will be empty for deletes that are only against series
	MeasurementName string
	Keys            []string
}

// idFromFileName parses the segment file ID from its name
func idFromFileName(name string) (int, error) {
	parts := strings.Split(filepath.Base(name), ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("file %s has wrong name format to have an id", name)
	}

	id, err := strconv.ParseUint(parts[0][1:], 10, 32)

	return int(id), err
}
