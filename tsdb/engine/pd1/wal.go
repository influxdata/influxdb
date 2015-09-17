package pd1

import (
	"bytes"
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

	"github.com/golang/snappy"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	// DefaultSegmentSize of 2MB is the size at which segment files will be rolled over
	DefaultSegmentSize = 2 * 1024 * 1024

	// FileExtension is the file extension we expect for wal segments
	WALFileExtension = "wal"

	WALFilePrefix = "_"

	// defaultFlushCheckInterval is how often flushes are triggered automatically by the flush criteria
	defaultFlushCheckInterval = time.Second

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
	// deleteFlush indicates that we're flushing because series need to be removed from the WAL
	deleteFlush
	// startupFlush indicates that we're flushing because the database is starting up
	startupFlush
)

// walEntry is a byte written to a wal segment file that indicates what the following compressed block contains
type walEntryType byte

const (
	pointsEntry walEntryType = 0x01
	fieldsEntry walEntryType = 0x02
	seriesEntry walEntryType = 0x03
)

type Log struct {
	path string

	flushCheckTimer    *time.Timer // check this often to see if a background flush should happen
	flushCheckInterval time.Duration

	// write variables
	writeLock          sync.Mutex
	currentSegmentID   int
	currentSegmentFile *os.File
	currentSegmentSize int

	// cache and flush variables
	cacheLock              sync.RWMutex
	lastWriteTime          time.Time
	flushRunning           bool
	cache                  map[string]Values
	cacheDirtySort         map[string]bool   // this map should be small, only for dirty vals
	flushCache             map[string]Values // temporary map while flushing
	memorySize             int
	measurementFieldsCache map[string]*tsdb.MeasurementFields
	seriesToCreateCache    []*tsdb.SeriesCreate

	// These coordinate closing and waiting for running goroutines.
	wg      sync.WaitGroup
	closing chan struct{}

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

	// Index is the database series will be flushed to
	Index IndexWriter

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
	WriteAndCompact(valuesByKey map[string]Values, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error
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
		flushCheckInterval:       defaultFlushCheckInterval,
		logger:                   log.New(os.Stderr, "[pd1wal] ", log.LstdFlags),
	}
}

// Open opens and initializes the Log. Will recover from previous unclosed shutdowns
func (l *Log) Open() error {

	if l.LoggingEnabled {
		l.logger.Printf("PD1 WAL starting with %d flush memory size threshold and %d max memory size threshold\n", l.FlushMemorySizeThreshold, l.MaxMemorySizeThreshold)
		l.logger.Printf("PD1 WAL writing to %s\n", l.path)
	}
	if err := os.MkdirAll(l.path, 0777); err != nil {
		return err
	}

	l.cache = make(map[string]Values)
	l.cacheDirtySort = make(map[string]bool)
	l.measurementFieldsCache = make(map[string]*tsdb.MeasurementFields)

	// flush out any WAL entries that are there from before
	if err := l.readAndFlushWAL(); err != nil {
		return err
	}

	l.flushCheckTimer = time.NewTimer(l.flushCheckInterval)

	// Start background goroutines.
	l.wg.Add(1)
	l.closing = make(chan struct{})
	go l.autoflusher(l.closing)

	return nil
}

// Cursor will return a cursor object to Seek and iterate with Next for the WAL cache for the given
func (l *Log) Cursor(key string, direction tsdb.Direction) tsdb.Cursor {
	l.cacheLock.RLock()
	defer l.cacheLock.RUnlock()

	// TODO: make this work for other fields
	ck := seriesFieldKey(key, "value")
	values := l.cache[ck]

	// if we're in the middle of a flush, combine the previous cache
	// with this one for the cursor
	if l.flushCache != nil {
		if fc, ok := l.flushCache[ck]; ok {
			c := make([]Value, len(fc), len(fc)+len(values))
			copy(c, fc)
			c = append(c, values...)

			return newWALCursor(c, direction)
		}
	}

	if l.cacheDirtySort[ck] {
		sort.Sort(values)
		delete(l.cacheDirtySort, ck)
	}

	// build a copy so writes afterwards don't change the result set
	a := make([]Value, len(values))
	copy(a, values)
	return newWALCursor(a, direction)
}

func (l *Log) WritePoints(points []tsdb.Point, fields map[string]*tsdb.MeasurementFields, series []*tsdb.SeriesCreate) error {
	// add everything to the cache, or return an error if we've hit our max memory
	if addedToCache := l.addToCache(points, fields, series, true); !addedToCache {
		return fmt.Errorf("WAL backed up flushing to index, hit max memory")
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
		l.flush(idleFlush)
	}

	return nil
}

// addToCache will add the points, measurements, and fields to the cache and return true if successful. They will be queryable
// immediately after return and will be flushed at the next flush cycle. Before adding to the cache we check if we're over the
// max memory threshold. If we are we request a flush in a new goroutine and return false, indicating we didn't add the values
// to the cache and that writes should return a failure.
func (l *Log) addToCache(points []tsdb.Point, fields map[string]*tsdb.MeasurementFields, series []*tsdb.SeriesCreate, checkMemory bool) bool {
	l.cacheLock.Lock()
	defer l.cacheLock.Unlock()

	// if we should check memory and we're over the threshold, mark a flush as running and kick one off in a goroutine
	if checkMemory && l.memorySize > l.MaxMemorySizeThreshold {
		if !l.flushRunning {
			l.flushRunning = true
			go l.flush(memoryFlush)
		}
		return false
	}

	for _, p := range points {
		for name, value := range p.Fields() {
			k := seriesFieldKey(string(p.Key()), name)
			v := NewValue(p.Time(), value)
			cacheValues := l.cache[k]

			// only mark it as dirty if it isn't already
			if _, ok := l.cacheDirtySort[k]; !ok && len(cacheValues) > 0 {
				dirty := cacheValues[len(cacheValues)-1].Time().UnixNano() > v.Time().UnixNano()
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

	return true
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
		if err == io.EOF {
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
			points, err := tsdb.ParsePoints(data)
			if err != nil {
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
			series := make([]*tsdb.SeriesCreate, 0)
			if err := json.Unmarshal(data, &series); err != nil {
				return err
			}
			l.addToCache(nil, nil, series, false)
		}
	}
}

func (l *Log) writeToLog(writeType walEntryType, data []byte) error {
	l.writeLock.Lock()
	defer l.writeLock.Unlock()

	if l.currentSegmentFile == nil {
		if err := l.newSegmentFile(); err != nil {
			// fail hard since we can't write data
			panic(fmt.Sprintf("error opening new segment file for wal: %s", err.Error()))
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

	return l.currentSegmentFile.Sync()
}

// Flush will force a flush of the WAL to the index
func (l *Log) Flush() error {
	return l.flush(idleFlush)
}

func (l *Log) DeleteSeries(keys []string) error {
	panic("not implemented")
}

// Close will finish any flush that is currently in process and close file handles
func (l *Log) Close() error {
	// stop the autoflushing process so it doesn't try to kick another one off
	l.writeLock.Lock()
	l.cacheLock.Lock()

	if l.closing != nil {
		close(l.closing)
		l.closing = nil
	}
	l.writeLock.Unlock()
	l.cacheLock.Unlock()

	// Allow goroutines to finish running.
	l.wg.Wait()

	// Lock the remainder of the closing process.
	l.writeLock.Lock()
	l.cacheLock.Lock()
	defer l.writeLock.Unlock()
	defer l.cacheLock.Unlock()

	l.cache = nil
	l.measurementFieldsCache = nil
	l.seriesToCreateCache = nil
	if l.currentSegmentFile == nil {
		return nil
	}
	if err := l.currentSegmentFile.Close(); err != nil {
		return err
	}
	l.currentSegmentFile = nil

	return nil
}

// close all the open Log partitions and file handles
func (l *Log) close() error {
	l.cache = nil
	l.cacheDirtySort = nil
	if l.currentSegmentFile == nil {
		return nil
	}
	if err := l.currentSegmentFile.Close(); err != nil {
		return err
	}
	l.currentSegmentFile = nil

	return nil
}

// flush writes all wal data in memory to the index
func (l *Log) flush(flush flushType) error {
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
	if err := l.newSegmentFile(); err != nil {
		// there's no recovering from this, fail hard
		panic(fmt.Sprintf("error creating new wal file: %s", err.Error()))
	}
	l.writeLock.Unlock()

	// copy the cache items to new maps so we can empty them out
	l.flushCache = l.cache
	l.cache = make(map[string]Values)
	l.cacheDirtySort = make(map[string]bool)
	valuesByKey := make(map[string]Values)

	valueCount := 0
	for key, v := range l.flushCache {
		valuesByKey[key] = v
		valueCount += len(v)
	}

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
	if len(valuesByKey) == 0 && len(mfc) == 0 && len(scc) == 0 {
		return nil
	}

	if l.LoggingEnabled {
		ftype := "idle"
		if flush == memoryFlush {
			ftype = "memory"
		} else if flush == startupFlush {
			ftype = "startup"
		}
		l.logger.Printf("%s flush of %d keys with %d values of %d bytes\n", ftype, len(valuesByKey), valueCount, flushSize)
	}

	startTime := time.Now()
	if err := l.Index.WriteAndCompact(valuesByKey, mfc, scc); err != nil {
		return err
	}
	if l.LoggingEnabled {
		l.logger.Printf("flush to index took %s\n", time.Since(startTime))
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
			err := os.Remove(fn)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// triggerAutoFlush will flush and compact any partitions that have hit the thresholds for compaction
func (l *Log) triggerAutoFlush() {
	//
	if f := l.shouldFlush(); f != noFlush {
		if err := l.flush(f); err != nil {
			l.logger.Printf("error flushing wal: %s\n", err)
		}
	}
}

// autoflusher waits for notification of a flush and kicks it off in the background.
// This method runs in a separate goroutine.
func (l *Log) autoflusher(closing chan struct{}) {
	defer l.wg.Done()

	for {
		// Wait for close or flush signal.
		select {
		case <-closing:
			return
		case <-l.flushCheckTimer.C:
			l.triggerAutoFlush()
			l.flushCheckTimer.Reset(l.flushCheckInterval)
		}
	}
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
	l.currentSegmentID += 1
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
	direction tsdb.Direction
}

func newWALCursor(cache Values, direction tsdb.Direction) *walCursor {
	// position is set such that a call to Next will successfully advance
	// to the next postion and return the value.
	c := &walCursor{cache: cache, direction: direction, position: -1}
	if direction.Reverse() {
		c.position = len(c.cache)
	}
	return c
}

func (c *walCursor) Direction() tsdb.Direction { return c.direction }

// Seek will point the cursor to the given time (or key)
func (c *walCursor) Seek(seek []byte) (key, value []byte) {
	// Seek cache index
	c.position = sort.Search(len(c.cache), func(i int) bool {
		return bytes.Compare(c.cache[i].TimeBytes(), seek) != -1
	})

	// If seek is not in the cache, return the last value in the cache
	if c.direction.Reverse() && c.position >= len(c.cache) {
		c.position = len(c.cache)
	}

	// Make sure our position points to something in the cache
	if c.position < 0 || c.position >= len(c.cache) {
		return nil, nil
	}

	v := c.cache[c.position]

	return v.TimeBytes(), v.ValueBytes()
}

// Next moves the cursor to the next key/value. will return nil if at the end
func (c *walCursor) Next() (key, value []byte) {
	var v Value
	if c.direction.Forward() {
		v = c.nextForward()
	} else {
		v = c.nextReverse()
	}

	return v.TimeBytes(), v.ValueBytes()
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

// idFromFileName parses the segment file ID from its name
func idFromFileName(name string) (int, error) {
	parts := strings.Split(filepath.Base(name), ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("file %s has wrong name format to have an id", name)
	}

	id, err := strconv.ParseUint(parts[0][1:], 10, 32)

	return int(id), err
}
