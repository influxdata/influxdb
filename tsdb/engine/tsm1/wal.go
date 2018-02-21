package tsm1

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/pkg/pool"
	"go.uber.org/zap"
)

const (
	// DefaultSegmentSize of 10MB is the size at which segment files will be rolled over.
	DefaultSegmentSize = 10 * 1024 * 1024

	// WALFileExtension is the file extension we expect for wal segments.
	WALFileExtension = "wal"

	// WALFilePrefix is the prefix on all wal segment files.
	WALFilePrefix = "_"

	// walEncodeBufSize is the size of the wal entry encoding buffer
	walEncodeBufSize = 4 * 1024 * 1024

	float64EntryType  = 1
	integerEntryType  = 2
	booleanEntryType  = 3
	stringEntryType   = 4
	unsignedEntryType = 5
)

// WalEntryType is a byte written to a wal segment file that indicates what the following compressed block contains.
type WalEntryType byte

const (
	// WriteWALEntryType indicates a write entry.
	WriteWALEntryType WalEntryType = 0x01

	// DeleteWALEntryType indicates a delete entry.
	DeleteWALEntryType WalEntryType = 0x02

	// DeleteRangeWALEntryType indicates a delete range entry.
	DeleteRangeWALEntryType WalEntryType = 0x03
)

var (
	// ErrWALClosed is returned when attempting to write to a closed WAL file.
	ErrWALClosed = fmt.Errorf("WAL closed")

	// ErrWALCorrupt is returned when reading a corrupt WAL entry.
	ErrWALCorrupt = fmt.Errorf("corrupted WAL entry")

	defaultWaitingWALWrites = runtime.GOMAXPROCS(0) * 2

	// bytePool is a shared bytes pool buffer re-cycle []byte slices to reduce allocations.
	bytesPool = pool.NewLimitedBytes(256, walEncodeBufSize*2)
)

// Statistics gathered by the WAL.
const (
	statWALOldBytes     = "oldSegmentsDiskBytes"
	statWALCurrentBytes = "currentSegmentDiskBytes"
	statWriteOk         = "writeOk"
	statWriteErr        = "writeErr"
)

// WAL represents the write-ahead log used for writing TSM files.
type WAL struct {
	// goroutines waiting for the next fsync
	syncCount   uint64
	syncWaiters chan chan error

	mu            sync.RWMutex
	lastWriteTime time.Time

	path string

	// write variables
	currentSegmentID     int
	currentSegmentWriter *WALSegmentWriter

	// cache and flush variables
	once    sync.Once
	closing chan struct{}

	// syncDelay sets the duration to wait before fsyncing writes.  A value of 0 (default)
	// will cause every write to be fsync'd.  This must be set before the WAL
	// is opened if a non-default value is required.
	syncDelay time.Duration

	// WALOutput is the writer used by the logger.
	logger       *zap.Logger // Logger to be used for important messages
	traceLogger  *zap.Logger // Logger to be used when trace-logging is on.
	traceLogging bool

	// SegmentSize is the file size at which a segment file will be rotated
	SegmentSize int

	// statistics for the WAL
	stats   *WALStatistics
	limiter limiter.Fixed
}

// NewWAL initializes a new WAL at the given directory.
func NewWAL(path string) *WAL {
	logger := zap.NewNop()
	return &WAL{
		path: path,

		// these options should be overriden by any options in the config
		SegmentSize: DefaultSegmentSize,
		closing:     make(chan struct{}),
		syncWaiters: make(chan chan error, 1024),
		stats:       &WALStatistics{},
		limiter:     limiter.NewFixed(defaultWaitingWALWrites),
		logger:      logger,
		traceLogger: logger,
	}
}

// enableTraceLogging must be called before the WAL is opened.
func (l *WAL) enableTraceLogging(enabled bool) {
	l.traceLogging = enabled
	if enabled {
		l.traceLogger = l.logger
	}
}

// WithLogger sets the WAL's logger.
func (l *WAL) WithLogger(log *zap.Logger) {
	l.logger = log.With(zap.String("service", "wal"))

	if l.traceLogging {
		l.traceLogger = l.logger
	}
}

// WALStatistics maintains statistics about the WAL.
type WALStatistics struct {
	OldBytes     int64
	CurrentBytes int64
	WriteOK      int64
	WriteErr     int64
}

// Statistics returns statistics for periodic monitoring.
func (l *WAL) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "tsm1_wal",
		Tags: tags,
		Values: map[string]interface{}{
			statWALOldBytes:     atomic.LoadInt64(&l.stats.OldBytes),
			statWALCurrentBytes: atomic.LoadInt64(&l.stats.CurrentBytes),
			statWriteOk:         atomic.LoadInt64(&l.stats.WriteOK),
			statWriteErr:        atomic.LoadInt64(&l.stats.WriteErr),
		},
	}}
}

// Path returns the directory the log was initialized with.
func (l *WAL) Path() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.path
}

// Open opens and initializes the Log. Open can recover from previous unclosed shutdowns.
func (l *WAL) Open() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.traceLogger.Info("tsm1 WAL starting", zap.Int("segment_size", l.SegmentSize))
	l.traceLogger.Info("tsm1 WAL writing", zap.String("path", l.path))

	if err := os.MkdirAll(l.path, 0777); err != nil {
		return err
	}

	segments, err := segmentFileNames(l.path)
	if err != nil {
		return err
	}

	if len(segments) > 0 {
		lastSegment := segments[len(segments)-1]
		id, err := idFromFileName(lastSegment)
		if err != nil {
			return err
		}

		l.currentSegmentID = id
		stat, err := os.Stat(lastSegment)
		if err != nil {
			return err
		}

		if stat.Size() == 0 {
			os.Remove(lastSegment)
			segments = segments[:len(segments)-1]
		} else {
			fd, err := os.OpenFile(lastSegment, os.O_RDWR, 0666)
			if err != nil {
				return err
			}
			if _, err := fd.Seek(0, io.SeekEnd); err != nil {
				return err
			}
			l.currentSegmentWriter = NewWALSegmentWriter(fd)

			// Reset the current segment size stat
			atomic.StoreInt64(&l.stats.CurrentBytes, stat.Size())
		}
	}

	var totalOldDiskSize int64
	for _, seg := range segments {
		stat, err := os.Stat(seg)
		if err != nil {
			return err
		}

		if stat.Size() > 0 {
			totalOldDiskSize += stat.Size()
			if stat.ModTime().After(l.lastWriteTime) {
				l.lastWriteTime = stat.ModTime().UTC()
			}
		}
	}
	atomic.StoreInt64(&l.stats.OldBytes, totalOldDiskSize)

	l.closing = make(chan struct{})

	return nil
}

// scheduleSync will schedule an fsync to the current wal segment and notify any
// waiting gorutines.  If an fsync is already scheduled, subsequent calls will
// not schedule a new fsync and will be handle by the existing scheduled fsync.
func (l *WAL) scheduleSync() {
	// If we're not the first to sync, then another goroutine is fsyncing the wal for us.
	if !atomic.CompareAndSwapUint64(&l.syncCount, 0, 1) {
		return
	}

	// Fsync the wal and notify all pending waiters
	go func() {
		var timerCh <-chan time.Time

		// time.NewTicker requires a > 0 delay, since 0 indicates no delay, use a closed
		// channel which will always be ready to read from.
		if l.syncDelay == 0 {
			// Create a RW chan and close it
			timerChrw := make(chan time.Time)
			close(timerChrw)
			// Convert it to a read-only
			timerCh = timerChrw
		} else {
			t := time.NewTicker(l.syncDelay)
			defer t.Stop()
			timerCh = t.C
		}
		for {
			select {
			case <-timerCh:
				l.mu.Lock()
				if len(l.syncWaiters) == 0 {
					atomic.StoreUint64(&l.syncCount, 0)
					l.mu.Unlock()
					return
				}

				l.sync()
				l.mu.Unlock()
			case <-l.closing:
				atomic.StoreUint64(&l.syncCount, 0)
				return
			}
		}
	}()
}

// sync fsyncs the current wal segments and notifies any waiters.  Callers must ensure
// a write lock on the WAL is obtained before calling sync.
func (l *WAL) sync() {
	err := l.currentSegmentWriter.sync()
	for len(l.syncWaiters) > 0 {
		errC := <-l.syncWaiters
		errC <- err
	}
}

// WriteMulti writes the given values to the WAL. It returns the WAL segment ID to
// which the points were written. If an error is returned the segment ID should
// be ignored.
func (l *WAL) WriteMulti(values map[string][]Value) (int, error) {
	entry := &WriteWALEntry{
		Values: values,
	}

	id, err := l.writeToLog(entry)
	if err != nil {
		atomic.AddInt64(&l.stats.WriteErr, 1)
		return -1, err
	}
	atomic.AddInt64(&l.stats.WriteOK, 1)

	return id, nil
}

// ClosedSegments returns a slice of the names of the closed segment files.
func (l *WAL) ClosedSegments() ([]string, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	// Not loading files from disk so nothing to do
	if l.path == "" {
		return nil, nil
	}

	var currentFile string
	if l.currentSegmentWriter != nil {
		currentFile = l.currentSegmentWriter.path()
	}

	files, err := segmentFileNames(l.path)
	if err != nil {
		return nil, err
	}

	var closedFiles []string
	for _, fn := range files {
		// Skip the current path
		if fn == currentFile {
			continue
		}

		closedFiles = append(closedFiles, fn)
	}

	return closedFiles, nil
}

// Remove deletes the given segment file paths from disk and cleans up any associated objects.
func (l *WAL) Remove(files []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, fn := range files {
		l.traceLogger.Info("Removing WAL file", zap.String("path", fn))
		os.RemoveAll(fn)
	}

	// Refresh the on-disk size stats
	segments, err := segmentFileNames(l.path)
	if err != nil {
		return err
	}

	var totalOldDiskSize int64
	for _, seg := range segments {
		stat, err := os.Stat(seg)
		if err != nil {
			return err
		}

		totalOldDiskSize += stat.Size()
	}
	atomic.StoreInt64(&l.stats.OldBytes, totalOldDiskSize)

	return nil
}

// LastWriteTime is the last time anything was written to the WAL.
func (l *WAL) LastWriteTime() time.Time {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastWriteTime
}

func (l *WAL) DiskSizeBytes() int64 {
	return atomic.LoadInt64(&l.stats.OldBytes) + atomic.LoadInt64(&l.stats.CurrentBytes)
}

func (l *WAL) writeToLog(entry WALEntry) (int, error) {
	// limit how many concurrent encodings can be in flight.  Since we can only
	// write one at a time to disk, a slow disk can cause the allocations below
	// to increase quickly.  If we're backed up, wait until others have completed.
	bytes := bytesPool.Get(entry.MarshalSize())

	b, err := entry.Encode(bytes)
	if err != nil {
		bytesPool.Put(bytes)
		return -1, err
	}

	encBuf := bytesPool.Get(snappy.MaxEncodedLen(len(b)))

	compressed := snappy.Encode(encBuf, b)
	bytesPool.Put(bytes)

	syncErr := make(chan error)

	segID, err := func() (int, error) {
		l.mu.Lock()
		defer l.mu.Unlock()

		// Make sure the log has not been closed
		select {
		case <-l.closing:
			return -1, ErrWALClosed
		default:
		}

		// roll the segment file if needed
		if err := l.rollSegment(); err != nil {
			return -1, fmt.Errorf("error rolling WAL segment: %v", err)
		}

		// write and sync
		if err := l.currentSegmentWriter.Write(entry.Type(), compressed); err != nil {
			return -1, fmt.Errorf("error writing WAL entry: %v", err)
		}

		select {
		case l.syncWaiters <- syncErr:
		default:
			return -1, fmt.Errorf("error syncing wal")
		}
		l.scheduleSync()

		// Update stats for current segment size
		atomic.StoreInt64(&l.stats.CurrentBytes, int64(l.currentSegmentWriter.size))

		l.lastWriteTime = time.Now().UTC()

		return l.currentSegmentID, nil

	}()

	bytesPool.Put(encBuf)

	if err != nil {
		return segID, err
	}

	// schedule an fsync and wait for it to complete
	return segID, <-syncErr
}

// rollSegment checks if the current segment is due to roll over to a new segment;
// and if so, opens a new segment file for future writes.
func (l *WAL) rollSegment() error {
	if l.currentSegmentWriter == nil || l.currentSegmentWriter.size > DefaultSegmentSize {
		if err := l.newSegmentFile(); err != nil {
			// A drop database or RP call could trigger this error if writes were in-flight
			// when the drop statement executes.
			return fmt.Errorf("error opening new segment file for wal (2): %v", err)
		}
		return nil
	}

	return nil
}

// CloseSegment closes the current segment if it is non-empty and opens a new one.
func (l *WAL) CloseSegment() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.currentSegmentWriter == nil || l.currentSegmentWriter.size > 0 {
		if err := l.newSegmentFile(); err != nil {
			// A drop database or RP call could trigger this error if writes were in-flight
			// when the drop statement executes.
			return fmt.Errorf("error opening new segment file for wal (1): %v", err)
		}
		return nil
	}
	return nil
}

// Delete deletes the given keys, returning the segment ID for the operation.
func (l *WAL) Delete(keys [][]byte) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	entry := &DeleteWALEntry{
		Keys: keys,
	}

	id, err := l.writeToLog(entry)
	if err != nil {
		return -1, err
	}
	return id, nil
}

// DeleteRange deletes the given keys within the given time range,
// returning the segment ID for the operation.
func (l *WAL) DeleteRange(keys [][]byte, min, max int64) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	entry := &DeleteRangeWALEntry{
		Keys: keys,
		Min:  min,
		Max:  max,
	}

	id, err := l.writeToLog(entry)
	if err != nil {
		return -1, err
	}
	return id, nil
}

// Close will finish any flush that is currently in progress and close file handles.
func (l *WAL) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.once.Do(func() {
		// Close, but don't set to nil so future goroutines can still be signaled
		l.traceLogger.Info("Closing WAL file", zap.String("path", l.path))
		close(l.closing)

		if l.currentSegmentWriter != nil {
			l.sync()
			l.currentSegmentWriter.close()
			l.currentSegmentWriter = nil
		}
	})

	return nil
}

// segmentFileNames will return all files that are WAL segment files in sorted order by ascending ID.
func segmentFileNames(dir string) ([]string, error) {
	names, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("%s*.%s", WALFilePrefix, WALFileExtension)))
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// newSegmentFile will close the current segment file and open a new one, updating bookkeeping info on the log.
func (l *WAL) newSegmentFile() error {
	l.currentSegmentID++
	if l.currentSegmentWriter != nil {
		l.sync()

		if err := l.currentSegmentWriter.close(); err != nil {
			return err
		}
		atomic.StoreInt64(&l.stats.OldBytes, int64(l.currentSegmentWriter.size))
	}

	fileName := filepath.Join(l.path, fmt.Sprintf("%s%05d.%s", WALFilePrefix, l.currentSegmentID, WALFileExtension))
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	l.currentSegmentWriter = NewWALSegmentWriter(fd)

	// Reset the current segment size stat
	atomic.StoreInt64(&l.stats.CurrentBytes, 0)

	return nil
}

// WALEntry is record stored in each WAL segment.  Each entry has a type
// and an opaque, type dependent byte slice data attribute.
type WALEntry interface {
	Type() WalEntryType
	Encode(dst []byte) ([]byte, error)
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(b []byte) error
	MarshalSize() int
}

// WriteWALEntry represents a write of points.
type WriteWALEntry struct {
	Values map[string][]Value
	sz     int
}

func (w *WriteWALEntry) MarshalSize() int {
	if w.sz > 0 || len(w.Values) == 0 {
		return w.sz
	}

	encLen := 7 * len(w.Values) // Type (1), Key Length (2), and Count (4) for each key

	// determine required length
	for k, v := range w.Values {
		encLen += len(k)
		if len(v) == 0 {
			return 0
		}

		encLen += 8 * len(v) // timestamps (8)

		switch v[0].(type) {
		case FloatValue, IntegerValue, UnsignedValue:
			encLen += 8 * len(v)
		case BooleanValue:
			encLen += 1 * len(v)
		case StringValue:
			for _, vv := range v {
				str, ok := vv.(StringValue)
				if !ok {
					return 0
				}
				encLen += 4 + len(str.value)
			}
		default:
			return 0
		}
	}

	w.sz = encLen

	return w.sz
}

// Encode converts the WriteWALEntry into a byte stream using dst if it
// is large enough.  If dst is too small, the slice will be grown to fit the
// encoded entry.
func (w *WriteWALEntry) Encode(dst []byte) ([]byte, error) {
	// The entries values are encode as follows:
	//
	// For each key and slice of values, first a 1 byte type for the []Values
	// slice is written.  Following the type, the length and key bytes are written.
	// Following the key, a 4 byte count followed by each value as a 8 byte time
	// and N byte value.  The value is dependent on the type being encoded.  float64,
	// int64, use 8 bytes, boolean uses 1 byte, and string is similar to the key encoding,
	// except that string values have a 4-byte length, and keys only use 2 bytes.
	//
	// This structure is then repeated for each key an value slices.
	//
	// ┌────────────────────────────────────────────────────────────────────┐
	// │                           WriteWALEntry                            │
	// ├──────┬─────────┬────────┬───────┬─────────┬─────────┬───┬──────┬───┤
	// │ Type │ Key Len │   Key  │ Count │  Time   │  Value  │...│ Type │...│
	// │1 byte│ 2 bytes │ N bytes│4 bytes│ 8 bytes │ N bytes │   │1 byte│   │
	// └──────┴─────────┴────────┴───────┴─────────┴─────────┴───┴──────┴───┘

	encLen := w.MarshalSize() // Type (1), Key Length (2), and Count (4) for each key

	// allocate or re-slice to correct size
	if len(dst) < encLen {
		dst = make([]byte, encLen)
	} else {
		dst = dst[:encLen]
	}

	// Finally, encode the entry
	var n int
	var curType byte

	for k, v := range w.Values {
		switch v[0].(type) {
		case FloatValue:
			curType = float64EntryType
		case IntegerValue:
			curType = integerEntryType
		case UnsignedValue:
			curType = unsignedEntryType
		case BooleanValue:
			curType = booleanEntryType
		case StringValue:
			curType = stringEntryType
		default:
			return nil, fmt.Errorf("unsupported value type: %T", v[0])
		}
		dst[n] = curType
		n++

		binary.BigEndian.PutUint16(dst[n:n+2], uint16(len(k)))
		n += 2
		n += copy(dst[n:], k)

		binary.BigEndian.PutUint32(dst[n:n+4], uint32(len(v)))
		n += 4

		for _, vv := range v {
			binary.BigEndian.PutUint64(dst[n:n+8], uint64(vv.UnixNano()))
			n += 8

			switch vv := vv.(type) {
			case FloatValue:
				if curType != float64EntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", v[0].Value(), vv)
				}
				binary.BigEndian.PutUint64(dst[n:n+8], math.Float64bits(vv.value))
				n += 8
			case IntegerValue:
				if curType != integerEntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", v[0].Value(), vv)
				}
				binary.BigEndian.PutUint64(dst[n:n+8], uint64(vv.value))
				n += 8
			case UnsignedValue:
				if curType != unsignedEntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", v[0].Value(), vv)
				}
				binary.BigEndian.PutUint64(dst[n:n+8], uint64(vv.value))
				n += 8
			case BooleanValue:
				if curType != booleanEntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", v[0].Value(), vv)
				}
				if vv.value {
					dst[n] = 1
				} else {
					dst[n] = 0
				}
				n++
			case StringValue:
				if curType != stringEntryType {
					return nil, fmt.Errorf("incorrect value found in %T slice: %T", v[0].Value(), vv)
				}
				binary.BigEndian.PutUint32(dst[n:n+4], uint32(len(vv.value)))
				n += 4
				n += copy(dst[n:], vv.value)
			default:
				return nil, fmt.Errorf("unsupported value found in %T slice: %T", v[0].Value(), vv)
			}
		}
	}

	return dst[:n], nil
}

// MarshalBinary returns a binary representation of the entry in a new byte slice.
func (w *WriteWALEntry) MarshalBinary() ([]byte, error) {
	// Temp buffer to write marshaled points into
	b := make([]byte, w.MarshalSize())
	return w.Encode(b)
}

// UnmarshalBinary deserializes the byte slice into w.
func (w *WriteWALEntry) UnmarshalBinary(b []byte) error {
	var i int
	for i < len(b) {
		typ := b[i]
		i++

		if i+2 > len(b) {
			return ErrWALCorrupt
		}

		length := int(binary.BigEndian.Uint16(b[i : i+2]))
		i += 2

		if i+length > len(b) {
			return ErrWALCorrupt
		}

		k := string(b[i : i+length])
		i += length

		if i+4 > len(b) {
			return ErrWALCorrupt
		}

		nvals := int(binary.BigEndian.Uint32(b[i : i+4]))
		i += 4

		if nvals <= 0 || nvals > len(b) {
			return ErrWALCorrupt
		}

		switch typ {
		case float64EntryType:
			if i+16*nvals > len(b) {
				return ErrWALCorrupt
			}

			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				v := math.Float64frombits((binary.BigEndian.Uint64(b[i : i+8])))
				i += 8
				values = append(values, NewFloatValue(un, v))
			}
			w.Values[k] = values
		case integerEntryType:
			if i+16*nvals > len(b) {
				return ErrWALCorrupt
			}

			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				v := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				values = append(values, NewIntegerValue(un, v))
			}
			w.Values[k] = values

		case unsignedEntryType:
			if i+16*nvals > len(b) {
				return ErrWALCorrupt
			}

			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				v := binary.BigEndian.Uint64(b[i : i+8])
				i += 8
				values = append(values, NewUnsignedValue(un, v))
			}
			w.Values[k] = values

		case booleanEntryType:
			if i+9*nvals > len(b) {
				return ErrWALCorrupt
			}

			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8

				v := b[i]
				i += 1
				if v == 1 {
					values = append(values, NewBooleanValue(un, true))
				} else {
					values = append(values, NewBooleanValue(un, false))
				}
			}
			w.Values[k] = values

		case stringEntryType:
			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				if i+12 > len(b) {
					return ErrWALCorrupt
				}

				un := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8

				length := int(binary.BigEndian.Uint32(b[i : i+4]))
				if i+length > len(b) {
					return ErrWALCorrupt
				}

				i += 4

				if i+length > len(b) {
					return ErrWALCorrupt
				}

				v := string(b[i : i+length])
				i += length
				values = append(values, NewStringValue(un, v))
			}
			w.Values[k] = values

		default:
			return fmt.Errorf("unsupported value type: %#v", typ)
		}
	}
	return nil
}

// Type returns WriteWALEntryType.
func (w *WriteWALEntry) Type() WalEntryType {
	return WriteWALEntryType
}

// DeleteWALEntry represents the deletion of multiple series.
type DeleteWALEntry struct {
	Keys [][]byte
	sz   int
}

// MarshalBinary returns a binary representation of the entry in a new byte slice.
func (w *DeleteWALEntry) MarshalBinary() ([]byte, error) {
	b := make([]byte, w.MarshalSize())
	return w.Encode(b)
}

// UnmarshalBinary deserializes the byte slice into w.
func (w *DeleteWALEntry) UnmarshalBinary(b []byte) error {
	w.Keys = bytes.Split(b, []byte("\n"))
	return nil
}

func (w *DeleteWALEntry) MarshalSize() int {
	if w.sz > 0 || len(w.Keys) == 0 {
		return w.sz
	}

	encLen := len(w.Keys) // newlines
	for _, k := range w.Keys {
		encLen += len(k)
	}

	w.sz = encLen

	return encLen
}

// Encode converts the DeleteWALEntry into a byte slice, appending to dst.
func (w *DeleteWALEntry) Encode(dst []byte) ([]byte, error) {
	sz := w.MarshalSize()

	if len(dst) < sz {
		dst = make([]byte, sz)
	}

	var n int
	for _, k := range w.Keys {
		n += copy(dst[n:], k)
		n += copy(dst[n:], "\n")
	}

	// We return n-1 to strip off the last newline so that unmarshalling the value
	// does not produce an empty string
	return []byte(dst[:n-1]), nil
}

// Type returns DeleteWALEntryType.
func (w *DeleteWALEntry) Type() WalEntryType {
	return DeleteWALEntryType
}

// DeleteRangeWALEntry represents the deletion of multiple series.
type DeleteRangeWALEntry struct {
	Keys     [][]byte
	Min, Max int64
	sz       int
}

// MarshalBinary returns a binary representation of the entry in a new byte slice.
func (w *DeleteRangeWALEntry) MarshalBinary() ([]byte, error) {
	b := make([]byte, w.MarshalSize())
	return w.Encode(b)
}

// UnmarshalBinary deserializes the byte slice into w.
func (w *DeleteRangeWALEntry) UnmarshalBinary(b []byte) error {
	if len(b) < 16 {
		return ErrWALCorrupt
	}

	w.Min = int64(binary.BigEndian.Uint64(b[:8]))
	w.Max = int64(binary.BigEndian.Uint64(b[8:16]))

	i := 16
	for i < len(b) {
		if i+4 > len(b) {
			return ErrWALCorrupt
		}
		sz := int(binary.BigEndian.Uint32(b[i : i+4]))
		i += 4

		if i+sz > len(b) {
			return ErrWALCorrupt
		}
		w.Keys = append(w.Keys, b[i:i+sz])
		i += sz
	}
	return nil
}

func (w *DeleteRangeWALEntry) MarshalSize() int {
	if w.sz > 0 {
		return w.sz
	}

	sz := 16 + len(w.Keys)*4
	for _, k := range w.Keys {
		sz += len(k)
	}

	w.sz = sz

	return sz
}

// Encode converts the DeleteRangeWALEntry into a byte slice, appending to b.
func (w *DeleteRangeWALEntry) Encode(b []byte) ([]byte, error) {
	sz := w.MarshalSize()

	if len(b) < sz {
		b = make([]byte, sz)
	}

	binary.BigEndian.PutUint64(b[:8], uint64(w.Min))
	binary.BigEndian.PutUint64(b[8:16], uint64(w.Max))

	i := 16
	for _, k := range w.Keys {
		binary.BigEndian.PutUint32(b[i:i+4], uint32(len(k)))
		i += 4
		i += copy(b[i:], k)
	}

	return b[:i], nil
}

// Type returns DeleteRangeWALEntryType.
func (w *DeleteRangeWALEntry) Type() WalEntryType {
	return DeleteRangeWALEntryType
}

// WALSegmentWriter writes WAL segments.
type WALSegmentWriter struct {
	bw   *bufio.Writer
	w    io.WriteCloser
	size int
}

// NewWALSegmentWriter returns a new WALSegmentWriter writing to w.
func NewWALSegmentWriter(w io.WriteCloser) *WALSegmentWriter {
	return &WALSegmentWriter{
		bw: bufio.NewWriterSize(w, 16*1024),
		w:  w,
	}
}

func (w *WALSegmentWriter) path() string {
	if f, ok := w.w.(*os.File); ok {
		return f.Name()
	}
	return ""
}

// Write writes entryType and the buffer containing compressed entry data.
func (w *WALSegmentWriter) Write(entryType WalEntryType, compressed []byte) error {
	var buf [5]byte
	buf[0] = byte(entryType)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(compressed)))

	if _, err := w.bw.Write(buf[:]); err != nil {
		return err
	}

	if _, err := w.bw.Write(compressed); err != nil {
		return err
	}

	w.size += len(buf) + len(compressed)

	return nil
}

// Sync flushes the file systems in-memory copy of recently written data to disk,
// if w is writing to an os.File.
func (w *WALSegmentWriter) sync() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}

	if f, ok := w.w.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

func (w *WALSegmentWriter) Flush() error {
	return w.bw.Flush()
}

func (w *WALSegmentWriter) close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	return w.w.Close()
}

// WALSegmentReader reads WAL segments.
type WALSegmentReader struct {
	rc    io.ReadCloser
	r     *bufio.Reader
	entry WALEntry
	n     int64
	err   error
}

// NewWALSegmentReader returns a new WALSegmentReader reading from r.
func NewWALSegmentReader(r io.ReadCloser) *WALSegmentReader {
	return &WALSegmentReader{
		rc: r,
		r:  bufio.NewReader(r),
	}
}

func (r *WALSegmentReader) Reset(rc io.ReadCloser) {
	r.rc = rc
	r.r.Reset(rc)
	r.entry = nil
	r.n = 0
	r.err = nil
}

// Next indicates if there is a value to read.
func (r *WALSegmentReader) Next() bool {
	var nReadOK int

	// read the type and the length of the entry
	var lv [5]byte
	n, err := io.ReadFull(r.r, lv[:])
	if err == io.EOF {
		return false
	}

	if err != nil {
		r.err = err
		// We return true here because we want the client code to call read which
		// will return the this error to be handled.
		return true
	}
	nReadOK += n

	entryType := lv[0]
	length := binary.BigEndian.Uint32(lv[1:5])

	b := *(getBuf(int(length)))
	defer putBuf(&b)

	// read the compressed block and decompress it
	n, err = io.ReadFull(r.r, b[:length])
	if err != nil {
		r.err = err
		return true
	}
	nReadOK += n

	decLen, err := snappy.DecodedLen(b[:length])
	if err != nil {
		r.err = err
		return true
	}
	decBuf := *(getBuf(decLen))
	defer putBuf(&decBuf)

	data, err := snappy.Decode(decBuf, b[:length])
	if err != nil {
		r.err = err
		return true
	}

	// and marshal it and send it to the cache
	switch WalEntryType(entryType) {
	case WriteWALEntryType:
		r.entry = &WriteWALEntry{
			Values: make(map[string][]Value),
		}
	case DeleteWALEntryType:
		r.entry = &DeleteWALEntry{}
	case DeleteRangeWALEntryType:
		r.entry = &DeleteRangeWALEntry{}
	default:
		r.err = fmt.Errorf("unknown wal entry type: %v", entryType)
		return true
	}
	r.err = r.entry.UnmarshalBinary(data)
	if r.err == nil {
		// Read and decode of this entry was successful.
		r.n += int64(nReadOK)
	}

	return true
}

// Read returns the next entry in the reader.
func (r *WALSegmentReader) Read() (WALEntry, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.entry, nil
}

// Count returns the total number of bytes read successfully from the segment, as
// of the last call to Read(). The segment is guaranteed to be valid up to and
// including this number of bytes.
func (r *WALSegmentReader) Count() int64 {
	return r.n
}

// Error returns the last error encountered by the reader.
func (r *WALSegmentReader) Error() error {
	return r.err
}

// Close closes the underlying io.Reader.
func (r *WALSegmentReader) Close() error {
	if r.rc == nil {
		return nil
	}
	err := r.rc.Close()
	r.rc = nil
	return err
}

// idFromFileName parses the segment file ID from its name.
func idFromFileName(name string) (int, error) {
	parts := strings.Split(filepath.Base(name), ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("file %s has wrong name format to have an id", name)
	}

	id, err := strconv.ParseUint(parts[0][1:], 10, 32)

	return int(id), err
}
