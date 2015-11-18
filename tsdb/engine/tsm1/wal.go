package tsm1

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

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

	writeBufLen = 128 << 10 // 128kb
)

// walEntry is a byte written to a wal segment file that indicates what the following compressed block contains
type walEntryType byte

const (
	WriteWALEntryType  walEntryType = 0x01
	DeleteWALEntryType walEntryType = 0x02
)

var ErrWALClosed = fmt.Errorf("WAL closed")

var bufPool sync.Pool

type WAL struct {
	path string

	// write variables
	writeLock          sync.Mutex
	currentSegmentID   int
	currentSegmentFile *os.File
	currentSegmentSize int

	// cache and flush variables
	closing chan struct{}

	// WALOutput is the writer used by the logger.
	LogOutput io.Writer
	logger    *log.Logger

	// SegmentSize is the file size at which a segment file will be rotated
	SegmentSize int

	// MaxMemorySizeThreshold specifies the limit at which writes to the WAL should be rejected
	MaxMemorySizeThreshold int

	// LoggingEnabled specifies if detailed logs should be output
	LoggingEnabled bool
}

func NewWAL(path string) *WAL {
	return &WAL{
		path: path,

		// these options should be overriden by any options in the config
		LogOutput:              os.Stderr,
		SegmentSize:            DefaultSegmentSize,
		MaxMemorySizeThreshold: tsdb.DefaultMaxMemorySizeThreshold,
		logger:                 log.New(os.Stderr, "[tsm1wal] ", log.LstdFlags),
		closing:                make(chan struct{}),
	}
}

// Path returns the path the log was initialized with.
func (l *WAL) Path() string { return l.path }

// Open opens and initializes the Log. Will recover from previous unclosed shutdowns
func (l *WAL) Open() error {
	if l.LoggingEnabled {
		l.logger.Printf("tsm1 WAL starting with %d max memory size threshold\n", l.MaxMemorySizeThreshold)
		l.logger.Printf("tsm1 WAL writing to %s\n", l.path)
	}
	if err := os.MkdirAll(l.path, 0777); err != nil {
		return err
	}

	l.closing = make(chan struct{})

	return nil
}

func (l *WAL) WritePoints(points []models.Point) error {
	entry := &WriteWALEntry{
		Points: points,
	}

	if err := l.writeToLog(entry); err != nil {
		return err
	}

	return nil
}

func (l *WAL) writeToLog(entry WALEntry) error {
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
			return fmt.Errorf("error opening new segment file for wal: %v", err)
		}
	}

	bytes, err := entry.MarshalBinary()
	if err != nil {
		return fmt.Errorf("error marshaling WAL entry: %v", err)
	}

	b := append([]byte{byte(entry.Type())})
	b = append(b, u32tob(uint32(len(bytes)))...)
	b = append(b, bytes...)

	if _, err := l.currentSegmentFile.Write(b); err != nil {
		return fmt.Errorf("error writing to WAL: %v", err)
	}

	l.currentSegmentSize += 5 + len(b)

	return l.currentSegmentFile.Sync()
}

func (l *WAL) DeleteMeasurement(measurement string, keys []string) error {
	d := &deleteData{MeasurementName: measurement, Keys: keys}
	err := l.writeDeleteEntry(d)
	if err != nil {
		return err
	}

	return nil
}

func (l *WAL) writeDeleteEntry(d *deleteData) error {
	panic("not implemented")
	// js, err := json.Marshal(d)
	// if err != nil {
	// 	return err
	// }
	// data := snappy.Encode(nil, js)
	// return l.writeToLog(deleteEntry, data)
}

func (l *WAL) DeleteSeries(keys []string) error {
	return l.writeDeleteEntry(&deleteData{Keys: keys})
}

// Close will finish any flush that is currently in process and close file handles
func (l *WAL) Close() error {
	l.writeLock.Lock()
	defer l.writeLock.Unlock()

	// Close, but don't set to nil so future goroutines can still be signaled
	close(l.closing)

	if l.currentSegmentFile != nil {
		l.currentSegmentFile.Close()
		l.currentSegmentFile = nil
	}

	return nil
}

// segmentFileNames will return all files that are WAL segment files in sorted order by ascending ID
func (l *WAL) segmentFileNames() ([]string, error) {
	names, err := filepath.Glob(filepath.Join(l.path, fmt.Sprintf("%s*.%s", WALFilePrefix, WALFileExtension)))
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// newSegmentFile will close the current segment file and open a new one, updating bookkeeping info on the log
func (l *WAL) newSegmentFile() error {
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

// WALEntry is record stored in each WAL segment.  Each entry has a type
// and an opaque, type dependent byte slice data attribute.
type WALEntry interface {
	Type() walEntryType
	Encode(dst []byte) ([]byte, error)
	MarshalBinary() ([]byte, error)
	UnmarshlBinary(b []byte) error
}

// WriteWALEntry represents a write of points.
type WriteWALEntry struct {
	Points []models.Point
}

func (w *WriteWALEntry) Encode(dst []byte) ([]byte, error) {
	var n int
	for _, p := range w.Points {
		// Marshaling points to bytes is relatively expensive, only do it once
		bytes, err := p.MarshalBinary()
		if err != nil {
			return nil, err
		}

		// Make sure we have enough space in our buf before copying.  If not,
		// grow the buf.
		if len(bytes)+4 > len(dst)-n {
			grow := make([]byte, writeBufLen)
			dst = append(dst, grow...)
		}
		n += copy(dst[n:], u32tob(uint32(len(bytes))))
		n += copy(dst[n:], bytes)
	}

	return dst[:n], nil
}

func (w *WriteWALEntry) MarshalBinary() ([]byte, error) {
	// Temp buffer to write marshaled points into
	b := make([]byte, writeBufLen)
	return w.Encode(b)
}

func (w *WriteWALEntry) UnmarshlBinary(b []byte) error {
	var i int

	for i < len(b) {
		length := int(btou32(b[i : i+4]))
		i += 4

		point, err := models.NewPointFromBytes(b[i : i+length])
		if err != nil {
			return err
		}
		i += length
		w.Points = append(w.Points, point)
	}
	return nil
}

func (w *WriteWALEntry) Type() walEntryType {
	return WriteWALEntryType
}

// DeleteWALEntry represents the deletion of multiple series.
type DeleteWALEntry struct {
	Keys []string
}

func (w *DeleteWALEntry) MarshalBinary() ([]byte, error) {
	b := make([]byte, writeBufLen)
	return w.Encode(b)
}

func (w *DeleteWALEntry) UnmarshlBinary(b []byte) error {
	w.Keys = strings.Split(string(b), "\n")
	return nil
}

func (w *DeleteWALEntry) Encode(dst []byte) ([]byte, error) {
	var n int
	for _, k := range w.Keys {
		if len(dst)+1 > len(dst)-n {
			grow := make([]byte, writeBufLen)
			dst = append(dst, grow...)
		}

		n += copy(dst[n:], k)
		n += copy(dst[n:], "\n")
	}

	// We return n-1 to strip off the last newline so that unmarshalling the value
	// does not produce an empty string
	return []byte(dst[:n-1]), nil
}

func (w *DeleteWALEntry) Type() walEntryType {
	return DeleteWALEntryType
}

// WALSegmentWriter writes WAL segments.
type WALSegmentWriter struct {
	w    io.WriteCloser
	size int
	b    []byte
}

func NewWALSegmentWriter(w io.WriteCloser) *WALSegmentWriter {
	return &WALSegmentWriter{
		w: w,
		b: make([]byte, writeBufLen),
	}
}

func (w *WALSegmentWriter) Write(e WALEntry) error {
	bytes := getBuf(writeBufLen)
	defer putBuf(bytes)

	b, err := e.Encode(bytes)
	if err != nil {
		return err
	}

	// Temp buf for snappy compression
	cb := getBuf(len(b))
	defer putBuf(cb)

	compressed := snappy.Encode(cb, b)

	// Temp buf for file writing
	wb := getBuf(len(compressed) + 5)
	defer putBuf(wb)

	n := copy(wb, []byte{byte(e.Type())})
	n += copy(wb[n:], u32tob(uint32(len(compressed))))
	n += copy(wb[n:], compressed)

	if _, err := w.w.Write(wb[:n]); err != nil {
		return fmt.Errorf("error writing to WAL: %v", err)
	}

	w.size += n

	// TODO: Move this up to the WAL
	return w.Sync()
}

// Sync flushes the file systems in-memory copy of recently written data to disk.
func (w *WALSegmentWriter) Sync() error {
	if f, ok := w.w.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

func (w *WALSegmentWriter) Size() int {
	return w.size
}

func (w *WALSegmentWriter) Close() error {
	return w.w.Close()
}

// WALSegmentReader reads WAL segments.
type WALSegmentReader struct {
	r     io.ReadCloser
	entry WALEntry
	err   error
}

func NewWALSegmentReader(r io.ReadCloser) *WALSegmentReader {
	return &WALSegmentReader{
		r: r,
	}
}

// Next indicates if there is a value to read
func (r *WALSegmentReader) Next() bool {
	b := getBuf(writeBufLen)
	defer putBuf(b)

	// read the type and the length of the entry
	_, err := io.ReadFull(r.r, b[:5])
	if err == io.EOF {
		return false
	}

	if err != nil {
		r.err = err
		// We return true here because we want the client code to call read which
		// will return the this error to be handled.
		return true
	}

	entryType := b[0]
	length := btou32(b[1:5])

	// read the compressed block and decompress it
	if int(length) > len(b) {
		b = make([]byte, length)
	}

	_, err = io.ReadFull(r.r, b[:length])
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		r.err = err
		return true
	}

	if err != nil {
		r.err = err
		return true
	}

	buf := getBuf(writeBufLen)
	defer putBuf(buf)

	data, err := snappy.Decode(buf, b[:length])
	if err != nil {
		r.err = err
		return true
	}

	// and marshal it and send it to the cache
	switch walEntryType(entryType) {
	case WriteWALEntryType:
		r.entry = &WriteWALEntry{}
	case DeleteWALEntryType:
		r.entry = &DeleteWALEntry{}
	default:
		r.err = fmt.Errorf("unknown wal entry type: %v", entryType)
		return true
	}
	r.err = r.entry.UnmarshlBinary(data)

	return true
}

func (r *WALSegmentReader) Read() (WALEntry, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.entry, nil
}

func (r *WALSegmentReader) Error() error {
	return r.err
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

// getBuf returns a buffer with length size from the buffer pool.
func getBuf(size int) []byte {
	x := bufPool.Get()
	if x == nil {
		return make([]byte, size)
	}
	buf := x.([]byte)
	if cap(buf) < size {
		return make([]byte, size)
	}
	return buf[:size]
}

// putBuf returns a buffer to the pool.
func putBuf(buf []byte) {
	bufPool.Put(buf)
}
