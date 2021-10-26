package durablequeue

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Possible errors returned by a queue.
var (
	ErrNotOpen      = fmt.Errorf("queue not open")
	ErrQueueFull    = fmt.Errorf("queue is full")
	ErrQueueBlocked = fmt.Errorf("queue is blocked")
	ErrSegmentFull  = fmt.Errorf("segment is full")
)

const (
	DefaultSegmentSize = 10 * 1024 * 1024
	footerSize         = 8
)

// MaxWritesPending is the number of writes that can be pending at any given time.
const MaxWritesPending = 1024

// Queue is a bounded, disk-backed, append-only type that combines Queue and
// log semantics.  byte slices can be appended and read back in-order.
// The Queue maintains a pointer to the current Head
// byte slice and can re-read from the Head until it has been advanced.
//
// Internally, the Queue writes byte slices to multiple segment files so
// that disk space can be reclaimed. When a segment file is larger than
// the max segment size, a new file is created.   Segments are removed
// after their Head pointer has advanced past the last entry.  The first
// segment is the head, and the last segment is the tail.  Reads are from
// the head segment and writes tail segment.
//
// queues can have a max size configured such that when the size of all
// segments on disk exceeds the size, write will fail.
//
// 	┌─────┐
// 	│Head │
// 	├─────┘
// 	│
// 	▼
// 	┌─────────────────┐ ┌─────────────────┐┌─────────────────┐
// 	│Segment 1 - 10MB │ │Segment 2 - 10MB ││Segment 3 - 10MB │
// 	└─────────────────┘ └─────────────────┘└─────────────────┘
// 	                                                         ▲
// 	                                                         │
// 	                                                         │
// 	                                                    ┌─────┐
// 	                                                    │Tail │
// 	                                                    └─────┘
type Queue struct {
	mu sync.RWMutex

	// Directory to create segments
	dir string

	// The head and tail segments.  Reads are from the beginning of head,
	// writes are appended to the tail.
	head, tail *segment

	// The maximum size in bytes of a segment file before a new one should be created
	maxSegmentSize int64

	// The maximum size allowed in bytes of all segments before writes will return
	// an error
	maxSize        int64
	queueTotalSize *SharedCount

	// The segments that exist on disk
	segments segments
	// verifyBlockFn is used to verify a block within a segment contains valid data.
	verifyBlockFn func([]byte) error

	// Channel used for throttling append requests.
	appendCh chan struct{}

	// scratch is a temporary in-memory space for staging writes
	scratch bytes.Buffer

	logger *zap.Logger
}

// SharedCount manages an integer value, which can be read/written concurrently.
type SharedCount struct {
	value int64
}

// Add adds delta to the counter value.
func (sc *SharedCount) Add(delta int64) {
	atomic.AddInt64(&sc.value, delta)
}

// Value returns the current value value.
func (sc *SharedCount) Value() int64 {
	return atomic.LoadInt64(&sc.value)
}

type QueuePos struct {
	Head string
	Tail string
}

type segments []*segment

func (a segments) Len() int           { return len(a) }
func (a segments) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a segments) Less(i, j int) bool { return a[i].id < a[j].id }

// NewQueue create a Queue that will store segments in dir and that will consume no more than maxSize on disk.
func NewQueue(dir string, maxSize int64, maxSegmentSize int64, queueTotalSize *SharedCount, depth int, verifyBlockFn func([]byte) error) (*Queue, error) {
	if maxSize < 2*maxSegmentSize {
		return nil, fmt.Errorf("max queue size %d too small: must be at least twice the max segment size %d", maxSize, maxSegmentSize)
	}

	return &Queue{
		dir:            dir,
		maxSegmentSize: maxSegmentSize,
		maxSize:        maxSize,
		queueTotalSize: queueTotalSize,
		segments:       segments{},
		appendCh:       make(chan struct{}, depth),
		logger:         zap.NewNop(),
		verifyBlockFn:  verifyBlockFn,
	}, nil
}

// WithLogger sets the internal logger to the logger passed in.
func (l *Queue) WithLogger(log *zap.Logger) {
	l.logger = log
}

// SetMaxSize updates the max queue size to the passed-in value.
//
// Max queue size must be at least twice the current max segment size, otherwise an error will be returned.
//
// If the new value is smaller than the amount of data currently in the queue,
// writes will be rejected until the queue drains to below the new maximum.
func (l *Queue) SetMaxSize(maxSize int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if maxSize < 2*l.maxSegmentSize {
		return fmt.Errorf("queue size %d too small: must be at least %d bytes", maxSize, 2*l.maxSegmentSize)
	}

	l.maxSize = maxSize
	return nil
}

// Open opens the queue for reading and writing.
func (l *Queue) Open() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	segments, err := l.loadSegments()
	if err != nil {
		return err
	}
	l.segments = segments

	if len(l.segments) == 0 {
		if err := l.addSegment(); err != nil {
			return err
		}
	}

	l.head = l.segments[0]
	l.tail = l.segments[len(l.segments)-1]

	// If the Head has been fully advanced and the segment size is modified,
	// existing segments an get stuck and never allow clients to advance further.
	// This advances the segment if the current Head is already at the end.
	_, err = l.head.current()
	if err == io.EOF {
		return l.trimHead(false)
	}

	l.queueTotalSize.Add(l.diskUsage())

	return nil
}

// Close stops the queue for reading and writing.
func (l *Queue) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.close(); err != nil {
			return err
		}
	}
	l.head = nil
	l.tail = nil
	l.segments = nil
	return nil
}

// Remove removes all underlying file-based resources for the queue.
// It is an error to call this on an open queue.
func (l *Queue) Remove() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head != nil || l.tail != nil || l.segments != nil {
		return fmt.Errorf("queue is open")
	}

	return os.RemoveAll(l.dir)
}

// RemoveSegments removes all segments for the queue.
// It is an error to call this on an open queue.
func (l *Queue) RemoveSegments() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head != nil || l.tail != nil || l.segments != nil {
		return fmt.Errorf("queue is open")
	}

	files, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return err
	}

	for _, segment := range files {
		// Segments should be files.  Skip anything that is a dir.
		if segment.IsDir() {
			continue
		}

		// Segments file names are all numeric
		_, err := strconv.ParseUint(segment.Name(), 10, 64)
		if err != nil {
			continue
		}

		path := filepath.Join(l.dir, segment.Name())
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

// SetMaxSegmentSize updates the max segment size for new and existing (tail) segments.
//
// The new segment size must be less than half the current max queue size, otherwise an error will be returned.
func (l *Queue) SetMaxSegmentSize(size int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if 2*size > l.maxSize {
		return fmt.Errorf("segment size %d is too large: must be at most half of max queue size %d", size, l.maxSize)
	}

	l.maxSegmentSize = size

	for _, s := range l.segments {
		s.SetMaxSegmentSize(size)
	}

	if l.tail.diskUsage() >= l.maxSegmentSize {
		if err := l.addSegment(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Queue) PurgeOlderThan(when time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.segments) == 0 {
		return nil
	}

	cutoff := when.Truncate(time.Second)
	for {
		mod, err := l.head.lastModified()
		if err != nil {
			return err
		}

		if mod.After(cutoff) || mod.Equal(cutoff) {
			return nil
		}

		// If this is the last segment, first append a new one allowing
		// trimming to proceed.
		if len(l.segments) == 1 {
			if err := l.addSegment(); err != nil {
				return err
			}
		}

		if err := l.trimHead(false); err != nil {
			return err
		}
	}
}

// LastModified returns the last time the queue was modified.
func (l *Queue) LastModified() (time.Time, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.tail != nil {
		return l.tail.lastModified()
	}
	return time.Time{}.UTC(), nil
}

func (l *Queue) Position() (*QueuePos, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	qp := &QueuePos{}
	if l.head != nil {
		qp.Head = fmt.Sprintf("%s:%d", l.head.path, l.head.pos)
	}
	if l.tail != nil {
		qp.Tail = fmt.Sprintf("%s:%d", l.tail.path, l.tail.filePos())
	}
	return qp, nil
}

// Empty returns whether the queue's underlying segments are empty.
func (l *Queue) Empty() bool {
	l.mu.RLock()
	empty := l.tail.empty()
	l.mu.RUnlock()
	return empty
}

// TotalBytes returns the number of bytes of data remaining in the queue.
func (l *Queue) TotalBytes() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var n int64
	for _, s := range l.segments {
		n += s.totalBytes()
	}
	return n
}

// Dir returns the directory associated with the queue.
func (l *Queue) Dir() string {
	return l.dir
}

// diskUsage returns the total size on disk used by the Queue.
func (l *Queue) diskUsage() int64 {
	var size int64
	for _, s := range l.segments {
		size += s.diskUsage()
	}
	return size
}

// addSegment creates a new empty segment file.
func (l *Queue) addSegment() error {
	nextID, err := l.nextSegmentID()
	if err != nil {
		return err
	}

	segment, err := newSegment(filepath.Join(l.dir, strconv.FormatUint(nextID, 10)), l.maxSegmentSize, l.verifyBlockFn)
	if err != nil {
		return err
	}

	l.tail = segment
	l.segments = append(l.segments, segment)
	return nil
}

// loadSegments loads all segments on disk.
func (l *Queue) loadSegments() (segments, error) {
	var ss segments

	files, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return ss, err
	}

	for _, segment := range files {
		// Segments should be files.  Skip anything that is a dir.
		if segment.IsDir() {
			continue
		}

		// Segments file names are all numeric
		_, err := strconv.ParseUint(segment.Name(), 10, 64)
		if err != nil {
			continue
		}

		path := filepath.Join(l.dir, segment.Name())
		l.logger.Info("Loading", zap.String("path", path))
		segment, err := newSegment(path, l.maxSegmentSize, l.verifyBlockFn)
		if err != nil {
			return ss, err
		}

		// Segment repair can leave files that have no data to process.  If this happens,
		// the queue can get stuck.  We need to remove any empty segments to prevent this.
		if segment.empty() {
			if err := segment.close(); err != nil {
				return ss, err
			}
			if err := os.Remove(segment.path); err != nil {
				return ss, err
			}
			continue
		}

		ss = append(ss, segment)
	}
	sort.Sort(ss)

	return ss, nil
}

// nextSegmentID returns the next segment ID that is free.
func (l *Queue) nextSegmentID() (uint64, error) {
	segments, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return 0, err
	}

	var maxID uint64
	for _, segment := range segments {
		// Segments should be files.  Skip anything that is not a dir.
		if segment.IsDir() {
			continue
		}

		// Segments file names are all numeric
		segmentID, err := strconv.ParseUint(segment.Name(), 10, 64)
		if err != nil {
			continue
		}

		if segmentID > maxID {
			maxID = segmentID
		}
	}

	return maxID + 1, nil
}

// TotalSegments determines how many segments the current Queue is
// utilising. Empty segments at the end of the Queue are not counted.
func (l *Queue) TotalSegments() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	n := len(l.segments)

	// Check last segment's size and if empty, ignore it.
	if n > 0 && l.segments[n-1].empty() {
		n--
	}
	return n
}

// Append appends a byte slice to the end of the queue.
func (l *Queue) Append(b []byte) error {
	// Only allow append if there aren't too many concurrent requests.
	select {
	case l.appendCh <- struct{}{}:
		defer func() { <-l.appendCh }()
	default:
		return ErrQueueBlocked
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.tail == nil {
		return ErrNotOpen
	}

	if l.queueTotalSize.Value()+int64(len(b)) > l.maxSize {
		return ErrQueueFull
	}

	// Append the entry to the tail, if the segment is full,
	// try to create new segment and retry the append
	bytesWritten, err := l.tail.append(b, &l.scratch)
	if err == ErrSegmentFull {
		if err := l.addSegment(); err != nil {
			return err
		}
		bytesWritten, err = l.tail.append(b, &l.scratch)
	}

	if err == nil {
		l.queueTotalSize.Add(bytesWritten)
	}

	return err
}

// Current returns the current byte slice at the Head of the queue.
func (l *Queue) Current() ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.head == nil {
		return nil, ErrNotOpen
	}

	return l.head.current()
}

// Peek returns the next n byte slices at the Head of the queue.
func (l *Queue) PeekN(n int) ([][]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.head == nil {
		return nil, ErrNotOpen
	}

	return l.head.peek(n)
}

func (l *Queue) NewScanner() (Scanner, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.head == nil {
		return nil, ErrNotOpen
	}

	ss, err := l.head.newScanner()
	if err != nil {
		return nil, err
	}

	return &queueScanner{q: l, ss: ss}, nil
}

// Advance moves the Head point to the next byte slice in the queue.
func (l *Queue) Advance() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.head == nil {
		return ErrNotOpen
	}

	err := l.head.advance()
	if err == io.EOF {
		if err := l.trimHead(false); err != nil {
			return err
		}
	}

	return nil
}

func (l *Queue) trimHead(force bool) error {
	// If there is only one segment, but it's full, add a new segment so
	// so the Head segment can be trimmed.
	if len(l.segments) == 1 && l.head.full() || force {
		if err := l.addSegment(); err != nil {
			return err
		}
	}

	var bytesDeleted int64

	if len(l.segments) > 1 {
		l.segments = l.segments[1:]

		bytesDeleted = l.head.diskUsage()

		err := l.head.close()
		if err != nil {
			l.logger.Info("Failed to close segment file.", zap.Error(err), zap.String("path", l.head.path))
		}

		err = os.Remove(l.head.path)
		if err != nil {
			l.logger.Info("Failed to remove segment file.", zap.Error(err), zap.String("path", l.head.path))
		}
		l.head = l.segments[0]
	}

	l.queueTotalSize.Add(-bytesDeleted)

	return nil
}

// Segment is a Queue using a single file.  The structure of a segment is a series
// lengths + block with a single footer point to the position in the segment of the
// current Head block.
//
// 	┌──────────────────────────┐ ┌──────────────────────────┐ ┌────────────┐
// 	│         Block 1          │ │         Block 2          │ │   Footer   │
// 	└──────────────────────────┘ └──────────────────────────┘ └────────────┘
// 	┌────────────┐┌────────────┐ ┌────────────┐┌────────────┐ ┌────────────┐
// 	│Block 1 Len ││Block 1 Body│ │Block 2 Len ││Block 2 Body│ │Head Offset │
// 	│  8 bytes   ││  N bytes   │ │  8 bytes   ││  N bytes   │ │  8 bytes   │
// 	└────────────┘└────────────┘ └────────────┘└────────────┘ └────────────┘
//
// The footer holds the pointer to the Head entry at the end of the segment to allow writes
// to seek to the end and write sequentially (vs having to seek back to the beginning of
// the segment to update the Head pointer).  Reads must seek to the end then back into the
// segment offset stored in the footer.
//
// Segments store arbitrary byte slices and leave the serialization to the caller.  Segments
// are created with a max size and will block writes when the segment is full.
type segment struct {
	mu      sync.RWMutex
	size    int64    // Size of the entire segment file, including previously read blocks and the footer.
	maxSize int64    // Maximum size of the segment file.
	pos     int64    // Position (offset) of current block.
	file    *os.File // Underlying file representing the segment.

	// verifyBlockFn is used to verify a block within a segment contains valid data.
	verifyBlockFn func([]byte) error

	path string // Path of underlying file as passed to newSegment.
	id   uint64 // Segment ID as encoded in the file name of the segment.
}

func newSegment(path string, maxSize int64, verifyBlockFn func([]byte) error) (*segment, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	id, err := strconv.ParseUint(filepath.Base(f.Name()), 10, 64)
	if err != nil {
		return nil, err
	}

	stats, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	// If the segment file is larger than the default segment size then we
	// should consider a size under the file size valid.
	if maxSize < stats.Size() {
		maxSize = stats.Size()
	}

	s := &segment{
		id:            id,
		file:          f,
		path:          path,
		size:          stats.Size(),
		maxSize:       maxSize,
		verifyBlockFn: verifyBlockFn,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.open(); err != nil {
		return nil, err
	}

	return s, nil
}

func (l *segment) open() error {
	// If it's a new segment then write the location of the current record in this segment
	if l.size == 0 {
		l.pos = 0

		if err := l.writeUint64(uint64(l.pos)); err != nil {
			return err
		}

		if err := l.file.Sync(); err != nil {
			return err
		}

		l.size = footerSize

		return nil
	}

	// Existing segment so read the current position and the size of the current block
	if err := l.seekEnd(-footerSize); err != nil {
		return err
	}

	pos, err := l.readUint64()
	if err != nil {
		return err
	}

	// Check if the segment is corrupted. A segment is corrupted if the position
	// value doesn't point to a valid location in the segment.
	if pos > uint64(l.size)-footerSize {
		if pos, err = l.repair(); err != nil {
			return err
		}
	}

	// Move to the part of the segment where the next block to read is.
	// If we had to repair the segment, this will be the beginning of the
	// segment.
	l.pos = int64(pos)
	if err := l.seekToCurrent(); err != nil {
		return err
	}

	// If we're at the end of the segment, we're done.
	if l.pos >= l.size-footerSize {
		return nil
	}

	// Read the current block size.
	currentSize, err := l.readUint64()
	if err != nil {
		return err
	}

	// Is the size reported larger than what could possibly left? If so, it's corrupted.
	if int64(currentSize) > l.size-footerSize-l.pos || int64(currentSize) < 0 {
		if _, err = l.repair(); err != nil {
			return err
		}
		return l.open()
	}

	// Extract the block data.
	block := make([]byte, int64(currentSize))
	if err := l.readBytes(block); err != nil {
		if _, err = l.repair(); err != nil {
			return err
		}
		return l.open()
	}

	// Seek back to the beginning of the block data.
	if err := l.seek(l.pos + 8); err != nil {
		return err
	}

	// Verify the block data.
	if err := l.verifyBlockFn(block); err != nil {
		// Verification of the block failed... This means we need to
		// truncate the segment.
		if err = l.file.Truncate(l.pos); err != nil {
			return err
		}

		if err := l.seek(l.pos); err != nil {
			return err
		}

		// Start from the beginning of the segment again.
		// TODO(edd): This could be improved to point at the last block in
		// the segment...
		if err = l.writeUint64(0); err != nil {
			return err
		}

		if err = l.file.Sync(); err != nil {
			return err
		}
		l.size = l.pos + footerSize

		// re-open the segment.
		return l.open()
	}
	return nil
}

// full returns true if the segment can no longer accept writes.
func (l *segment) full() bool {
	l.mu.RLock()
	b := l.size >= l.maxSize
	l.mu.RUnlock()
	return b
}

// repair fixes a corrupted segment.
//
// A segment is either corrupted within a block, or the eight byte position
// value in the footer is itself corrupted (more unlikely).
//
// A corrupted segment is corrected by walking the segment until the corrupted
// block is located, which is then truncated. Regardless of which way the
// segment is corrupted, a new position pointing to the beginning of the
// segment, is written into the footer.
//
// repair returns the new position value that the segment should continue to be
// processed from.
//
// Note: if a block has been corrupted internally, e.g., due to a bit flip,
// repair will not be able to detect this.
func (l *segment) repair() (pos uint64, err error) {
	// Seek to beginning of segment.
	if err = l.seek(0); err != nil {
		return pos, err
	}

	var (
		recordSize uint64
		offset     int64
		truncate   bool
	)

	// Seek through each block in the segment until we have either read up to
	// the footer, or we reach the end of the segment prematurely.
	for {
		offset = l.filePos()

		if offset == l.size-footerSize {
			// Segment looks good as we've successfully reached the end. Segment
			// position in footer must be bad. This is a very unlikely case,
			// since it means only the last eight bytes of an otherwise
			// acceptable segment were corrupted.
			break
		}

		// Read the record size.
		if recordSize, err = l.readUint64(); err != nil {
			truncate = true
			break
		}

		// Skip the rest of the record. If we go beyond the end of the segment,
		// or we hit an error, then we will truncate.
		if _, err = l.file.Seek(int64(recordSize), io.SeekCurrent); err != nil || l.filePos() > l.size-footerSize {
			truncate = true
			break
		}
	}

	if truncate {
		// We reached the end of the segment before we were supposed to, which
		// means the last block is short. Truncate the corrupted last block
		// onwards.
		if err = l.file.Truncate(offset); err != nil {
			return pos, err
		}
	}

	// Set the position as the beginning of the segment, so that the entire
	// segment will be replayed.
	if err = l.seek(offset); err != nil {
		return pos, err
	}

	if err = l.writeUint64(pos); err != nil {
		return pos, err
	}

	if err = l.file.Sync(); err != nil {
		return pos, err
	}

	l.size = offset + 8
	return pos, err // Current implementation always returns 0 position.
}

// append adds byte slice to the end of segment.
func (l *segment) append(b []byte, scratch *bytes.Buffer) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return 0, ErrNotOpen
	}

	if l.size > l.maxSize {
		return 0, ErrSegmentFull
	}

	if err := l.seekEnd(-footerSize); err != nil {
		return 0, err
	}

	// TODO(SGC): error condition: (len(b) + l.size) > l.maxSize == true; scanner.Next will fail reading last block and get stuck

	// If the size of this block is over the max size of the file,
	// update the max file size so we don't get an error indicating
	// the size is invalid when reading it back.
	l64 := int64(len(b))
	if l64 > l.maxSize {
		l.maxSize = l64
	}

	// Construct the segment entry in memory first so it can be
	// written to file atomically.
	scratch.Reset()

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(len(b)))
	if _, err := scratch.Write(buf[:]); err != nil {
		return 0, err
	}

	if _, err := scratch.Write(b); err != nil {
		return 0, err
	}

	binary.BigEndian.PutUint64(buf[:], uint64(l.pos))
	if _, err := scratch.Write(buf[:]); err != nil {
		return 0, err
	}

	// Write the segment entry to disk.
	if err := l.writeBytes(scratch.Bytes()); err != nil {
		return 0, err
	}

	if err := l.file.Sync(); err != nil {
		return 0, err
	}

	bytesWritten := int64(len(b)) + 8 // uint64 for length
	l.size += bytesWritten

	return bytesWritten, nil
}

// empty returns whether there are any remaining blocks to be read from the segment.
func (l *segment) empty() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return int64(l.pos) == l.size-footerSize
}

// current returns the byte slice that the current segment points to.
func (l *segment) current() ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if int64(l.pos) == l.size-footerSize {
		return nil, io.EOF
	}

	if err := l.seekToCurrent(); err != nil {
		return nil, err
	}

	// read the record size
	sz, err := l.readUint64()
	if err != nil {
		return nil, err
	}

	if sz > uint64(l.maxSize) {
		return nil, fmt.Errorf("record size out of range: max %d: got %d", l.maxSize, sz)
	}

	b := make([]byte, sz)
	if err := l.readBytes(b); err != nil {
		return nil, err
	}

	return b, nil
}

func (l *segment) peek(n int) ([][]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if int64(l.pos) == l.size-footerSize {
		return nil, io.EOF
	}

	if err := l.seekToCurrent(); err != nil {
		return nil, err
	}

	var blocks [][]byte
	pos := l.pos
	for i := 0; i < n; i++ {

		if int64(pos) == l.size-footerSize {
			return blocks, nil
		}

		// read the record size
		sz, err := l.readUint64()
		if err == io.EOF {
			return blocks, nil
		} else if err != nil {
			return nil, err
		}
		pos += 8

		if sz == 0 {
			continue
		}

		if sz > uint64(l.maxSize) {
			return nil, fmt.Errorf("record size out of range: max %d: got %d", l.maxSize, sz)
		}

		pos += int64(sz)

		b := make([]byte, sz)
		if err := l.readBytes(b); err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	return blocks, nil
}

// advance advances the current value pointer.
//
// Usually a scanner should be used instead of calling advance
func (l *segment) advance() error {
	if err := l.seekToCurrent(); err != nil {
		return err
	}
	sz, err := l.readUint64()
	if err != nil {
		return err
	}
	currentSize := int64(sz)
	return l.advanceTo(l.pos + currentSize + 8)
}

// advanceTo advances the segment to the position specified by pos
func (l *segment) advanceTo(pos int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return ErrNotOpen
	}

	if pos < l.pos {
		return fmt.Errorf("attempt to unread queue from %d to %d", l.pos, pos)
	}

	l.pos = pos

	// If we're attempting to move beyond the end of the file, can't advance
	if int64(pos) > l.size-footerSize {
		return io.EOF
	}

	if err := l.seekEnd(-footerSize); err != nil {
		return err
	}

	if err := l.writeUint64(uint64(pos)); err != nil {
		return err
	}

	if err := l.file.Sync(); err != nil {
		return err
	}

	if err := l.seekToCurrent(); err != nil {
		return err
	}

	_, err := l.readUint64()
	if err != nil {
		return err
	}

	if int64(l.pos) == l.size-footerSize {
		return io.EOF
	}

	return nil
}

// totalBytes returns the number of bytes remaining in the segment file, excluding the footer.
func (l *segment) totalBytes() (n int64) {
	l.mu.RLock()
	n = l.size - int64(l.pos) - footerSize
	l.mu.RUnlock()
	return
}

func (l *segment) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	err := l.file.Close()
	l.file = nil
	return err
}

func (l *segment) lastModified() (time.Time, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.file == nil {
		return time.Time{}, ErrNotOpen
	}

	stats, err := os.Stat(l.file.Name())
	if err != nil {
		return time.Time{}, err
	}
	return stats.ModTime().UTC(), nil
}

func (l *segment) diskUsage() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.size
}

func (l *segment) SetMaxSegmentSize(size int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.maxSize = size
}

func (l *segment) seekToCurrent() error {
	return l.seek(int64(l.pos))
}

func (l *segment) seek(pos int64) error {
	n, err := l.file.Seek(pos, io.SeekStart)
	if err != nil {
		return err
	}

	if n != pos {
		return fmt.Errorf("bad seek. exp %v, got %v", pos, n)
	}

	return nil
}

func (l *segment) seekEnd(pos int64) error {
	_, err := l.file.Seek(pos, io.SeekEnd)
	return err
}

func (l *segment) filePos() int64 {
	n, _ := l.file.Seek(0, io.SeekCurrent)
	return n
}

func (l *segment) readUint64() (uint64, error) {
	var b [8]byte
	if err := l.readBytes(b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

func (l *segment) writeUint64(sz uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sz)
	return l.writeBytes(buf[:])
}

func (l *segment) writeBytes(b []byte) error {
	n, err := l.file.Write(b)
	if err != nil {
		return err
	}

	if n != len(b) {
		return fmt.Errorf("short write. got %d, exp %d", n, len(b))
	}
	return nil
}

func (l *segment) readBytes(b []byte) error {
	n, err := l.file.Read(b)
	if err != nil {
		return err
	}

	if n != len(b) {
		return fmt.Errorf("bad read. exp %v, got %v", len(b), n)
	}
	return nil
}
