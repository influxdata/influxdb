package tsm1

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/influxdata/platform/pkg/file"
	"go.uber.org/zap"
)

// ErrFileInUse is returned when attempting to remove or close a TSM file that is still being used.
var ErrFileInUse = fmt.Errorf("file still in use")

// TSMReader is a reader for a TSM file.
type TSMReader struct {
	// refs is the count of active references to this reader.
	refs   int64
	refsWG sync.WaitGroup

	logger          *zap.Logger
	madviseWillNeed bool // Hint to the kernel with MADV_WILLNEED.
	mu              sync.RWMutex

	// accessor provides access and decoding of blocks for the reader.
	accessor blockAccessor

	// index is the index of all blocks.
	index TSMIndex

	// tombstoner ensures tombstoned keys are not available by the index.
	tombstoner *Tombstoner

	// size is the size of the file on disk.
	size int64

	// lastModified is the last time this file was modified on disk
	lastModified int64

	// deleteMu limits concurrent deletes
	deleteMu sync.Mutex
}

type tsmReaderOption func(*TSMReader)

// WithMadviseWillNeed is an option for specifying whether to provide a MADV_WILL need hint to the kernel.
var WithMadviseWillNeed = func(willNeed bool) tsmReaderOption {
	return func(r *TSMReader) {
		r.madviseWillNeed = willNeed
	}
}

var WithTSMReaderLogger = func(logger *zap.Logger) tsmReaderOption {
	return func(r *TSMReader) {
		r.logger = logger
	}
}

// NewTSMReader returns a new TSMReader from the given file.
func NewTSMReader(f *os.File, options ...tsmReaderOption) (*TSMReader, error) {
	t := &TSMReader{
		logger: zap.NewNop(),
	}
	for _, option := range options {
		option(t)
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	t.size = stat.Size()
	t.lastModified = stat.ModTime().UnixNano()
	t.accessor = &mmapAccessor{
		logger:       t.logger,
		f:            f,
		mmapWillNeed: t.madviseWillNeed,
	}

	index, err := t.accessor.init()
	if err != nil {
		return nil, err
	}

	t.index = index
	t.tombstoner = NewTombstoner(t.Path(), index.ContainsKey)

	if err := t.applyTombstones(); err != nil {
		return nil, err
	}

	return t, nil
}

// WithObserver sets the observer for the TSM reader.
func (t *TSMReader) WithObserver(obs FileStoreObserver) {
	if obs == nil {
		obs = noFileStoreObserver{}
	}
	t.tombstoner.WithObserver(obs)
}

func (t *TSMReader) applyTombstones() error {
	var cur, prev Tombstone
	batch := make([][]byte, 0, 4096)

	if err := t.tombstoner.Walk(func(ts Tombstone) error {
		cur = ts
		if len(batch) > 0 {
			if prev.Min != cur.Min || prev.Max != cur.Max {
				t.index.DeleteRange(batch, prev.Min, prev.Max)
				batch = batch[:0]
			}
		}

		// Copy the tombstone key and re-use the buffers to avoid allocations
		n := len(batch)
		batch = batch[:n+1]
		if cap(batch[n]) < len(ts.Key) {
			batch[n] = make([]byte, len(ts.Key))
		} else {
			batch[n] = batch[n][:len(ts.Key)]
		}
		copy(batch[n], ts.Key)

		if len(batch) >= 4096 {
			t.index.DeleteRange(batch, prev.Min, prev.Max)
			batch = batch[:0]
		}

		prev = ts
		return nil
	}); err != nil {
		return fmt.Errorf("init: read tombstones: %v", err)
	}

	if len(batch) > 0 {
		t.index.DeleteRange(batch, cur.Min, cur.Max)
	}
	return nil
}

func (t *TSMReader) Free() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.accessor.free()
}

// Path returns the path of the file the TSMReader was initialized with.
func (t *TSMReader) Path() string {
	t.mu.RLock()
	p := t.accessor.path()
	t.mu.RUnlock()
	return p
}

// ReadAt returns the values corresponding to the given index entry.
func (t *TSMReader) ReadAt(entry *IndexEntry, vals []Value) ([]Value, error) {
	t.mu.RLock()
	v, err := t.accessor.readBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// Read returns the values corresponding to the block at the given key and timestamp.
func (t *TSMReader) Read(key []byte, timestamp int64) ([]Value, error) {
	t.mu.RLock()
	v, err := t.accessor.read(key, timestamp)
	t.mu.RUnlock()
	return v, err
}

// ReadAll returns all values for a key in all blocks.
func (t *TSMReader) ReadAll(key []byte) ([]Value, error) {
	t.mu.RLock()
	v, err := t.accessor.readAll(key)
	t.mu.RUnlock()
	return v, err
}

func (t *TSMReader) ReadBytes(e *IndexEntry, b []byte) (uint32, []byte, error) {
	t.mu.RLock()
	n, v, err := t.accessor.readBytes(e, b)
	t.mu.RUnlock()
	return n, v, err
}

// Type returns the type of values stored at the given key.
func (t *TSMReader) Type(key []byte) (byte, error) {
	return t.index.Type(key)
}

// MeasurementStats returns the on-disk measurement stats for this file, if available.
func (t *TSMReader) MeasurementStats() (MeasurementStats, error) {
	f, err := os.Open(StatsFilename(t.Path()))
	if os.IsNotExist(err) {
		return make(MeasurementStats), nil
	} else if err != nil {
		return nil, err
	}
	defer f.Close()

	stats := make(MeasurementStats)
	if _, err := stats.ReadFrom(bufio.NewReader(f)); err != nil {
		return nil, err
	}
	return stats, err
}

// Close closes the TSMReader.
func (t *TSMReader) Close() error {
	t.refsWG.Wait()

	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.accessor.close(); err != nil {
		return err
	}

	return t.index.Close()
}

// Ref records a usage of this TSMReader.  If there are active references
// when the reader is closed or removed, the reader will remain open until
// there are no more references.
func (t *TSMReader) Ref() {
	atomic.AddInt64(&t.refs, 1)
	t.refsWG.Add(1)
}

// Unref removes a usage record of this TSMReader.  If the Reader was closed
// by another goroutine while there were active references, the file will
// be closed and remove
func (t *TSMReader) Unref() {
	atomic.AddInt64(&t.refs, -1)
	t.refsWG.Done()
}

// InUse returns whether the TSMReader currently has any active references.
func (t *TSMReader) InUse() bool {
	refs := atomic.LoadInt64(&t.refs)
	return refs > 0
}

// Remove removes any underlying files stored on disk for this reader.
func (t *TSMReader) Remove() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.remove()
}

// Rename renames the underlying file to the new path.
func (t *TSMReader) Rename(path string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.accessor.rename(path)
}

// Remove removes any underlying files stored on disk for this reader.
func (t *TSMReader) remove() error {
	path := t.accessor.path()

	if t.InUse() {
		return ErrFileInUse
	}

	if path != "" {
		if err := os.RemoveAll(path); err != nil {
			return err
		} else if err := os.RemoveAll(StatsFilename(path)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	if err := t.tombstoner.Delete(); err != nil {
		return err
	}
	return nil
}

// Contains returns whether the given key is present in the index.
func (t *TSMReader) Contains(key []byte) bool {
	return t.index.Contains(key)
}

// ContainsValue returns true if key and time might exists in this file.  This function could
// return true even though the actual point does not exist.  For example, the key may
// exist in this file, but not have a point exactly at time t.
func (t *TSMReader) ContainsValue(key []byte, ts int64) bool {
	return t.index.ContainsValue(key, ts)
}

// DeleteRange removes the given points for keys between minTime and maxTime.   The series
// keys passed in must be sorted.
func (t *TSMReader) DeleteRange(keys [][]byte, minTime, maxTime int64) error {
	if len(keys) == 0 {
		return nil
	}

	batch := t.BatchDelete()
	if err := batch.DeleteRange(keys, minTime, maxTime); err != nil {
		batch.Rollback()
		return err
	}
	return batch.Commit()
}

// Delete deletes blocks indicated by keys.
func (t *TSMReader) Delete(keys [][]byte) error {
	if err := t.tombstoner.Add(keys); err != nil {
		return err
	}

	if err := t.tombstoner.Flush(); err != nil {
		return err
	}

	t.index.Delete(keys)
	return nil
}

// Iterator returns an iterator over the keys starting at the provided key. You must
// call Next before calling any of the accessors.
func (t *TSMReader) Iterator(key []byte) TSMIterator {
	return t.index.Iterator(key)
}

// OverlapsTimeRange returns true if the time range of the file intersect min and max.
func (t *TSMReader) OverlapsTimeRange(min, max int64) bool {
	return t.index.OverlapsTimeRange(min, max)
}

// OverlapsKeyRange returns true if the key range of the file intersect min and max.
func (t *TSMReader) OverlapsKeyRange(min, max []byte) bool {
	return t.index.OverlapsKeyRange(min, max)
}

// TimeRange returns the min and max time across all keys in the file.
func (t *TSMReader) TimeRange() (int64, int64) {
	return t.index.TimeRange()
}

// KeyRange returns the min and max key across all keys in the file.
func (t *TSMReader) KeyRange() ([]byte, []byte) {
	return t.index.KeyRange()
}

// KeyCount returns the count of unique keys in the TSMReader.
func (t *TSMReader) KeyCount() int {
	return t.index.KeyCount()
}

// ReadEntries reads the index entries for key into entries.
func (t *TSMReader) ReadEntries(key []byte, entries []IndexEntry) ([]IndexEntry, error) {
	return t.index.ReadEntries(key, entries)
}

// IndexSize returns the size of the index in bytes.
func (t *TSMReader) IndexSize() uint32 {
	return t.index.Size()
}

// Size returns the size of the underlying file in bytes.
func (t *TSMReader) Size() uint32 {
	t.mu.RLock()
	size := t.size
	t.mu.RUnlock()
	return uint32(size)
}

// LastModified returns the last time the underlying file was modified.
func (t *TSMReader) LastModified() int64 {
	t.mu.RLock()
	lm := t.lastModified
	for _, ts := range t.tombstoner.TombstoneFiles() {
		if ts.LastModified > lm {
			lm = ts.LastModified
		}
	}
	t.mu.RUnlock()
	return lm
}

// HasTombstones return true if there are any tombstone entries recorded.
func (t *TSMReader) HasTombstones() bool {
	t.mu.RLock()
	b := t.tombstoner.HasTombstones()
	t.mu.RUnlock()
	return b
}

// TombstoneFiles returns any tombstone files associated with this TSM file.
func (t *TSMReader) TombstoneFiles() []FileStat {
	t.mu.RLock()
	fs := t.tombstoner.TombstoneFiles()
	t.mu.RUnlock()
	return fs
}

// TombstoneRange returns ranges of time that are deleted for the given key.
func (t *TSMReader) TombstoneRange(key []byte, buf []TimeRange) []TimeRange {
	t.mu.RLock()
	tr := t.index.TombstoneRange(key, buf)
	t.mu.RUnlock()
	return tr
}

// Stats returns the FileStat for the TSMReader's underlying file.
func (t *TSMReader) Stats() FileStat {
	minTime, maxTime := t.index.TimeRange()
	minKey, maxKey := t.index.KeyRange()
	return FileStat{
		Path:         t.Path(),
		Size:         t.Size(),
		LastModified: t.LastModified(),
		MinTime:      minTime,
		MaxTime:      maxTime,
		MinKey:       minKey,
		MaxKey:       maxKey,
		HasTombstone: t.tombstoner.HasTombstones(),
	}
}

// BlockIterator returns a BlockIterator for the underlying TSM file.
func (t *TSMReader) BlockIterator() *BlockIterator {
	t.mu.RLock()
	iter := t.index.Iterator(nil)
	t.mu.RUnlock()

	return &BlockIterator{
		r:    t,
		iter: iter,
	}
}

type BatchDeleter interface {
	DeleteRange(keys [][]byte, min, max int64) error
	Commit() error
	Rollback() error
}

type batchDelete struct {
	r *TSMReader
}

func (b *batchDelete) DeleteRange(keys [][]byte, minTime, maxTime int64) error {
	if len(keys) == 0 {
		return nil
	}

	// If the keys can't exist in this TSM file, skip it.
	minKey, maxKey := keys[0], keys[len(keys)-1]
	if !b.r.index.OverlapsKeyRange(minKey, maxKey) {
		return nil
	}

	// If the timerange can't exist in this TSM file, skip it.
	if !b.r.index.OverlapsTimeRange(minTime, maxTime) {
		return nil
	}

	if err := b.r.tombstoner.AddRange(keys, minTime, maxTime); err != nil {
		return err
	}

	return nil
}

func (b *batchDelete) Commit() error {
	defer b.r.deleteMu.Unlock()
	if err := b.r.tombstoner.Flush(); err != nil {
		return err
	}

	return b.r.applyTombstones()
}

func (b *batchDelete) Rollback() error {
	defer b.r.deleteMu.Unlock()
	return b.r.tombstoner.Rollback()
}

// BatchDelete returns a BatchDeleter.  Only a single goroutine may run a BatchDelete at a time.
// Callers must either Commit or Rollback the operation.
func (r *TSMReader) BatchDelete() BatchDeleter {
	r.deleteMu.Lock()
	return &batchDelete{r: r}
}

type BatchDeleters []BatchDeleter

func (a BatchDeleters) DeleteRange(keys [][]byte, min, max int64) error {
	errC := make(chan error, len(a))
	for _, b := range a {
		go func(b BatchDeleter) { errC <- b.DeleteRange(keys, min, max) }(b)
	}

	var err error
	for i := 0; i < len(a); i++ {
		dErr := <-errC
		if dErr != nil {
			err = dErr
		}
	}
	return err
}

func (a BatchDeleters) Commit() error {
	errC := make(chan error, len(a))
	for _, b := range a {
		go func(b BatchDeleter) { errC <- b.Commit() }(b)
	}

	var err error
	for i := 0; i < len(a); i++ {
		dErr := <-errC
		if dErr != nil {
			err = dErr
		}
	}
	return err
}

func (a BatchDeleters) Rollback() error {
	errC := make(chan error, len(a))
	for _, b := range a {
		go func(b BatchDeleter) { errC <- b.Rollback() }(b)
	}

	var err error
	for i := 0; i < len(a); i++ {
		dErr := <-errC
		if dErr != nil {
			err = dErr
		}
	}
	return err
}

// mmapAccess is mmap based block accessor.  It access blocks through an
// MMAP file interface.
type mmapAccessor struct {
	accessCount uint64 // Counter incremented everytime the mmapAccessor is accessed
	freeCount   uint64 // Counter to determine whether the accessor can free its resources

	logger       *zap.Logger
	mmapWillNeed bool // If true then mmap advise value MADV_WILLNEED will be provided the kernel for b.

	mu sync.RWMutex
	b  []byte
	f  *os.File

	index *indirectIndex
}

func (m *mmapAccessor) init() (*indirectIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := verifyVersion(m.f); err != nil {
		return nil, err
	}

	var err error

	if _, err := m.f.Seek(0, 0); err != nil {
		return nil, err
	}

	stat, err := m.f.Stat()
	if err != nil {
		return nil, err
	}

	m.b, err = mmap(m.f, 0, int(stat.Size()))
	if err != nil {
		return nil, err
	}
	if len(m.b) < 8 {
		return nil, fmt.Errorf("mmapAccessor: byte slice too small for indirectIndex")
	}

	// Hint to the kernel that we will be reading the file.  It would be better to hint
	// that we will be reading the index section, but that's not been
	// implemented as yet.
	if m.mmapWillNeed {
		if err := madviseWillNeed(m.b); err != nil {
			return nil, err
		}
	}

	indexOfsPos := len(m.b) - 8
	indexStart := binary.BigEndian.Uint64(m.b[indexOfsPos : indexOfsPos+8])
	if indexStart >= uint64(indexOfsPos) {
		return nil, fmt.Errorf("mmapAccessor: invalid indexStart")
	}

	m.index = NewIndirectIndex()
	if err := m.index.UnmarshalBinary(m.b[indexStart:indexOfsPos]); err != nil {
		return nil, err
	}
	m.index.logger = m.logger

	// Allow resources to be freed immediately if requested
	m.incAccess()
	atomic.StoreUint64(&m.freeCount, 1)

	return m.index, nil
}

func (m *mmapAccessor) free() error {
	accessCount := atomic.LoadUint64(&m.accessCount)
	freeCount := atomic.LoadUint64(&m.freeCount)

	// Already freed everything.
	if freeCount == 0 && accessCount == 0 {
		return nil
	}

	// Were there accesses after the last time we tried to free?
	// If so, don't free anything and record the access count that we
	// see now for the next check.
	if accessCount != freeCount {
		atomic.StoreUint64(&m.freeCount, accessCount)
		return nil
	}

	// Reset both counters to zero to indicate that we have freed everything.
	atomic.StoreUint64(&m.accessCount, 0)
	atomic.StoreUint64(&m.freeCount, 0)

	m.mu.RLock()
	defer m.mu.RUnlock()

	return madviseDontNeed(m.b)
}

func (m *mmapAccessor) incAccess() {
	atomic.AddUint64(&m.accessCount, 1)
}

func (m *mmapAccessor) rename(path string) error {
	m.incAccess()

	m.mu.Lock()
	defer m.mu.Unlock()

	err := munmap(m.b)
	if err != nil {
		return err
	}

	if err := m.f.Close(); err != nil {
		return err
	}

	if err := file.RenameFile(m.f.Name(), path); err != nil {
		return err
	}

	m.f, err = os.Open(path)
	if err != nil {
		return err
	}

	if _, err := m.f.Seek(0, 0); err != nil {
		return err
	}

	stat, err := m.f.Stat()
	if err != nil {
		return err
	}

	m.b, err = mmap(m.f, 0, int(stat.Size()))
	if err != nil {
		return err
	}

	if m.mmapWillNeed {
		return madviseWillNeed(m.b)
	}
	return nil
}

func (m *mmapAccessor) read(key []byte, timestamp int64) ([]Value, error) {
	entry := m.index.Entry(key, timestamp)
	if entry == nil {
		return nil, nil
	}

	return m.readBlock(entry, nil)
}

func (m *mmapAccessor) readBlock(entry *IndexEntry, values []Value) ([]Value, error) {
	m.incAccess()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		return nil, ErrTSMClosed
	}
	//TODO: Validate checksum
	var err error
	values, err = DecodeBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (m *mmapAccessor) readBytes(entry *IndexEntry, b []byte) (uint32, []byte, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return 0, nil, ErrTSMClosed
	}

	// return the bytes after the 4 byte checksum
	crc, block := binary.BigEndian.Uint32(m.b[entry.Offset:entry.Offset+4]), m.b[entry.Offset+4:entry.Offset+int64(entry.Size)]
	m.mu.RUnlock()

	return crc, block, nil
}

// readAll returns all values for a key in all blocks.
func (m *mmapAccessor) readAll(key []byte) ([]Value, error) {
	m.incAccess()

	blocks, err := m.index.ReadEntries(key, nil)
	if len(blocks) == 0 || err != nil {
		return nil, err
	}

	tombstones := m.index.TombstoneRange(key, nil)

	m.mu.RLock()
	defer m.mu.RUnlock()

	var temp []Value
	var values []Value
	for _, block := range blocks {
		var skip bool
		for _, t := range tombstones {
			// Should we skip this block because it contains points that have been deleted
			if t.Min <= block.MinTime && t.Max >= block.MaxTime {
				skip = true
				break
			}
		}

		if skip {
			continue
		}
		//TODO: Validate checksum
		temp = temp[:0]
		// The +4 is the 4 byte checksum length
		temp, err = DecodeBlock(m.b[block.Offset+4:block.Offset+int64(block.Size)], temp)
		if err != nil {
			return nil, err
		}

		// Filter out any values that were deleted
		for _, t := range tombstones {
			temp = Values(temp).Exclude(t.Min, t.Max)
		}

		values = append(values, temp...)
	}

	return values, nil
}

func (m *mmapAccessor) path() string {
	m.mu.RLock()
	path := m.f.Name()
	m.mu.RUnlock()
	return path
}

func (m *mmapAccessor) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.b == nil {
		return nil
	}

	err := munmap(m.b)
	if err != nil {
		return err
	}

	m.b = nil
	return m.f.Close()
}

type indexEntries struct {
	Type    byte
	entries []IndexEntry
}

func (a *indexEntries) Len() int      { return len(a.entries) }
func (a *indexEntries) Swap(i, j int) { a.entries[i], a.entries[j] = a.entries[j], a.entries[i] }
func (a *indexEntries) Less(i, j int) bool {
	return a.entries[i].MinTime < a.entries[j].MinTime
}

func (a *indexEntries) MarshalBinary() ([]byte, error) {
	buf := make([]byte, len(a.entries)*indexEntrySize)

	for i, entry := range a.entries {
		entry.AppendTo(buf[indexEntrySize*i:])
	}

	return buf, nil
}

func (a *indexEntries) WriteTo(w io.Writer) (total int64, err error) {
	var buf [indexEntrySize]byte
	var n int

	for _, entry := range a.entries {
		entry.AppendTo(buf[:])
		n, err = w.Write(buf[:])
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	return total, nil
}
