package tsm1

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

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
	t.tombstoner = NewTombstoner(t.Path(), index.MaybeContainsKey)

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
		// TODO(jeff): maybe we need to do batches of prefixes
		if ts.Prefix {
			t.index.DeletePrefix(ts.Key, ts.Min, ts.Max, nil)
			return nil
		}

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

// MaybeContainsValue returns true if key and time might exists in this file. This function
// could return true even though the actual point does not exist. For example, the key may
// exist in this file, but not have a point exactly at time t.
func (t *TSMReader) MaybeContainsValue(key []byte, ts int64) bool {
	return t.index.MaybeContainsValue(key, ts)
}

// DeleteRange removes the given points for keys between minTime and maxTime. The series
// keys passed in must be sorted.
func (t *TSMReader) DeleteRange(keys [][]byte, minTime, maxTime int64) error {
	if !t.index.DeleteRange(keys, minTime, maxTime) {
		return nil
	}
	if err := t.tombstoner.AddRange(keys, minTime, maxTime); err != nil {
		return err
	}
	if err := t.tombstoner.Flush(); err != nil {
		return err
	}
	return nil
}

// DeletePrefix removes the given points for keys beginning with prefix. It calls dead with
// any keys that became dead as a result of this call.
func (t *TSMReader) DeletePrefix(prefix []byte, minTime, maxTime int64, dead func([]byte)) error {
	if !t.index.DeletePrefix(prefix, minTime, maxTime, dead) {
		return nil
	}
	if err := t.tombstoner.AddPrefixRange(prefix, minTime, maxTime); err != nil {
		return err
	}
	if err := t.tombstoner.Flush(); err != nil {
		return err
	}
	return nil
}

// Delete deletes blocks indicated by keys.
func (t *TSMReader) Delete(keys [][]byte) error {
	if !t.index.Delete(keys) {
		return nil
	}
	if err := t.tombstoner.Add(keys); err != nil {
		return err
	}
	if err := t.tombstoner.Flush(); err != nil {
		return err
	}
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
