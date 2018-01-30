package tsm1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/influxdata/influxdb/pkg/bytesutil"
)

// ErrFileInUse is returned when attempting to remove or close a TSM file that is still being used.
var ErrFileInUse = fmt.Errorf("file still in use")

// nilOffset is the value written to the offsets to indicate that position is deleted.  The value is the max
// uint32 which is an invalid position.  We don't use 0 as 0 is actually a valid position.
var nilOffset = []byte{255, 255, 255, 255}

// TSMReader is a reader for a TSM file.
type TSMReader struct {
	// refs is the count of active references to this reader.
	refs int64

	mu sync.RWMutex

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

// TSMIndex represent the index section of a TSM file.  The index records all
// blocks, their locations, sizes, min and max times.
type TSMIndex interface {
	// Delete removes the given keys from the index.
	Delete(keys [][]byte)

	// DeleteRange removes the given keys with data between minTime and maxTime from the index.
	DeleteRange(keys [][]byte, minTime, maxTime int64)

	// ContainsKey returns true if the given key may exist in the index.  This func is faster than
	// Contains but, may return false positives.
	ContainsKey(key []byte) bool

	// Contains return true if the given key exists in the index.
	Contains(key []byte) bool

	// ContainsValue returns true if key and time might exist in this file.  This function could
	// return true even though the actual point does not exists.  For example, the key may
	// exist in this file, but not have a point exactly at time t.
	ContainsValue(key []byte, timestamp int64) bool

	// Entries returns all index entries for a key.
	Entries(key []byte) []IndexEntry

	// ReadEntries reads the index entries for key into entries.
	ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry

	// Entry returns the index entry for the specified key and timestamp.  If no entry
	// matches the key and timestamp, nil is returned.
	Entry(key []byte, timestamp int64) *IndexEntry

	// Key returns the key in the index at the given position, using entries to avoid allocations.
	Key(index int, entries *[]IndexEntry) ([]byte, byte, []IndexEntry)

	// KeyAt returns the key in the index at the given position.
	KeyAt(index int) ([]byte, byte)

	// KeyCount returns the count of unique keys in the index.
	KeyCount() int

	// Seek returns the position in the index where key <= value in the index.
	Seek(key []byte) int

	// OverlapsTimeRange returns true if the time range of the file intersect min and max.
	OverlapsTimeRange(min, max int64) bool

	// OverlapsKeyRange returns true if the min and max keys of the file overlap the arguments min and max.
	OverlapsKeyRange(min, max []byte) bool

	// Size returns the size of the current index in bytes.
	Size() uint32

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (int64, int64)

	// TombstoneRange returns ranges of time that are deleted for the given key.
	TombstoneRange(key []byte) []TimeRange

	// KeyRange returns the min and max keys in the file.
	KeyRange() ([]byte, []byte)

	// Type returns the block type of the values stored for the key.  Returns one of
	// BlockFloat64, BlockInt64, BlockBool, BlockString.  If key does not exist,
	// an error is returned.
	Type(key []byte) (byte, error)

	// UnmarshalBinary populates an index from an encoded byte slice
	// representation of an index.
	UnmarshalBinary(b []byte) error

	// Close closes the index and releases any resources.
	Close() error
}

// BlockIterator allows iterating over each block in a TSM file in order.  It provides
// raw access to the block bytes without decoding them.
type BlockIterator struct {
	r *TSMReader

	// i is the current key index
	i int

	// n is the total number of keys
	n int

	key     []byte
	cache   []IndexEntry
	entries []IndexEntry
	err     error
	typ     byte
}

// PeekNext returns the next key to be iterated or an empty string.
func (b *BlockIterator) PeekNext() []byte {
	if len(b.entries) > 1 {
		return b.key
	} else if b.n-b.i > 1 {
		key, _ := b.r.KeyAt(b.i + 1)
		return key
	}
	return nil
}

// Next returns true if there are more blocks to iterate through.
func (b *BlockIterator) Next() bool {
	if b.err != nil {
		return false
	}

	if b.n-b.i == 0 && len(b.entries) == 0 {
		return false
	}

	if len(b.entries) > 0 {
		b.entries = b.entries[1:]
		if len(b.entries) > 0 {
			return true
		}
	}

	if b.n-b.i > 0 {
		b.key, b.typ, b.entries = b.r.Key(b.i, &b.cache)
		b.i++

		// If there were deletes on the TSMReader, then our index is now off and we
		// can't proceed.  What we just read may not actually the next block.
		if b.n != b.r.KeyCount() {
			b.err = fmt.Errorf("delete during iteration")
			return false
		}

		if len(b.entries) > 0 {
			return true
		}
	}

	return false
}

// Read reads information about the next block to be iterated.
func (b *BlockIterator) Read() (key []byte, minTime int64, maxTime int64, typ byte, checksum uint32, buf []byte, err error) {
	if b.err != nil {
		return nil, 0, 0, 0, 0, nil, b.err
	}
	checksum, buf, err = b.r.ReadBytes(&b.entries[0], nil)
	if err != nil {
		return nil, 0, 0, 0, 0, nil, err
	}
	return b.key, b.entries[0].MinTime, b.entries[0].MaxTime, b.typ, checksum, buf, err
}

// Err returns any errors encounter during iteration.
func (b *BlockIterator) Err() error {
	return b.err
}

// blockAccessor abstracts a method of accessing blocks from a
// TSM file.
type blockAccessor interface {
	init() (*indirectIndex, error)
	read(key []byte, timestamp int64) ([]Value, error)
	readAll(key []byte) ([]Value, error)
	readBlock(entry *IndexEntry, values []Value) ([]Value, error)
	readFloatBlock(entry *IndexEntry, values *[]FloatValue) ([]FloatValue, error)
	readIntegerBlock(entry *IndexEntry, values *[]IntegerValue) ([]IntegerValue, error)
	readUnsignedBlock(entry *IndexEntry, values *[]UnsignedValue) ([]UnsignedValue, error)
	readStringBlock(entry *IndexEntry, values *[]StringValue) ([]StringValue, error)
	readBooleanBlock(entry *IndexEntry, values *[]BooleanValue) ([]BooleanValue, error)
	readBytes(entry *IndexEntry, buf []byte) (uint32, []byte, error)
	rename(path string) error
	path() string
	close() error
	free() error
}

// NewTSMReader returns a new TSMReader from the given file.
func NewTSMReader(f *os.File) (*TSMReader, error) {
	t := &TSMReader{}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	t.size = stat.Size()
	t.lastModified = stat.ModTime().UnixNano()
	t.accessor = &mmapAccessor{
		f: f,
	}

	index, err := t.accessor.init()
	if err != nil {
		return nil, err
	}

	t.index = index
	t.tombstoner = &Tombstoner{Path: t.Path(), FilterFn: index.ContainsKey}

	if err := t.applyTombstones(); err != nil {
		return nil, err
	}

	return t, nil
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

// Key returns the key and the underlying entry at the numeric index.
func (t *TSMReader) Key(index int, entries *[]IndexEntry) ([]byte, byte, []IndexEntry) {
	return t.index.Key(index, entries)
}

// KeyAt returns the key and key type at position idx in the index.
func (t *TSMReader) KeyAt(idx int) ([]byte, byte) {
	return t.index.KeyAt(idx)
}

func (t *TSMReader) Seek(key []byte) int {
	return t.index.Seek(key)
}

// ReadAt returns the values corresponding to the given index entry.
func (t *TSMReader) ReadAt(entry *IndexEntry, vals []Value) ([]Value, error) {
	t.mu.RLock()
	v, err := t.accessor.readBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadFloatBlockAt returns the float values corresponding to the given index entry.
func (t *TSMReader) ReadFloatBlockAt(entry *IndexEntry, vals *[]FloatValue) ([]FloatValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readFloatBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadIntegerBlockAt returns the integer values corresponding to the given index entry.
func (t *TSMReader) ReadIntegerBlockAt(entry *IndexEntry, vals *[]IntegerValue) ([]IntegerValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readIntegerBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadUnsignedBlockAt returns the unsigned integer values corresponding to the given index entry.
func (t *TSMReader) ReadUnsignedBlockAt(entry *IndexEntry, vals *[]UnsignedValue) ([]UnsignedValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readUnsignedBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadStringBlockAt returns the string values corresponding to the given index entry.
func (t *TSMReader) ReadStringBlockAt(entry *IndexEntry, vals *[]StringValue) ([]StringValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readStringBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadBooleanBlockAt returns the boolean values corresponding to the given index entry.
func (t *TSMReader) ReadBooleanBlockAt(entry *IndexEntry, vals *[]BooleanValue) ([]BooleanValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readBooleanBlock(entry, vals)
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

// Close closes the TSMReader.
func (t *TSMReader) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.InUse() {
		return ErrFileInUse
	}

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
}

// Unref removes a usage record of this TSMReader.  If the Reader was closed
// by another goroutine while there were active references, the file will
// be closed and remove
func (t *TSMReader) Unref() {
	atomic.AddInt64(&t.refs, -1)
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
		os.RemoveAll(path)
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

// Entries returns all index entries for key.
func (t *TSMReader) Entries(key []byte) []IndexEntry {
	return t.index.Entries(key)
}

// ReadEntries reads the index entries for key into entries.
func (t *TSMReader) ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry {
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
func (t *TSMReader) TombstoneRange(key []byte) []TimeRange {
	t.mu.RLock()
	tr := t.index.TombstoneRange(key)
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
	return &BlockIterator{
		r: t,
		n: t.index.KeyCount(),
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

// indirectIndex is a TSMIndex that uses a raw byte slice representation of an index.  This
// implementation can be used for indexes that may be MMAPed into memory.
type indirectIndex struct {
	mu sync.RWMutex

	// indirectIndex works a follows.  Assuming we have an index structure in memory as
	// the diagram below:
	//
	// ┌────────────────────────────────────────────────────────────────────┐
	// │                               Index                                │
	// ├─┬──────────────────────┬──┬───────────────────────┬───┬────────────┘
	// │0│                      │62│                       │145│
	// ├─┴───────┬─────────┬────┼──┴──────┬─────────┬──────┼───┴─────┬──────┐
	// │Key 1 Len│   Key   │... │Key 2 Len│  Key 2  │ ...  │  Key 3  │ ...  │
	// │ 2 bytes │ N bytes │    │ 2 bytes │ N bytes │      │ 2 bytes │      │
	// └─────────┴─────────┴────┴─────────┴─────────┴──────┴─────────┴──────┘

	// We would build an `offsets` slices where each element pointers to the byte location
	// for the first key in the index slice.

	// ┌────────────────────────────────────────────────────────────────────┐
	// │                              Offsets                               │
	// ├────┬────┬────┬─────────────────────────────────────────────────────┘
	// │ 0  │ 62 │145 │
	// └────┴────┴────┘

	// Using this offset slice we can find `Key 2` by doing a binary search
	// over the offsets slice.  Instead of comparing the value in the offsets
	// (e.g. `62`), we use that as an index into the underlying index to
	// retrieve the key at postion `62` and perform our comparisons with that.

	// When we have identified the correct position in the index for a given
	// key, we could perform another binary search or a linear scan.  This
	// should be fast as well since each index entry is 28 bytes and all
	// contiguous in memory.  The current implementation uses a linear scan since the
	// number of block entries is expected to be < 100 per key.

	// b is the underlying index byte slice.  This could be a copy on the heap or an MMAP
	// slice reference
	b []byte

	// offsets contains the positions in b for each key.  It points to the 2 byte length of
	// key.
	offsets []byte

	// minKey, maxKey are the minium and maximum (lexicographically sorted) contained in the
	// file
	minKey, maxKey []byte

	// minTime, maxTime are the minimum and maximum times contained in the file across all
	// series.
	minTime, maxTime int64

	// tombstones contains only the tombstoned keys with subset of time values deleted.  An
	// entry would exist here if a subset of the points for a key were deleted and the file
	// had not be re-compacted to remove the points on disk.
	tombstones map[string][]TimeRange
}

// TimeRange holds a min and max timestamp.
type TimeRange struct {
	Min, Max int64
}

func (t TimeRange) Overlaps(min, max int64) bool {
	return t.Min <= max && t.Max >= min
}

// NewIndirectIndex returns a new indirect index.
func NewIndirectIndex() *indirectIndex {
	return &indirectIndex{
		tombstones: make(map[string][]TimeRange),
	}
}

func (d *indirectIndex) offset(i int) int {
	if i < 0 || i+4 > len(d.offsets) {
		return -1
	}
	return int(binary.BigEndian.Uint32(d.offsets[i*4 : i*4+4]))
}

func (d *indirectIndex) Seek(key []byte) int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.searchOffset(key)
}

// searchOffset searches the offsets slice for key and returns the position in
// offsets where key would exist.
func (d *indirectIndex) searchOffset(key []byte) int {
	// We use a binary search across our indirect offsets (pointers to all the keys
	// in the index slice).
	i := bytesutil.SearchBytesFixed(d.offsets, 4, func(x []byte) bool {
		// i is the position in offsets we are at so get offset it points to
		offset := int32(binary.BigEndian.Uint32(x))

		// It's pointing to the start of the key which is a 2 byte length
		keyLen := int32(binary.BigEndian.Uint16(d.b[offset : offset+2]))

		// See if it matches
		return bytes.Compare(d.b[offset+2:offset+2+keyLen], key) >= 0
	})

	// See if we might have found the right index
	if i < len(d.offsets) {
		return int(i / 4)
	}

	// The key is not in the index.  i is the index where it would be inserted so return
	// a value outside our offset range.
	return int(len(d.offsets)) / 4
}

// search returns the byte position of key in the index.  If key is not
// in the index, len(index) is returned.
func (d *indirectIndex) search(key []byte) int {
	// We use a binary search across our indirect offsets (pointers to all the keys
	// in the index slice).
	i := bytesutil.SearchBytesFixed(d.offsets, 4, func(x []byte) bool {
		// i is the position in offsets we are at so get offset it points to
		offset := int32(binary.BigEndian.Uint32(x))

		// It's pointing to the start of the key which is a 2 byte length
		keyLen := int32(binary.BigEndian.Uint16(d.b[offset : offset+2]))

		// See if it matches
		return bytes.Compare(d.b[offset+2:offset+2+keyLen], key) >= 0
	})

	// See if we might have found the right index
	if i < len(d.offsets) {
		ofs := binary.BigEndian.Uint32(d.offsets[i : i+4])
		_, k := readKey(d.b[ofs:])

		// The search may have returned an i == 0 which could indicated that the value
		// searched should be inserted at postion 0.  Make sure the key in the index
		// matches the search value.
		if !bytes.Equal(key, k) {
			return len(d.b)
		}

		return int(ofs)
	}

	// The key is not in the index.  i is the index where it would be inserted so return
	// a value outside our offset range.
	return len(d.b)
}

// ContainsKey returns true of key may exist in this index.
func (d *indirectIndex) ContainsKey(key []byte) bool {
	return bytes.Compare(key, d.minKey) >= 0 && bytes.Compare(key, d.maxKey) <= 0
}

// Entries returns all index entries for a key.
func (d *indirectIndex) Entries(key []byte) []IndexEntry {
	return d.ReadEntries(key, nil)
}

func (d *indirectIndex) readEntriesAt(ofs int, entries *[]IndexEntry) ([]byte, []IndexEntry) {
	n, k := readKey(d.b[ofs:])

	// Read and return all the entries
	ofs += n
	var ie indexEntries
	if entries != nil {
		ie.entries = *entries
	}
	if _, err := readEntries(d.b[ofs:], &ie); err != nil {
		panic(fmt.Sprintf("error reading entries: %v", err))
	}
	if entries != nil {
		*entries = ie.entries
	}
	return k, ie.entries
}

// ReadEntries returns all index entries for a key.
func (d *indirectIndex) ReadEntries(key []byte, entries *[]IndexEntry) []IndexEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	ofs := d.search(key)
	if ofs < len(d.b) {
		k, entries := d.readEntriesAt(ofs, entries)
		// The search may have returned an i == 0 which could indicated that the value
		// searched should be inserted at position 0.  Make sure the key in the index
		// matches the search value.
		if !bytes.Equal(key, k) {
			return nil
		}

		return entries
	}

	// The key is not in the index.  i is the index where it would be inserted.
	return nil
}

// Entry returns the index entry for the specified key and timestamp.  If no entry
// matches the key an timestamp, nil is returned.
func (d *indirectIndex) Entry(key []byte, timestamp int64) *IndexEntry {
	entries := d.Entries(key)
	for _, entry := range entries {
		if entry.Contains(timestamp) {
			return &entry
		}
	}
	return nil
}

// Key returns the key in the index at the given position.
func (d *indirectIndex) Key(idx int, entries *[]IndexEntry) ([]byte, byte, []IndexEntry) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if idx < 0 || idx*4+4 > len(d.offsets) {
		return nil, 0, nil
	}
	ofs := binary.BigEndian.Uint32(d.offsets[idx*4 : idx*4+4])
	n, key := readKey(d.b[ofs:])

	typ := d.b[int(ofs)+n]

	var ie indexEntries
	if entries != nil {
		ie.entries = *entries
	}
	if _, err := readEntries(d.b[int(ofs)+n:], &ie); err != nil {
		return nil, 0, nil
	}
	if entries != nil {
		*entries = ie.entries
	}

	return key, typ, ie.entries
}

// KeyAt returns the key in the index at the given position.
func (d *indirectIndex) KeyAt(idx int) ([]byte, byte) {
	d.mu.RLock()

	if idx < 0 || idx*4+4 > len(d.offsets) {
		d.mu.RUnlock()
		return nil, 0
	}
	ofs := int32(binary.BigEndian.Uint32(d.offsets[idx*4 : idx*4+4]))

	n, key := readKey(d.b[ofs:])
	ofs = ofs + int32(n)
	typ := d.b[ofs]
	d.mu.RUnlock()
	return key, typ
}

// KeyCount returns the count of unique keys in the index.
func (d *indirectIndex) KeyCount() int {
	d.mu.RLock()
	n := len(d.offsets) / 4
	d.mu.RUnlock()
	return n
}

// Delete removes the given keys from the index.
func (d *indirectIndex) Delete(keys [][]byte) {
	if len(keys) == 0 {
		return
	}

	if !bytesutil.IsSorted(keys) {
		bytesutil.Sort(keys)
	}

	// Both keys and offsets are sorted.  Walk both in order and skip
	// any keys that exist in both.
	d.mu.Lock()
	start := d.searchOffset(keys[0])
	for i := start * 4; i+4 <= len(d.offsets) && len(keys) > 0; i += 4 {
		offset := binary.BigEndian.Uint32(d.offsets[i : i+4])
		_, indexKey := readKey(d.b[offset:])

		for len(keys) > 0 && bytes.Compare(keys[0], indexKey) < 0 {
			keys = keys[1:]
		}

		if len(keys) > 0 && bytes.Equal(keys[0], indexKey) {
			keys = keys[1:]
			copy(d.offsets[i:i+4], nilOffset[:])
		}
	}
	d.offsets = bytesutil.Pack(d.offsets, 4, 255)
	d.mu.Unlock()
}

// DeleteRange removes the given keys with data between minTime and maxTime from the index.
func (d *indirectIndex) DeleteRange(keys [][]byte, minTime, maxTime int64) {
	// No keys, nothing to do
	if len(keys) == 0 {
		return
	}

	if !bytesutil.IsSorted(keys) {
		bytesutil.Sort(keys)
	}

	// If we're deleting the max time range, just use tombstoning to remove the
	// key from the offsets slice
	if minTime == math.MinInt64 && maxTime == math.MaxInt64 {
		d.Delete(keys)
		return
	}

	// Is the range passed in outside of the time range for the file?
	min, max := d.TimeRange()
	if minTime > max || maxTime < min {
		return
	}

	fullKeys := make([][]byte, 0, len(keys))
	tombstones := map[string][]TimeRange{}
	var ie []IndexEntry

	for i := 0; len(keys) > 0 && i < d.KeyCount(); i++ {
		k, entries := d.readEntriesAt(d.offset(i), &ie)

		// Skip any keys that don't exist.  These are less than the current key.
		for len(keys) > 0 && bytes.Compare(keys[0], k) < 0 {
			keys = keys[1:]
		}

		// No more keys to delete, we're done.
		if len(keys) == 0 {
			break
		}

		// If the current key is greater than the index one, continue to the next
		// index key.
		if len(keys) > 0 && bytes.Compare(keys[0], k) > 0 {
			continue
		}

		// If multiple tombstones are saved for the same key
		if len(entries) == 0 {
			continue
		}

		// Is the time range passed outside of the time range we've have stored for this key?
		min, max := entries[0].MinTime, entries[len(entries)-1].MaxTime
		if minTime > max || maxTime < min {
			continue
		}

		// Does the range passed in cover every value for the key?
		if minTime <= min && maxTime >= max {
			fullKeys = append(fullKeys, keys[0])
			keys = keys[1:]
			continue
		}

		d.mu.RLock()
		existing := d.tombstones[string(k)]
		d.mu.RUnlock()

		// Append the new tombonstes to the existing ones
		newTs := append(existing, append(tombstones[string(k)], TimeRange{minTime, maxTime})...)
		fn := func(i, j int) bool {
			a, b := newTs[i], newTs[j]
			if a.Min == b.Min {
				return a.Max <= b.Max
			}
			return a.Min < b.Min
		}

		// Sort the updated tombstones if necessary
		if len(newTs) > 1 && !sort.SliceIsSorted(newTs, fn) {
			sort.Slice(newTs, fn)
		}

		tombstones[string(k)] = newTs

		// We need to see if all the tombstones end up deleting the entire series.  This
		// could happen if their is one tombstore with min,max time spanning all the block
		// time ranges or from multiple smaller tombstones the delete segments.  To detect
		// this cases, we use a window starting at the first tombstone and grow it be each
		// tombstone that is immediately adjacent to the current window or if it overlaps.
		// If there are any gaps, we abort.
		minTs, maxTs := newTs[0].Min, newTs[0].Max
		for j := 1; j < len(newTs); j++ {
			prevTs := newTs[j-1]
			ts := newTs[j]

			// Make sure all the tombstone line up for a continuous range.  We don't
			// want to have two small deletes on each edges end up causing us to
			// remove the full key.
			if prevTs.Max != ts.Min-1 && !prevTs.Overlaps(ts.Min, ts.Max) {
				minTs, maxTs = int64(math.MaxInt64), int64(math.MinInt64)
				break
			}

			if ts.Min < minTs {
				minTs = ts.Min
			}
			if ts.Max > maxTs {
				maxTs = ts.Max
			}
		}

		// If we have a fully deleted series, delete it all of it.
		if minTs <= min && maxTs >= max {
			fullKeys = append(fullKeys, keys[0])
			keys = keys[1:]
			continue
		}
	}

	// Delete all the keys that fully deleted in bulk
	if len(fullKeys) > 0 {
		d.Delete(fullKeys)
	}

	if len(tombstones) == 0 {
		return
	}

	d.mu.Lock()
	for k, v := range tombstones {
		d.tombstones[k] = v
	}
	d.mu.Unlock()
}

// TombstoneRange returns ranges of time that are deleted for the given key.
func (d *indirectIndex) TombstoneRange(key []byte) []TimeRange {
	d.mu.RLock()
	r := d.tombstones[string(key)]
	d.mu.RUnlock()
	return r
}

// Contains return true if the given key exists in the index.
func (d *indirectIndex) Contains(key []byte) bool {
	return len(d.Entries(key)) > 0
}

// ContainsValue returns true if key and time might exist in this file.
func (d *indirectIndex) ContainsValue(key []byte, timestamp int64) bool {
	entry := d.Entry(key, timestamp)
	if entry == nil {
		return false
	}

	d.mu.RLock()
	tombstones := d.tombstones[string(key)]
	d.mu.RUnlock()

	for _, t := range tombstones {
		if t.Min <= timestamp && t.Max >= timestamp {
			return false
		}
	}
	return true
}

// Type returns the block type of the values stored for the key.
func (d *indirectIndex) Type(key []byte) (byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	ofs := d.search(key)
	if ofs < len(d.b) {
		n, _ := readKey(d.b[ofs:])
		ofs += n
		return d.b[ofs], nil
	}
	return 0, fmt.Errorf("key does not exist: %s", key)
}

// OverlapsTimeRange returns true if the time range of the file intersect min and max.
func (d *indirectIndex) OverlapsTimeRange(min, max int64) bool {
	return d.minTime <= max && d.maxTime >= min
}

// OverlapsKeyRange returns true if the min and max keys of the file overlap the arguments min and max.
func (d *indirectIndex) OverlapsKeyRange(min, max []byte) bool {
	return bytes.Compare(d.minKey, max) <= 0 && bytes.Compare(d.maxKey, min) >= 0
}

// KeyRange returns the min and max keys in the index.
func (d *indirectIndex) KeyRange() ([]byte, []byte) {
	return d.minKey, d.maxKey
}

// TimeRange returns the min and max time across all keys in the index.
func (d *indirectIndex) TimeRange() (int64, int64) {
	return d.minTime, d.maxTime
}

// MarshalBinary returns a byte slice encoded version of the index.
func (d *indirectIndex) MarshalBinary() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.b, nil
}

// UnmarshalBinary populates an index from an encoded byte slice
// representation of an index.
func (d *indirectIndex) UnmarshalBinary(b []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Keep a reference to the actual index bytes
	d.b = b
	if len(b) == 0 {
		return nil
	}

	//var minKey, maxKey []byte
	var minTime, maxTime int64 = math.MaxInt64, 0

	// To create our "indirect" index, we need to find the location of all the keys in
	// the raw byte slice.  The keys are listed once each (in sorted order).  Following
	// each key is a time ordered list of index entry blocks for that key.  The loop below
	// basically skips across the slice keeping track of the counter when we are at a key
	// field.
	var i int32
	var offsets []int32
	iMax := int32(len(b))
	for i < iMax {
		offsets = append(offsets, i)

		// Skip to the start of the values
		// key length value (2) + type (1) + length of key
		if i+2 >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for key length value")
		}
		i += 3 + int32(binary.BigEndian.Uint16(b[i:i+2]))

		// count of index entries
		if i+indexCountSize >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for index entries count")
		}
		count := int32(binary.BigEndian.Uint16(b[i : i+indexCountSize]))
		i += indexCountSize

		// Find the min time for the block
		if i+8 >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for min time")
		}
		minT := int64(binary.BigEndian.Uint64(b[i : i+8]))
		if minT < minTime {
			minTime = minT
		}

		i += (count - 1) * indexEntrySize

		// Find the max time for the block
		if i+16 >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for max time")
		}
		maxT := int64(binary.BigEndian.Uint64(b[i+8 : i+16]))
		if maxT > maxTime {
			maxTime = maxT
		}

		i += indexEntrySize
	}

	firstOfs := offsets[0]
	_, key := readKey(b[firstOfs:])
	d.minKey = key

	lastOfs := offsets[len(offsets)-1]
	_, key = readKey(b[lastOfs:])
	d.maxKey = key

	d.minTime = minTime
	d.maxTime = maxTime

	var err error
	d.offsets, err = mmap(nil, 0, len(offsets)*4)
	if err != nil {
		return err
	}
	for i, v := range offsets {
		binary.BigEndian.PutUint32(d.offsets[i*4:i*4+4], uint32(v))
	}

	return nil
}

// Size returns the size of the current index in bytes.
func (d *indirectIndex) Size() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return uint32(len(d.b))
}

func (d *indirectIndex) Close() error {
	// Windows doesn't use the anonymous map for the offsets index
	if runtime.GOOS == "windows" {
		return nil
	}
	return munmap(d.offsets[:cap(d.offsets)])
}

// mmapAccess is mmap based block accessor.  It access blocks through an
// MMAP file interface.
type mmapAccessor struct {
	// Counter incremented everytime the mmapAccessor is accessed
	accessCount uint64
	// Counter to determine whether the accessor can free its resources
	freeCount uint64

	mu sync.RWMutex

	f     *os.File
	b     []byte
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

	indexOfsPos := len(m.b) - 8
	indexStart := binary.BigEndian.Uint64(m.b[indexOfsPos : indexOfsPos+8])
	if indexStart >= uint64(indexOfsPos) {
		return nil, fmt.Errorf("mmapAccessor: invalid indexStart")
	}

	// Hint to the kernal that we will be reading the file.  It would be better to hint
	// that we will be reading the index section, but that doesn't seem to work ATM.
	_ = madviseWillNeed(m.b)

	m.index = NewIndirectIndex()
	if err := m.index.UnmarshalBinary(m.b[indexStart:indexOfsPos]); err != nil {
		return nil, err
	}

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

	if err := renameFile(m.f.Name(), path); err != nil {
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
	return err
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

func (m *mmapAccessor) readFloatBlock(entry *IndexEntry, values *[]FloatValue) ([]FloatValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeFloatBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readIntegerBlock(entry *IndexEntry, values *[]IntegerValue) ([]IntegerValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeIntegerBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readUnsignedBlock(entry *IndexEntry, values *[]UnsignedValue) ([]UnsignedValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeUnsignedBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readStringBlock(entry *IndexEntry, values *[]StringValue) ([]StringValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeStringBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (m *mmapAccessor) readBooleanBlock(entry *IndexEntry, values *[]BooleanValue) ([]BooleanValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeBooleanBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	return a, nil
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

	blocks := m.index.Entries(key)
	if len(blocks) == 0 {
		return nil, nil
	}

	tombstones := m.index.TombstoneRange(key)

	m.mu.RLock()
	defer m.mu.RUnlock()

	var temp []Value
	var err error
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

func readKey(b []byte) (n int, key []byte) {
	// 2 byte size of key
	n, size := 2, int(binary.BigEndian.Uint16(b[:2]))

	// N byte key
	key = b[n : n+size]

	n += len(key)
	return
}

func readEntries(b []byte, entries *indexEntries) (n int, err error) {
	if len(b) < 1+indexCountSize {
		return 0, fmt.Errorf("readEntries: data too short for headers")
	}

	// 1 byte block type
	entries.Type = b[n]
	n++

	// 2 byte count of index entries
	count := int(binary.BigEndian.Uint16(b[n : n+indexCountSize]))
	n += indexCountSize

	if cap(entries.entries) < count {
		entries.entries = make([]IndexEntry, count)
	} else {
		entries.entries = entries.entries[:count]
	}

	b = b[indexCountSize+indexTypeSize:]
	for i := 0; i < len(entries.entries); i++ {
		if err = entries.entries[i].UnmarshalBinary(b); err != nil {
			return 0, fmt.Errorf("readEntries: unmarshal error: %v", err)
		}
		b = b[indexEntrySize:]
	}

	n += count * indexEntrySize

	return
}
