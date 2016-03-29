package tsm1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"
)

type TSMReader struct {
	mu sync.RWMutex

	// accessor provides access and decoding of blocks for the reader
	accessor blockAccessor

	// index is the index of all blocks.
	index TSMIndex

	// tombstoner ensures tombstoned keys are not available by the index.
	tombstoner *Tombstoner

	// size is the size of the file on disk.
	size int64

	// lastModified is the last time this file was modified on disk
	lastModified int64
}

// BlockIterator allows iterating over each block in a TSM file in order.  It provides
// raw access to the block bytes without decoding them.
type BlockIterator struct {
	r *TSMReader

	// i is the current key index
	i int

	// n is the total number of keys
	n int

	key     string
	entries []IndexEntry
	err     error
}

func (b *BlockIterator) PeekNext() string {
	if len(b.entries) > 1 {
		return b.key
	} else if b.n-b.i > 1 {
		key, _ := b.r.KeyAt(b.i + 1)
		return key
	}
	return ""
}

func (b *BlockIterator) Next() bool {
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
		b.key, b.entries = b.r.Key(b.i)
		b.i++
		return true
	}

	return false
}

func (b *BlockIterator) Read() (string, int64, int64, []byte, error) {
	if b.err != nil {
		return "", 0, 0, nil, b.err
	}

	buf, err := b.r.readBytes(&b.entries[0], nil)
	if err != nil {
		return "", 0, 0, nil, err
	}
	return b.key, b.entries[0].MinTime, b.entries[0].MaxTime, buf, err
}

// blockAccessor abstracts a method of accessing blocks from a
// TSM file.
type blockAccessor interface {
	init() (TSMIndex, error)
	read(key string, timestamp int64) ([]Value, error)
	readAll(key string) ([]Value, error)
	readBlock(entry *IndexEntry, values []Value) ([]Value, error)
	readFloatBlock(entry *IndexEntry, values []FloatValue) ([]FloatValue, error)
	readIntegerBlock(entry *IndexEntry, values []IntegerValue) ([]IntegerValue, error)
	readStringBlock(entry *IndexEntry, values []StringValue) ([]StringValue, error)
	readBooleanBlock(entry *IndexEntry, values []BooleanValue) ([]BooleanValue, error)
	readBytes(entry *IndexEntry, buf []byte) ([]byte, error)
	path() string
	close() error
}

type TSMReaderOptions struct {
	// Reader is used to create file IO based reader.
	Reader io.ReadSeeker

	// MMAPFile is used to create an MMAP based reader.
	MMAPFile *os.File
}

func NewTSMReader(r io.ReadSeeker) (*TSMReader, error) {
	return NewTSMReaderWithOptions(
		TSMReaderOptions{
			Reader: r,
		})
}

func NewTSMReaderWithOptions(opt TSMReaderOptions) (*TSMReader, error) {
	t := &TSMReader{}
	if opt.Reader != nil {
		// Seek to the end of the file to determine the size
		size, err := opt.Reader.Seek(0, os.SEEK_END)
		if err != nil {
			return nil, err
		}
		t.size = size
		if f, ok := opt.Reader.(*os.File); ok {
			stat, err := f.Stat()
			if err != nil {
				return nil, err
			}

			t.lastModified = stat.ModTime().UnixNano()
		}
		t.accessor = &fileAccessor{
			r: opt.Reader,
		}

	} else if opt.MMAPFile != nil {
		stat, err := opt.MMAPFile.Stat()
		if err != nil {
			return nil, err
		}
		t.size = stat.Size()
		t.lastModified = stat.ModTime().UnixNano()
		t.accessor = &mmapAccessor{
			f: opt.MMAPFile,
		}
	} else {
		panic("invalid options: need Reader or MMAPFile")
	}

	index, err := t.accessor.init()
	if err != nil {
		return nil, err
	}

	t.index = index
	t.tombstoner = &Tombstoner{Path: t.Path()}

	if err := t.applyTombstones(); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *TSMReader) applyTombstones() error {
	// Read any tombstone entries if the exist
	tombstones, err := t.tombstoner.ReadAll()
	if err != nil {
		return fmt.Errorf("init: read tombstones: %v", err)
	}

	// Update our index
	t.index.Delete(tombstones)
	return nil
}

func (t *TSMReader) Path() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.accessor.path()
}

func (t *TSMReader) Keys() []string {
	return t.index.Keys()
}

func (t *TSMReader) Key(index int) (string, []IndexEntry) {
	return t.index.Key(index)
}

// KeyAt returns the key and key typegi at position idx in the index.
func (t *TSMReader) KeyAt(idx int) (string, byte) {
	return t.index.KeyAt(idx)
}

func (t *TSMReader) ReadAt(entry *IndexEntry, vals []Value) ([]Value, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.accessor.readBlock(entry, vals)
}

func (t *TSMReader) ReadFloatBlockAt(entry *IndexEntry, vals []FloatValue) ([]FloatValue, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.accessor.readFloatBlock(entry, vals)
}

func (t *TSMReader) ReadIntegerBlockAt(entry *IndexEntry, vals []IntegerValue) ([]IntegerValue, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.accessor.readIntegerBlock(entry, vals)
}

func (t *TSMReader) ReadStringBlockAt(entry *IndexEntry, vals []StringValue) ([]StringValue, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.accessor.readStringBlock(entry, vals)
}

func (t *TSMReader) ReadBooleanBlockAt(entry *IndexEntry, vals []BooleanValue) ([]BooleanValue, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.accessor.readBooleanBlock(entry, vals)
}

func (t *TSMReader) Read(key string, timestamp int64) ([]Value, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.accessor.read(key, timestamp)
}

// ReadAll returns all values for a key in all blocks.
func (t *TSMReader) ReadAll(key string) ([]Value, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.accessor.readAll(key)
}

func (t *TSMReader) readBytes(e *IndexEntry, b []byte) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.accessor.readBytes(e, b)
}

func (t *TSMReader) Type(key string) (byte, error) {
	return t.index.Type(key)
}

func (t *TSMReader) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.accessor.close()
}

// Remove removes any underlying files stored on disk for this reader.
func (t *TSMReader) Remove() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	path := t.accessor.path()
	if path != "" {
		os.RemoveAll(path)
	}

	if err := t.tombstoner.Delete(); err != nil {
		return err
	}
	return nil
}

func (t *TSMReader) Contains(key string) bool {
	return t.index.Contains(key)
}

// ContainsValue returns true if key and time might exists in this file.  This function could
// return true even though the actual point does not exists.  For example, the key may
// exists in this file, but not have point exactly at time t.
func (t *TSMReader) ContainsValue(key string, ts int64) bool {
	return t.index.ContainsValue(key, ts)
}

func (t *TSMReader) Delete(keys []string) error {
	if err := t.tombstoner.Add(keys); err != nil {
		return err
	}

	t.index.Delete(keys)
	return nil
}

// TimeRange returns the min and max time across all keys in the file.
func (t *TSMReader) TimeRange() (int64, int64) {
	return t.index.TimeRange()
}

// KeyRange returns the min and max key across all keys in the file.
func (t *TSMReader) KeyRange() (string, string) {
	return t.index.KeyRange()
}

func (t *TSMReader) KeyCount() int {
	return t.index.KeyCount()
}

func (t *TSMReader) Entries(key string) []IndexEntry {
	return t.index.Entries(key)
}

func (t *TSMReader) ReadEntries(key string, entries *[]IndexEntry) {
	t.index.ReadEntries(key, entries)
}

func (t *TSMReader) IndexSize() uint32 {
	return t.index.Size()
}

func (t *TSMReader) Size() uint32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return uint32(t.size)
}

func (t *TSMReader) LastModified() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastModified
}

// HasTombstones return true if there are any tombstone entries recorded.
func (t *TSMReader) HasTombstones() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tombstoner.HasTombstones()
}

// TombstoneFiles returns any tombstone files associated with this TSM file.
func (t *TSMReader) TombstoneFiles() []FileStat {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tombstoner.TombstoneFiles()
}

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

func (t *TSMReader) BlockIterator() *BlockIterator {
	return &BlockIterator{
		r: t,
		n: t.index.KeyCount(),
	}
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
	offsets []int32

	// minKey, maxKey are the minium and maximum (lexicographically sorted) contained in the
	// file
	minKey, maxKey string

	// minTime, maxTime are the minimum and maximum times contained in the file across all
	// series.
	minTime, maxTime int64
}

func NewIndirectIndex() TSMIndex {
	return &indirectIndex{}
}

// Add records a new block entry for a key in the index.
func (d *indirectIndex) Add(key string, blockType byte, minTime, maxTime int64, offset int64, size uint32) {
	panic("unsupported operation")
}

func (d *indirectIndex) Write(w io.Writer) error {
	panic("unsupported operation")
}

// search returns the index of i in offsets for where key is located.  If key is not
// in the index, len(index) is returned.
func (d *indirectIndex) search(key []byte) int {
	// We use a binary search across our indirect offsets (pointers to all the keys
	// in the index slice).
	i := sort.Search(len(d.offsets), func(i int) bool {
		// i is the position in offsets we are at so get offset it points to
		offset := d.offsets[i]

		// It's pointing to the start of the key which is a 2 byte length
		keyLen := int32(binary.BigEndian.Uint16(d.b[offset : offset+2]))

		// See if it matches
		return bytes.Compare(d.b[offset+2:offset+2+keyLen], key) >= 0
	})

	// See if we might have found the right index
	if i < len(d.offsets) {
		ofs := d.offsets[i]
		_, k, err := readKey(d.b[ofs:])
		if err != nil {
			panic(fmt.Sprintf("error reading key: %v", err))
		}

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

// Entries returns all index entries for a key.
func (d *indirectIndex) Entries(key string) []IndexEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	kb := []byte(key)

	ofs := d.search(kb)
	if ofs < len(d.b) {
		n, k, err := readKey(d.b[ofs:])
		if err != nil {
			panic(fmt.Sprintf("error reading key: %v", err))
		}

		// The search may have returned an i == 0 which could indicated that the value
		// searched should be inserted at postion 0.  Make sure the key in the index
		// matches the search value.
		if !bytes.Equal(kb, k) {
			return nil
		}

		// Read and return all the entries
		ofs += n
		var entries indexEntries
		if _, err := readEntries(d.b[ofs:], &entries); err != nil {
			panic(fmt.Sprintf("error reading entries: %v", err))
		}
		return entries.entries
	}

	// The key is not in the index.  i is the index where it would be inserted.
	return nil
}

// ReadEntries returns all index entries for a key.
func (d *indirectIndex) ReadEntries(key string, entries *[]IndexEntry) {
	*entries = d.Entries(key)
}

// Entry returns the index entry for the specified key and timestamp.  If no entry
// matches the key an timestamp, nil is returned.
func (d *indirectIndex) Entry(key string, timestamp int64) *IndexEntry {
	entries := d.Entries(key)
	for _, entry := range entries {
		if entry.Contains(timestamp) {
			return &entry
		}
	}
	return nil
}

func (d *indirectIndex) Keys() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var keys []string
	for _, offset := range d.offsets {
		_, key, _ := readKey(d.b[offset:])
		keys = append(keys, string(key))
	}
	return keys
}

func (d *indirectIndex) Key(idx int) (string, []IndexEntry) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if idx < 0 || idx >= len(d.offsets) {
		return "", nil
	}
	n, key, _ := readKey(d.b[d.offsets[idx]:])

	var entries indexEntries
	readEntries(d.b[int(d.offsets[idx])+n:], &entries)
	return string(key), entries.entries
}

func (d *indirectIndex) KeyAt(idx int) (string, byte) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if idx < 0 || idx >= len(d.offsets) {
		return "", 0
	}
	n, key, _ := readKey(d.b[d.offsets[idx]:])
	return string(key), d.b[d.offsets[idx]+int32(n)]
}

func (d *indirectIndex) KeyCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return len(d.offsets)
}

func (d *indirectIndex) Delete(keys []string) {
	if len(keys) == 0 {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	lookup := map[string]struct{}{}
	for _, k := range keys {
		lookup[k] = struct{}{}
	}

	var offsets []int32
	for _, offset := range d.offsets {
		_, indexKey, _ := readKey(d.b[offset:])

		if _, ok := lookup[string(indexKey)]; ok {
			continue
		}
		offsets = append(offsets, int32(offset))
	}
	d.offsets = offsets
}

func (d *indirectIndex) Contains(key string) bool {
	return len(d.Entries(key)) > 0
}

func (d *indirectIndex) ContainsValue(key string, timestamp int64) bool {
	return d.Entry(key, timestamp) != nil
}

func (d *indirectIndex) Type(key string) (byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	kb := []byte(key)
	ofs := d.search(kb)
	if ofs < len(d.b) {
		n, _, err := readKey(d.b[ofs:])
		if err != nil {
			panic(fmt.Sprintf("error reading key: %v", err))
		}

		ofs += n
		return d.b[ofs], nil
	}
	return 0, fmt.Errorf("key does not exist: %v", key)
}

func (d *indirectIndex) KeyRange() (string, string) {
	return d.minKey, d.maxKey
}

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

	//var minKey, maxKey []byte
	var minTime, maxTime int64 = math.MaxInt64, 0

	// To create our "indirect" index, we need to find the location of all the keys in
	// the raw byte slice.  The keys are listed once each (in sorted order).  Following
	// each key is a time ordered list of index entry blocks for that key.  The loop below
	// basically skips across the slice keeping track of the counter when we are at a key
	// field.
	var i int32
	for i < int32(len(b)) {
		d.offsets = append(d.offsets, i)

		// Skip to the start of the values
		// key length value (2) + type (1) + length of key
		i += 3 + int32(binary.BigEndian.Uint16(b[i:i+2]))

		// count of index entries
		count := int32(binary.BigEndian.Uint16(b[i : i+indexCountSize]))
		i += indexCountSize

		// Find the min time for the block
		minT := int64(binary.BigEndian.Uint64(b[i : i+8]))
		if minT < minTime {
			minTime = minT
		}

		i += (count - 1) * indexEntrySize

		// Find the max time for the block
		maxT := int64(binary.BigEndian.Uint64(b[i+8 : i+16]))
		if maxT > maxTime {
			maxTime = maxT
		}

		i += indexEntrySize
	}

	firstOfs := d.offsets[0]
	_, key, err := readKey(b[firstOfs:])
	if err != nil {
		return err
	}
	d.minKey = string(key)

	lastOfs := d.offsets[len(d.offsets)-1]
	_, key, err = readKey(b[lastOfs:])
	if err != nil {
		return err
	}
	d.maxKey = string(key)

	d.minTime = minTime
	d.maxTime = maxTime

	return nil
}

func (d *indirectIndex) Size() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return uint32(len(d.b))
}

// fileAccessor is file IO based block accessor.  It provides access to blocks
// using a file IO based approach (seek, read, etc.)
type fileAccessor struct {
	mu    sync.Mutex
	r     io.ReadSeeker
	index TSMIndex
}

func (f *fileAccessor) init() (TSMIndex, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Verify it's a TSM file of the right version
	if err := verifyVersion(f.r); err != nil {
		return nil, err
	}

	// Current the readers size
	size, err := f.r.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, fmt.Errorf("init: failed to seek: %v", err)
	}

	indexEnd := size - 8

	// Seek to index location pointer
	_, err = f.r.Seek(-8, os.SEEK_END)
	if err != nil {
		return nil, fmt.Errorf("init: failed to seek to index ptr: %v", err)
	}

	// Read the absolute position of the start of the index
	b := make([]byte, 8)
	_, err = f.r.Read(b)
	if err != nil {
		return nil, fmt.Errorf("init: failed to read index ptr: %v", err)

	}

	indexStart := int64(binary.BigEndian.Uint64(b))

	_, err = f.r.Seek(indexStart, os.SEEK_SET)
	if err != nil {
		return nil, fmt.Errorf("init: failed to seek to index: %v", err)
	}

	b = make([]byte, indexEnd-indexStart)
	f.index = &directIndex{
		blocks: map[string]*indexEntries{},
	}
	_, err = f.r.Read(b)
	if err != nil {
		return nil, fmt.Errorf("init: read index: %v", err)
	}

	if err := f.index.UnmarshalBinary(b); err != nil {
		return nil, fmt.Errorf("init: unmarshal error: %v", err)
	}

	return f.index, nil
}

func (f *fileAccessor) read(key string, timestamp int64) ([]Value, error) {
	entry := f.index.Entry(key, timestamp)

	if entry == nil {
		return nil, nil
	}

	return f.readBlock(entry, nil)
}

func (f *fileAccessor) readBlock(entry *IndexEntry, values []Value) ([]Value, error) {
	b, err := f.readBytes(entry, nil)
	if err != nil {
		return nil, err
	}

	//TODO: Validate checksum
	values, err = DecodeBlock(b, values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (f *fileAccessor) readFloatBlock(entry *IndexEntry, values []FloatValue) ([]FloatValue, error) {
	b, err := f.readBytes(entry, nil)
	if err != nil {
		return nil, err
	}

	// TODO: Validate checksum
	values, err = DecodeFloatBlock(b, values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (f *fileAccessor) readIntegerBlock(entry *IndexEntry, values []IntegerValue) ([]IntegerValue, error) {
	b, err := f.readBytes(entry, nil)
	if err != nil {
		return nil, err
	}

	// TODO: Validate checksum
	values, err = DecodeIntegerBlock(b, values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (f *fileAccessor) readStringBlock(entry *IndexEntry, values []StringValue) ([]StringValue, error) {
	b, err := f.readBytes(entry, nil)
	if err != nil {
		return nil, err
	}

	// TODO: Validate checksum
	values, err = DecodeStringBlock(b, values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (f *fileAccessor) readBooleanBlock(entry *IndexEntry, values []BooleanValue) ([]BooleanValue, error) {
	b, err := f.readBytes(entry, nil)
	if err != nil {
		return nil, err
	}

	// TODO: Validate checksum
	values, err = DecodeBooleanBlock(b, values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (f *fileAccessor) readBytes(entry *IndexEntry, b []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// TODO: remove this allocation
	if b == nil {
		b = make([]byte, entry.Size)
	}

	_, err := f.r.Seek(entry.Offset, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	if int(entry.Size) > len(b) {
		b = make([]byte, entry.Size)
	}

	n, err := f.r.Read(b[:entry.Size])
	if err != nil {
		return nil, err
	}

	return b[4:n], nil
}

// ReadAll returns all values for a key in all blocks.
func (f *fileAccessor) readAll(key string) ([]Value, error) {
	var values []Value
	blocks := f.index.Entries(key)
	if len(blocks) == 0 {
		return values, nil
	}

	var temp []Value
	// TODO: we can determine the max block size when loading the file create/re-use
	// a reader level buf then.
	b := make([]byte, 16*1024)
	for _, block := range blocks {

		b, err := f.readBytes(&block, b)
		if err != nil {
			return nil, err
		}

		//TODO: Validate checksum
		temp = temp[:0]
		temp, err = DecodeBlock(b, temp)
		if err != nil {
			return nil, err
		}
		values = append(values, temp...)
	}

	return values, nil
}

func (f *fileAccessor) path() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	if fd, ok := f.r.(*os.File); ok {
		return fd.Name()
	}
	return ""
}

func (f *fileAccessor) close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if c, ok := f.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// mmapAccess is mmap based block accessor.  It access blocks through an
// MMAP file interface.
type mmapAccessor struct {
	mu sync.RWMutex

	f     *os.File
	b     []byte
	index TSMIndex
}

func (m *mmapAccessor) init() (TSMIndex, error) {
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

	indexOfsPos := len(m.b) - 8
	indexStart := binary.BigEndian.Uint64(m.b[indexOfsPos : indexOfsPos+8])

	m.index = NewIndirectIndex()
	if err := m.index.UnmarshalBinary(m.b[indexStart:indexOfsPos]); err != nil {
		return nil, err
	}

	return m.index, nil
}

func (m *mmapAccessor) read(key string, timestamp int64) ([]Value, error) {
	entry := m.index.Entry(key, timestamp)
	if entry == nil {
		return nil, nil
	}

	return m.readBlock(entry, nil)
}

func (m *mmapAccessor) readBlock(entry *IndexEntry, values []Value) ([]Value, error) {
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

func (m *mmapAccessor) readFloatBlock(entry *IndexEntry, values []FloatValue) ([]FloatValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		return nil, ErrTSMClosed
	}
	//TODO: Validate checksum
	var err error
	values, err = DecodeFloatBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (m *mmapAccessor) readIntegerBlock(entry *IndexEntry, values []IntegerValue) ([]IntegerValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		return nil, ErrTSMClosed
	}
	//TODO: Validate checksum
	var err error
	values, err = DecodeIntegerBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (m *mmapAccessor) readStringBlock(entry *IndexEntry, values []StringValue) ([]StringValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		return nil, ErrTSMClosed
	}
	//TODO: Validate checksum
	var err error
	values, err = DecodeStringBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (m *mmapAccessor) readBooleanBlock(entry *IndexEntry, values []BooleanValue) ([]BooleanValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		return nil, ErrTSMClosed
	}
	//TODO: Validate checksum
	var err error
	values, err = DecodeBooleanBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (m *mmapAccessor) readBytes(entry *IndexEntry, b []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		return nil, ErrTSMClosed
	}

	// return the bytes after the 4 byte checksum
	return m.b[entry.Offset+4 : entry.Offset+int64(entry.Size)], nil
}

// ReadAll returns all values for a key in all blocks.
func (m *mmapAccessor) readAll(key string) ([]Value, error) {
	blocks := m.index.Entries(key)
	if len(blocks) == 0 {
		return nil, nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var temp []Value
	var err error
	var values []Value
	for _, block := range blocks {
		//TODO: Validate checksum
		temp = temp[:0]
		// The +4 is the 4 byte checksum length
		temp, err = DecodeBlock(m.b[block.Offset+4:block.Offset+int64(block.Size)], temp)
		if err != nil {
			return nil, err
		}
		values = append(values, temp...)
	}

	return values, nil
}

func (m *mmapAccessor) path() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.f.Name()
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

func readKey(b []byte) (n int, key []byte, err error) {
	// 2 byte size of key
	n, size := 2, int(binary.BigEndian.Uint16(b[:2]))

	// N byte key
	key = b[n : n+size]

	n += len(key)
	return
}

func readEntries(b []byte, entries *indexEntries) (n int, err error) {
	// 1 byte block type
	entries.Type = b[n]
	n++

	// 2 byte count of index entries
	count := int(binary.BigEndian.Uint16(b[n : n+indexCountSize]))
	n += indexCountSize

	entries.entries = make([]IndexEntry, count)
	for i := 0; i < count; i++ {
		var ie IndexEntry
		if err := ie.UnmarshalBinary(b[i*indexEntrySize+indexCountSize+indexTypeSize : i*indexEntrySize+indexCountSize+indexEntrySize+indexTypeSize]); err != nil {
			return 0, fmt.Errorf("readEntries: unmarshal error: %v", err)
		}
		entries.entries[i] = ie
		n += indexEntrySize
	}
	return
}
