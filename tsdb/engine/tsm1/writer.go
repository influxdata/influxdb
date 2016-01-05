package tsm1

/*
A TSM file is composed for four sections: header, blocks, index and the footer.

┌────────┬────────────────────────────────────┬─────────────┬──────────────┐
│ Header │               Blocks               │    Index    │    Footer    │
│5 bytes │              N bytes               │   N bytes   │   4 bytes    │
└────────┴────────────────────────────────────┴─────────────┴──────────────┘

Header is composed of a magic number to identify the file type and a version
number.

┌───────────────────┐
│      Header       │
├─────────┬─────────┤
│  Magic  │ Version │
│ 4 bytes │ 1 byte  │
└─────────┴─────────┘

Blocks are sequences of pairs of CRC32 and data.  The block data is opaque to the
file.  The CRC32 is used for block level error detection.  The length of the blocks
is stored in the index.

┌───────────────────────────────────────────────────────────┐
│                          Blocks                           │
├───────────────────┬───────────────────┬───────────────────┤
│      Block 1      │      Block 2      │      Block N      │
├─────────┬─────────┼─────────┬─────────┼─────────┬─────────┤
│  CRC    │  Data   │  CRC    │  Data   │  CRC    │  Data   │
│ 4 bytes │ N bytes │ 4 bytes │ N bytes │ 4 bytes │ N bytes │
└─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘

Following the blocks is the index for the blocks in the file.  The index is
composed of a sequence of index entries ordered lexicographically by key and
then by time.  Each index entry starts with a key length and key followed by a
count of the number of blocks in the file.  Each block entry is composed of
the min and max time for the block, the offset into the file where the block
is located and the the size of the block.

The index structure can provide efficient access to all blocks as well as the
ability to determine the cost associated with acessing a given key.  Given a key
and timestamp, we can determine whether a file contains the block for that
timestamp as well as where that block resides and how much data to read to
retrieve the block.  If we know we need to read all or multiple blocks in a
file, we can use the size to determine how much to read in a given IO.

┌────────────────────────────────────────────────────────────────────────────┐
│                                   Index                                    │
├─────────┬─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┬───┤
│ Key Len │   Key   │ Type │ Count │Min Time │Max Time │ Offset │  Size  │...│
│ 2 bytes │ N bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │8 bytes │4 bytes │   │
└─────────┴─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┴───┘

The last section is the footer that stores the offset of the start of the index.

┌─────────┐
│ Footer  │
├─────────┤
│Index Ofs│
│ 8 bytes │
└─────────┘
*/

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sort"
	"sync"
	"time"
)

const (
	// MagicNumber is written as the first 4 bytes of a data file to
	// identify the file as a tsm1 formatted file
	MagicNumber uint32 = 0x16D116D1

	Version byte = 1

	// Size in bytes of an index entry
	indexEntrySize = 28

	// Size in bytes used to store the count of index entries for a key
	indexCountSize = 2

	// Size in bytes used to store the type of block encoded
	indexTypeSize = 1

	// Max number of blocks for a given key that can exist in a single file
	maxIndexEntries = (1 << (indexCountSize * 8)) - 1
)

var (
	ErrNoValues  = fmt.Errorf("no values written")
	ErrTSMClosed = fmt.Errorf("tsm file closed")
)

// TSMWriter writes TSM formatted key and values.
type TSMWriter interface {
	// Write writes a new block for key containing and values.  Writes append
	// blocks in the order that the Write function is called.  The caller is
	// responsible for ensuring keys and blocks or sorted appropriately.
	// Values are encoded as a full block.  The caller is responsible for
	// ensuring a fixed number of values are encoded in each block as wells as
	// ensuring the Values are sorted. The first and last timestamp values are
	// used as the minimum and maximum values for the index entry.
	Write(key string, values Values) error

	// WriteBlock writes a new block for key containing the bytes in block.  WriteBlock appends
	// blocks in the order that the WriteBlock function is called.  The caller is
	// responsible for ensuring keys and blocks are sorted appropriately, and that the
	// block and index information is correct for the block.  The minTime and maxTime
	// timestamp values are used as the minimum and maximum values for the index entry.
	WriteBlock(key string, minTime, maxTime time.Time, block []byte) error

	// WriteIndex finishes the TSM write streams and writes the index.
	WriteIndex() error

	// Closes any underlying file resources.
	Close() error

	// Size returns the current size in bytes of the file
	Size() uint32
}

// TSMIndex represent the index section of a TSM file.  The index records all
// blocks, their locations, sizes, min and max times.
type TSMIndex interface {

	// Add records a new block entry for a key in the index.
	Add(key string, blockType byte, minTime, maxTime time.Time, offset int64, size uint32)

	// Delete removes the given keys from the index.
	Delete(keys []string)

	// Contains return true if the given key exists in the index.
	Contains(key string) bool

	// ContainsValue returns true if key and time might exists in this file.  This function could
	// return true even though the actual point does not exists.  For example, the key may
	// exists in this file, but not have point exactly at time t.
	ContainsValue(key string, timestamp time.Time) bool

	// Entries returns all index entries for a key.
	Entries(key string) []*IndexEntry

	// Entry returns the index entry for the specified key and timestamp.  If no entry
	// matches the key and timestamp, nil is returned.
	Entry(key string, timestamp time.Time) *IndexEntry

	// Keys returns the unique set of keys in the index.
	Keys() []string

	// Key returns the key in the index at the given postion.
	Key(index int) (string, []*IndexEntry)

	// KeyAt returns the key in the index at the given postion.
	KeyAt(index int) string

	// KeyCount returns the count of unique keys in the index.
	KeyCount() int

	// Size returns the size of a the current index in bytes
	Size() uint32

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (time.Time, time.Time)

	// KeyRange returns the min and max keys in the file.
	KeyRange() (string, string)

	// Type returns the block type of the values stored for the key.  Returns one of
	// BlockFloat64, BlockInt64, BlockBool, BlockString.  If key does not exist,
	// an error is returned.
	Type(key string) (byte, error)

	// MarshalBinary returns a byte slice encoded version of the index.
	MarshalBinary() ([]byte, error)

	// UnmarshalBinary populates an index from an encoded byte slice
	// representation of an index.
	UnmarshalBinary(b []byte) error

	// Write writes the index contents to a writer
	Write(w io.Writer) error
}

// IndexEntry is the index information for a given block in a TSM file.
type IndexEntry struct {

	// The min and max time of all points stored in the block.
	MinTime, MaxTime time.Time

	// The absolute position in the file where this block is located.
	Offset int64

	// The size in bytes of the block in the file.
	Size uint32
}

func (e *IndexEntry) UnmarshalBinary(b []byte) error {
	if len(b) != indexEntrySize {
		return fmt.Errorf("unmarshalBinary: short buf: %v != %v", indexEntrySize, len(b))
	}
	e.MinTime = time.Unix(0, int64(btou64(b[:8])))
	e.MaxTime = time.Unix(0, int64(btou64(b[8:16])))
	e.Offset = int64(btou64(b[16:24]))
	e.Size = btou32(b[24:28])
	return nil
}

// Returns true if this IndexEntry may contain values for the given time.  The min and max
// times are inclusive.
func (e *IndexEntry) Contains(t time.Time) bool {
	return (e.MinTime.Equal(t) || e.MinTime.Before(t)) &&
		(e.MaxTime.Equal(t) || e.MaxTime.After(t))
}

func (e *IndexEntry) OverlapsTimeRange(min, max time.Time) bool {
	return (e.MinTime.Equal(max) || e.MinTime.Before(max)) &&
		(e.MaxTime.Equal(min) || e.MaxTime.After(min))
}

func (e *IndexEntry) String() string {
	return fmt.Sprintf("min=%s max=%s ofs=%d siz=%d", e.MinTime.UTC(), e.MaxTime.UTC(), e.Offset, e.Size)
}

func NewDirectIndex() TSMIndex {
	return &directIndex{
		blocks: map[string]*indexEntries{},
	}
}

// directIndex is a simple in-memory index implementation for a TSM file.  The full index
// must fit in memory.
type directIndex struct {
	mu     sync.RWMutex
	size   uint32
	blocks map[string]*indexEntries
}

func (d *directIndex) Add(key string, blockType byte, minTime, maxTime time.Time, offset int64, size uint32) {
	d.mu.Lock()
	defer d.mu.Unlock()

	entries := d.blocks[key]
	if entries == nil {
		entries = &indexEntries{
			Type: blockType,
		}
		d.blocks[key] = entries
		// size of the key stored in the index
		d.size += uint32(2 + len(key))

		// size of the count of entries stored in the index
		d.size += indexCountSize
	}
	entries.Append(&IndexEntry{
		MinTime: minTime,
		MaxTime: maxTime,
		Offset:  offset,
		Size:    size,
	})

	// size of the encoded index entry
	d.size += indexEntrySize

}

func (d *directIndex) Entries(key string) []*IndexEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entries := d.blocks[key]
	if entries == nil {
		return nil
	}
	return d.blocks[key].entries
}

func (d *directIndex) Entry(key string, t time.Time) *IndexEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entries := d.Entries(key)
	for _, entry := range entries {
		if entry.Contains(t) {
			return entry
		}
	}
	return nil
}

func (d *directIndex) Type(key string) (byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	entries := d.blocks[key]
	if entries != nil {
		return entries.Type, nil
	}
	return 0, fmt.Errorf("key does not exist: %v", key)
}

func (d *directIndex) Contains(key string) bool {
	return len(d.Entries(key)) > 0
}

func (d *directIndex) ContainsValue(key string, t time.Time) bool {
	return d.Entry(key, t) != nil
}

func (d *directIndex) Delete(keys []string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, k := range keys {
		delete(d.blocks, k)
	}
}

func (d *directIndex) Keys() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var keys []string
	for k := range d.blocks {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (d *directIndex) Key(idx int) (string, []*IndexEntry) {
	if idx < 0 || idx >= len(d.blocks) {
		return "", nil
	}
	k := d.Keys()[idx]
	return k, d.blocks[k].entries
}

func (d *directIndex) KeyAt(idx int) string {
	if idx < 0 || idx >= len(d.blocks) {
		return ""
	}
	return d.Keys()[idx]
}

func (d *directIndex) KeyCount() int {
	return len(d.blocks)
}

func (d *directIndex) KeyRange() (string, string) {
	var min, max string
	for k := range d.blocks {
		if min == "" || k < min {
			min = k
		}
		if max == "" || k > max {
			max = k
		}

	}
	return min, max
}

func (d *directIndex) TimeRange() (time.Time, time.Time) {
	min, max := time.Unix(0, math.MaxInt64), time.Unix(0, math.MinInt64)
	for _, entries := range d.blocks {
		for _, e := range entries.entries {
			if e.MinTime.Before(min) {
				min = e.MinTime
			}
			if e.MaxTime.After(max) {
				max = e.MaxTime
			}
		}
	}
	return min, max
}

func (d *directIndex) addEntries(key string, entries *indexEntries) {
	existing := d.blocks[key]
	if existing == nil {
		d.blocks[key] = entries
		return
	}
	existing.Append(entries.entries...)
}

func (d *directIndex) Write(w io.Writer) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Index blocks are writtens sorted by key
	var keys []string
	for k := range d.blocks {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// For each key, individual entries are sorted by time
	for _, key := range keys {
		entries := d.blocks[key]

		if entries.Len() > maxIndexEntries {
			return fmt.Errorf("key '%s' exceeds max index entries: %d > %d",
				key, entries.Len(), maxIndexEntries)
		}
		sort.Sort(entries)

		// Append the key length and key
		_, err := w.Write(u16tob(uint16(len(key))))
		if err != nil {
			return fmt.Errorf("write: writer key length error: %v", err)
		}

		_, err = io.WriteString(w, key)
		if err != nil {
			return fmt.Errorf("write: writer key error: %v", err)
		}

		// Append the block type
		_, err = w.Write([]byte{entries.Type})
		if err != nil {
			return fmt.Errorf("write: writer key type error: %v", err)
		}

		// Append the index block count
		_, err = w.Write(u16tob(uint16(entries.Len())))
		if err != nil {
			return fmt.Errorf("write: writer block count error: %v", err)
		}

		// Append each index entry for all blocks for this key
		if err = entries.Write(w); err != nil {
			return fmt.Errorf("write: writer entries error: %v", err)
		}
	}
	return nil
}

func (d *directIndex) MarshalBinary() ([]byte, error) {
	var b bytes.Buffer
	if err := d.Write(&b); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (d *directIndex) UnmarshalBinary(b []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.size = uint32(len(b))

	var pos int
	for pos < len(b) {
		n, key, err := readKey(b[pos:])
		if err != nil {
			return fmt.Errorf("readIndex: read key error: %v", err)
		}
		pos += n

		n, entries, err := readEntries(b[pos:])
		if err != nil {
			return fmt.Errorf("readIndex: read entries error: %v", err)
		}

		pos += n
		d.addEntries(string(key), entries)
	}
	return nil
}

func (d *directIndex) Size() uint32 {
	return d.size
}

// tsmWriter writes keys and values in the TSM format
type tsmWriter struct {
	wrapped io.Writer
	w       *bufio.Writer
	index   TSMIndex
	n       int64
}

func NewTSMWriter(w io.Writer) (TSMWriter, error) {
	index := &directIndex{
		blocks: map[string]*indexEntries{},
	}

	return &tsmWriter{wrapped: w, w: bufio.NewWriterSize(w, 4*1024*1024), index: index}, nil
}

func (t *tsmWriter) writeHeader() error {
	n, err := t.w.Write(append(u32tob(MagicNumber), Version))
	if err != nil {
		return err
	}
	t.n = int64(n)
	return nil
}

func (t *tsmWriter) Write(key string, values Values) error {
	// Nothing to write
	if len(values) == 0 {
		return nil
	}

	// Write header only after we have some data to write.
	if t.n == 0 {
		if err := t.writeHeader(); err != nil {
			return err
		}
	}

	block, err := values.Encode(nil)
	if err != nil {
		return err
	}

	checksum := crc32.ChecksumIEEE(block)

	n, err := t.w.Write(append(u32tob(checksum), block...))
	if err != nil {
		return err
	}

	blockType, err := BlockType(block)
	if err != nil {
		return err
	}
	// Record this block in index
	t.index.Add(key, blockType, values[0].Time(), values[len(values)-1].Time(), t.n, uint32(n))

	// Increment file position pointer
	t.n += int64(n)
	return nil
}

func (t *tsmWriter) WriteBlock(key string, minTime, maxTime time.Time, block []byte) error {
	// Nothing to write
	if len(block) == 0 {
		return nil
	}

	// Write header only after we have some data to write.
	if t.n == 0 {
		if err := t.writeHeader(); err != nil {
			return err
		}
	}

	checksum := crc32.ChecksumIEEE(block)

	_, err := t.w.Write(u32tob(checksum))
	if err != nil {
		return err
	}

	n, err := t.w.Write(block)
	if err != nil {
		return err
	}

	// 4 byte checksum + len of block
	blockSize := 4 + n

	blockType, err := BlockType(block)
	if err != nil {
		return err
	}
	// Record this block in index
	t.index.Add(key, blockType, minTime, maxTime, t.n, uint32(blockSize))

	// Increment file position pointer (checksum + block len)
	t.n += int64(blockSize)

	return nil
}

// WriteIndex writes the index section of the file.  If there are no index entries to write,
// this returns ErrNoValues
func (t *tsmWriter) WriteIndex() error {
	indexPos := t.n

	if t.index.KeyCount() == 0 {
		return ErrNoValues
	}

	// Write the index
	if err := t.index.Write(t.w); err != nil {
		return err
	}

	// Write the index index position
	_, err := t.w.Write(u64tob(uint64(indexPos)))
	if err != nil {
		return err
	}
	return nil
}

func (t *tsmWriter) Close() error {
	if err := t.w.Flush(); err != nil {
		return err
	}

	if c, ok := t.wrapped.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (t *tsmWriter) Size() uint32 {
	return uint32(t.n) + t.index.Size()
}

func u16tob(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func btou16(b []byte) uint16 {
	return binary.BigEndian.Uint16(b)
}

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btou64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func u32tob(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func btou32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// verifyVersion will verify that the reader's bytes are a TSM byte
// stream of the correct version (1)
func verifyVersion(r io.ReadSeeker) error {
	_, err := r.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("init: failed to seek: %v", err)
	}
	b := make([]byte, 4)
	_, err = r.Read(b)
	if err != nil {
		return fmt.Errorf("init: error reading magic number of file: %v", err)
	}
	if bytes.Compare(b, u32tob(MagicNumber)) != 0 {
		return fmt.Errorf("can only read from tsm file")
	}
	_, err = r.Read(b)
	if err != nil {
		return fmt.Errorf("init: error reading version: %v", err)
	}
	if b[0] != Version {
		return fmt.Errorf("init: file is version %b. expected %b", b[0], Version)
	}

	return nil
}
