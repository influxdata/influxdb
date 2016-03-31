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
	"os"
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

	// max length of a key in an index entry (measurement + tags)
	maxKeyLength = (1 << (2 * 8)) - 1
)

var (
	ErrNoValues             = fmt.Errorf("no values written")
	ErrTSMClosed            = fmt.Errorf("tsm file closed")
	ErrMaxKeyLengthExceeded = fmt.Errorf("max key length exceeded")
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
	WriteBlock(key string, minTime, maxTime int64, block []byte) error

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
	Add(key string, blockType byte, minTime, maxTime int64, offset int64, size uint32)

	// Delete removes the given keys from the index.
	Delete(keys []string)

	// Contains return true if the given key exists in the index.
	Contains(key string) bool

	// ContainsValue returns true if key and time might exists in this file.  This function could
	// return true even though the actual point does not exists.  For example, the key may
	// exists in this file, but not have point exactly at time t.
	ContainsValue(key string, timestamp int64) bool

	// Entries returns all index entries for a key.
	Entries(key string) []IndexEntry
	ReadEntries(key string, entries *[]IndexEntry)

	// Entry returns the index entry for the specified key and timestamp.  If no entry
	// matches the key and timestamp, nil is returned.
	Entry(key string, timestamp int64) *IndexEntry

	// Keys returns the unique set of keys in the index.
	Keys() []string

	// Key returns the key in the index at the given postion.
	Key(index int) (string, []IndexEntry)

	// KeyAt returns the key in the index at the given postion.
	KeyAt(index int) (string, byte)

	// KeyCount returns the count of unique keys in the index.
	KeyCount() int

	// Size returns the size of a the current index in bytes
	Size() uint32

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (int64, int64)

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
	MinTime, MaxTime int64

	// The absolute position in the file where this block is located.
	Offset int64

	// The size in bytes of the block in the file.
	Size uint32
}

// UnmarshalBinary decodes an IndexEntry from a byte slice
func (e *IndexEntry) UnmarshalBinary(b []byte) error {
	if len(b) != indexEntrySize {
		return fmt.Errorf("unmarshalBinary: short buf: %v != %v", indexEntrySize, len(b))
	}
	e.MinTime = int64(binary.BigEndian.Uint64(b[:8]))
	e.MaxTime = int64(binary.BigEndian.Uint64(b[8:16]))
	e.Offset = int64(binary.BigEndian.Uint64(b[16:24]))
	e.Size = binary.BigEndian.Uint32(b[24:28])
	return nil
}

// AppendTo will write a binary-encoded version of IndexEntry to b, allocating
// and returning a new slice, if necessary
func (e *IndexEntry) AppendTo(b []byte) []byte {
	if len(b) < indexEntrySize {
		if cap(b) < indexEntrySize {
			b = make([]byte, indexEntrySize)
		} else {
			b = b[:indexEntrySize]
		}
	}

	binary.BigEndian.PutUint64(b[:8], uint64(e.MinTime))
	binary.BigEndian.PutUint64(b[8:16], uint64(e.MaxTime))
	binary.BigEndian.PutUint64(b[16:24], uint64(e.Offset))
	binary.BigEndian.PutUint32(b[24:28], uint32(e.Size))

	return b
}

// Returns true if this IndexEntry may contain values for the given time.  The min and max
// times are inclusive.
func (e *IndexEntry) Contains(t int64) bool {
	return e.MinTime <= t && e.MaxTime >= t
}

func (e *IndexEntry) OverlapsTimeRange(min, max int64) bool {
	return e.MinTime <= max && e.MaxTime >= min
}

func (e *IndexEntry) String() string {
	return fmt.Sprintf("min=%s max=%s ofs=%d siz=%d",
		time.Unix(0, e.MinTime).UTC(), time.Unix(0, e.MaxTime).UTC(), e.Offset, e.Size)
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

func (d *directIndex) Add(key string, blockType byte, minTime, maxTime int64, offset int64, size uint32) {
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
	entries.entries = append(entries.entries, IndexEntry{
		MinTime: minTime,
		MaxTime: maxTime,
		Offset:  offset,
		Size:    size,
	})

	// size of the encoded index entry
	d.size += indexEntrySize
}

func (d *directIndex) Entries(key string) []IndexEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entries := d.blocks[key]
	if entries == nil {
		return nil
	}
	return d.blocks[key].entries
}

func (d *directIndex) ReadEntries(key string, entries *[]IndexEntry) {
	*entries = d.Entries(key)
}

func (d *directIndex) Entry(key string, t int64) *IndexEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entries := d.Entries(key)
	for _, entry := range entries {
		if entry.Contains(t) {
			return &entry
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

func (d *directIndex) ContainsValue(key string, t int64) bool {
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

func (d *directIndex) Key(idx int) (string, []IndexEntry) {
	if idx < 0 || idx >= len(d.blocks) {
		return "", nil
	}
	k := d.Keys()[idx]
	return k, d.blocks[k].entries
}

func (d *directIndex) KeyAt(idx int) (string, byte) {
	if idx < 0 || idx >= len(d.blocks) {
		return "", 0
	}
	key := d.Keys()[idx]
	entries := d.blocks[key]
	return key, entries.Type

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

func (d *directIndex) TimeRange() (int64, int64) {
	min, max := int64(math.MaxInt64), int64(math.MinInt64)
	for _, entries := range d.blocks {
		for _, e := range entries.entries {
			if e.MinTime < min {
				min = e.MinTime
			}
			if e.MaxTime > max {
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
	existing.entries = append(existing.entries, entries.entries...)
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

	var buf [5]byte
	var err error

	// For each key, individual entries are sorted by time
	for _, key := range keys {
		entries := d.blocks[key]

		if entries.Len() > maxIndexEntries {
			return fmt.Errorf("key '%s' exceeds max index entries: %d > %d",
				key, entries.Len(), maxIndexEntries)
		}
		sort.Sort(entries)

		binary.BigEndian.PutUint16(buf[0:2], uint16(len(key)))
		buf[2] = entries.Type
		binary.BigEndian.PutUint16(buf[3:5], uint16(entries.Len()))

		// Append the key length and key
		if _, err = w.Write(buf[0:2]); err != nil {
			return fmt.Errorf("write: writer key length error: %v", err)
		}

		if _, err = io.WriteString(w, key); err != nil {
			return fmt.Errorf("write: writer key error: %v", err)
		}

		// Append the block type and count
		if _, err = w.Write(buf[2:5]); err != nil {
			return fmt.Errorf("write: writer block type and count error: %v", err)
		}

		// Append each index entry for all blocks for this key
		if _, err = entries.WriteTo(w); err != nil {
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

		var entries indexEntries
		n, err = readEntries(b[pos:], &entries)
		if err != nil {
			return fmt.Errorf("readIndex: read entries error: %v", err)
		}

		pos += n
		d.addEntries(string(key), &entries)
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
	var buf [5]byte
	binary.BigEndian.PutUint32(buf[0:4], MagicNumber)
	buf[4] = Version

	n, err := t.w.Write(buf[:])
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

	if len(key) > maxKeyLength {
		return ErrMaxKeyLengthExceeded
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

	blockType, err := BlockType(block)
	if err != nil {
		return err
	}

	var checksum [crc32.Size]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(block))

	_, err = t.w.Write(checksum[:])
	if err != nil {
		return err
	}

	n, err := t.w.Write(block)
	if err != nil {
		return err
	}
	n += len(checksum)

	// Record this block in index
	t.index.Add(key, blockType, values[0].UnixNano(), values[len(values)-1].UnixNano(), t.n, uint32(n))

	// Increment file position pointer
	t.n += int64(n)
	return nil
}

func (t *tsmWriter) WriteBlock(key string, minTime, maxTime int64, block []byte) error {
	// Nothing to write
	if len(block) == 0 {
		return nil
	}

	blockType, err := BlockType(block)
	if err != nil {
		return err
	}

	// Write header only after we have some data to write.
	if t.n == 0 {
		if err := t.writeHeader(); err != nil {
			return err
		}
	}

	var checksum [crc32.Size]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(block))

	_, err = t.w.Write(checksum[:])
	if err != nil {
		return err
	}

	n, err := t.w.Write(block)
	if err != nil {
		return err
	}
	n += len(checksum)

	// Record this block in index
	t.index.Add(key, blockType, minTime, maxTime, t.n, uint32(n))

	// Increment file position pointer (checksum + block len)
	t.n += int64(n)

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

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(indexPos))

	// Write the index index position
	_, err := t.w.Write(buf[:])
	return err
}

func (t *tsmWriter) Close() error {
	if err := t.w.Flush(); err != nil {
		return err
	}

	if f, ok := t.wrapped.(*os.File); ok {
		if err := f.Sync(); err != nil {
			return err
		}
	}

	if c, ok := t.wrapped.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (t *tsmWriter) Size() uint32 {
	return uint32(t.n) + t.index.Size()
}

// verifyVersion will verify that the reader's bytes are a TSM byte
// stream of the correct version (1)
func verifyVersion(r io.ReadSeeker) error {
	_, err := r.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("init: failed to seek: %v", err)
	}
	var b [4]byte
	_, err = io.ReadFull(r, b[:])
	if err != nil {
		return fmt.Errorf("init: error reading magic number of file: %v", err)
	}
	if binary.BigEndian.Uint32(b[:]) != MagicNumber {
		return fmt.Errorf("can only read from tsm file")
	}
	_, err = io.ReadFull(r, b[:1])
	if err != nil {
		return fmt.Errorf("init: error reading version: %v", err)
	}
	if b[0] != Version {
		return fmt.Errorf("init: file is version %b. expected %b", b[0], Version)
	}

	return nil
}
