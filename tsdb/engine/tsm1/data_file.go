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

┌──────────────────────────────────────────────────────────────────────────┐
│                                  Index                                   │
├─────────┬─────────┬───────┬─────────┬─────────┬─────────┬─────────┬──────┤
│ Key Len │   Key   │ Count │Min Time │Max Time │ Offset  │  Size   │ ...  │
│ 2 bytes │ N bytes │2 bytes│ 8 bytes │ 8 bytes │ 8 bytes │ 4 bytes │      │
└─────────┴─────────┴───────┴─────────┴─────────┴─────────┴─────────┴──────┘

The last section is the footer that stores the offset of the start of the index.

┌─────────┐
│ Footer  │
├─────────┤
│Index Ofs│
│ 8 bytes │
└─────────┘
*/

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"time"
)

const (
	// MagicNumber is written as the first 4 bytes of a data file to
	// identify the file as a tsm1 formatted file
	MagicNumber uint32 = 0x16D116D1

	Version byte = 1

	indexEntrySize = 28
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

	// Close finishes the TSM write streams and writes the index.
	Close() error
}

// TSMIndex represent the index section of a TSM file.  The index records all
// blocks, their locations, sizes, min and max times.
type TSMIndex interface {

	// Add records a new block entry for a key in the index.
	Add(key string, minTime, maxTime time.Time, offset int64, size uint32)

	// Entries returns all index entries for a key.
	Entries(key string) []*IndexEntry

	// Entry returns the index entry for the specified key and timestamp.  If no entry
	// matches the key an timestamp, nil is returned.
	Entry(key string, timestamp time.Time) *IndexEntry

	// MarshalBinary returns a byte slice encoded version of the index.
	MarshalBinary() ([]byte, error)

	// UnmarshalBinary populates an index from an encoded byte slice
	// representation of an index.
	UnmarshalBinary(b []byte) error
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

// directIndex is a simple in-memory index implementation for a TSM file.  The full index
// must fit in memory.
type directIndex struct {
	blocks map[string]indexEntries
}

func (d *directIndex) Add(key string, minTime, maxTime time.Time, offset int64, size uint32) {
	d.blocks[key] = append(d.blocks[key], &IndexEntry{
		MinTime: minTime,
		MaxTime: maxTime,
		Offset:  offset,
		Size:    size,
	})
}

func (d *directIndex) Entries(key string) []*IndexEntry {
	return d.blocks[key]
}

func (d *directIndex) Entry(key string, timestamp time.Time) *IndexEntry {
	entries := d.Entries(key)
	for _, entry := range entries {
		if entry.MinTime.Equal(timestamp) || entry.MinTime.Before(timestamp) &&
			entry.MaxTime.Equal(timestamp) || entry.MaxTime.After(timestamp) {
			return entry
		}
	}
	return nil
}

func (d *directIndex) addEntries(key string, entries indexEntries) {
	d.blocks[key] = append(d.blocks[key], entries...)
}

func (d *directIndex) Write(w io.Writer) error {
	b, err := d.MarshalBinary()
	if err != nil {
		return fmt.Errorf("write: marshal error: %v", err)
	}

	// Write out the index bytes
	_, err = w.Write(b)
	if err != nil {
		return fmt.Errorf("write: writer error: %v", err)
	}
	return nil
}

func (d *directIndex) MarshalBinary() ([]byte, error) {
	// Index blocks are writtens sorted by key
	var keys []string
	for k := range d.blocks {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Buffer to build up the index and write in bulk
	var b []byte

	// For each key, individual entries are sorted by time
	for _, key := range keys {
		entries := d.blocks[key]
		sort.Sort(entries)

		// Append the key length and key
		b = append(b, u16tob(uint16(len(key)))...)
		b = append(b, key...)

		// Append the index block count
		b = append(b, u16tob(uint16(len(entries)))...)

		// Append each index entry for all blocks for this key
		for _, entry := range entries {
			b = append(b, u64tob(uint64(entry.MinTime.UnixNano()))...)
			b = append(b, u64tob(uint64(entry.MaxTime.UnixNano()))...)
			b = append(b, u64tob(uint64(entry.Offset))...)
			b = append(b, u32tob(entry.Size)...)
		}
	}
	return b, nil
}

func (d *directIndex) UnmarshalBinary(b []byte) error {
	var pos int
	for pos < len(b) {
		n, key, err := d.readKey(b[pos:])
		if err != nil {
			return fmt.Errorf("readIndex: read key error: %v", err)
		}

		pos += n
		n, entries, err := d.readEntries(b[pos:])
		if err != nil {
			return fmt.Errorf("readIndex: read entries error: %v", err)
		}

		pos += n
		d.addEntries(key, entries)
	}
	return nil
}

func (d *directIndex) readKey(b []byte) (n int, key string, err error) {
	// 2 byte size of key
	n, size := 2, int(btou16(b[:2]))

	// N byte key
	key = string(b[n : n+size])
	n += len(key)
	return
}

func (d *directIndex) readEntries(b []byte) (n int, entries indexEntries, err error) {
	// 2 byte count of index entries
	n, count := 2, int(btou16(b[:2]))

	for i := 0; i < count; i++ {
		ie := &IndexEntry{}
		if err := ie.UnmarshalBinary(b[i*indexEntrySize+2 : i*indexEntrySize+2+indexEntrySize]); err != nil {
			return 0, nil, fmt.Errorf("readEntries: unmarshal error: %v", err)
		}
		entries = append(entries, ie)
		n += indexEntrySize
	}
	return
}

// tsmWriter writes keys and values in the TSM format
type tsmWriter struct {
	w     io.Writer
	index TSMIndex
	n     int64
}

func NewTSMWriter(w io.Writer) (TSMWriter, error) {
	n, err := w.Write(append(u32tob(MagicNumber), Version))
	if err != nil {
		return nil, err
	}

	index := &directIndex{
		blocks: map[string]indexEntries{},
	}

	return &tsmWriter{w: w, index: index, n: int64(n)}, nil
}

func (t *tsmWriter) Write(key string, values Values) error {
	block, err := values.Encode(nil)
	if err != nil {
		return err
	}

	checksum := crc32.ChecksumIEEE(block)

	n, err := t.w.Write(append(u32tob(checksum), block...))
	if err != nil {
		return err
	}

	// Record this block in index
	t.index.Add(key, values[0].Time(), values[len(values)-1].Time(), t.n, uint32(n))

	// Increment file position pointer
	t.n += int64(n)
	return nil
}

func (t *tsmWriter) Close() error {
	indexPos := t.n

	// Generate the index bytes
	b, err := t.index.MarshalBinary()
	if err != nil {
		return err
	}

	// Write the index followed by index position
	_, err = t.w.Write(append(b, u64tob(uint64(indexPos))...))
	if err != nil {
		return err
	}

	return nil
}

type tsmReader struct {
	r                    io.ReadSeeker
	indexStart, indexEnd int64
	index                TSMIndex
}

func NewTSMReader(r io.ReadSeeker) (*tsmReader, error) {
	t := &tsmReader{r: r}
	if err := t.init(); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *tsmReader) init() error {
	// Current the readers size
	size, err := t.r.Seek(0, os.SEEK_END)
	if err != nil {
		return fmt.Errorf("init: failed to seek: %v", err)
	}

	t.indexEnd = size - 8

	// Seek to index location pointer
	_, err = t.r.Seek(-8, os.SEEK_END)
	if err != nil {
		return fmt.Errorf("init: failed to seek to index ptr: %v", err)
	}

	// Read the absolute position of the start of the index
	b := make([]byte, 8)
	_, err = t.r.Read(b)
	if err != nil {
		return fmt.Errorf("init: failed to read index ptr: %v", err)

	}

	t.indexStart = int64(btou64(b))

	_, err = t.r.Seek(t.indexStart, os.SEEK_SET)
	if err != nil {
		return fmt.Errorf("init: failed to seek to index: %v", err)
	}

	b = make([]byte, t.indexEnd-t.indexStart)
	t.index = &directIndex{
		blocks: map[string]indexEntries{},
	}
	_, err = t.r.Read(b)
	if err != nil {
		return fmt.Errorf("init: read index: %v", err)
	}

	if err := t.index.UnmarshalBinary(b); err != nil {
		return fmt.Errorf("init: unmarshal error: %v", err)
	}

	return nil
}

func (t *tsmReader) Read(key string, timestamp time.Time) ([]Value, error) {
	block := t.index.Entry(key, timestamp)
	if block == nil {
		return nil, nil
	}

	// TODO: remove this allocation
	b := make([]byte, 16*1024)
	_, err := t.r.Seek(block.Offset, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	if int(block.Size) > len(b) {
		b = make([]byte, block.Size)
	}

	n, err := t.r.Read(b)
	if err != nil {
		return nil, err
	}

	//TODO: Validate checksum
	var values []Value
	err = DecodeBlock(b[4:n], &values)
	if err != nil {
		return nil, err
	}

	return values, nil
}

// ReadAll returns all values for a key in all blocks.
func (t *tsmReader) ReadAll(key string) ([]Value, error) {
	var values []Value
	blocks := t.index.Entries(key)
	if len(blocks) == 0 {
		return values, nil
	}

	var temp []Value
	// TODO: we can determine the max block size when loading the file create/re-use
	// a reader level buf then.
	b := make([]byte, 16*1024)
	for _, block := range blocks {
		_, err := t.r.Seek(block.Offset, os.SEEK_SET)
		if err != nil {
			return nil, err
		}

		if int(block.Size) > len(b) {
			b = make([]byte, block.Size)
		}

		n, err := t.r.Read(b)
		if err != nil {
			return nil, err
		}

		//TODO: Validate checksum
		temp = temp[:0]
		err = DecodeBlock(b[4:n], &temp)
		if err != nil {
			return nil, err
		}
		values = append(values, temp...)
	}

	return values, nil
}

type indexEntries []*IndexEntry

func (a indexEntries) Len() int           { return len(a) }
func (a indexEntries) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a indexEntries) Less(i, j int) bool { return a[i].MinTime.UnixNano() < a[j].MinTime.UnixNano() }

func u16tob(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func btou16(b []byte) uint16 {
	return uint16(binary.BigEndian.Uint16(b))
}
