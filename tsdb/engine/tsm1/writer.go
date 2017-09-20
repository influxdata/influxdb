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
	"os"
	"sort"
	"strings"
	"time"
)

const (
	// MagicNumber is written as the first 4 bytes of a data file to
	// identify the file as a tsm1 formatted file
	MagicNumber uint32 = 0x16D116D1

	// Version indicates the version of the TSM file format.
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
	//ErrNoValues is returned when TSMWriter.WriteIndex is called and there are no values to write.
	ErrNoValues = fmt.Errorf("no values written")

	// ErrTSMClosed is returned when performing an operation against a closed TSM file.
	ErrTSMClosed = fmt.Errorf("tsm file closed")

	// ErrMaxKeyLengthExceeded is returned when attempting to write a key that is too long.
	ErrMaxKeyLengthExceeded = fmt.Errorf("max key length exceeded")

	// ErrMaxBlocksExceeded is returned when attempting to write a block past the allowed number.
	ErrMaxBlocksExceeded = fmt.Errorf("max blocks exceeded")
)

// TSMWriter writes TSM formatted key and values.
type TSMWriter interface {
	// Write writes a new block for key containing and values.  Writes append
	// blocks in the order that the Write function is called.  The caller is
	// responsible for ensuring keys and blocks are sorted appropriately.
	// Values are encoded as a full block.  The caller is responsible for
	// ensuring a fixed number of values are encoded in each block as well as
	// ensuring the Values are sorted. The first and last timestamp values are
	// used as the minimum and maximum values for the index entry.
	Write(key []byte, values Values) error

	// WriteBlock writes a new block for key containing the bytes in block.  WriteBlock appends
	// blocks in the order that the WriteBlock function is called.  The caller is
	// responsible for ensuring keys and blocks are sorted appropriately, and that the
	// block and index information is correct for the block.  The minTime and maxTime
	// timestamp values are used as the minimum and maximum values for the index entry.
	WriteBlock(key []byte, minTime, maxTime int64, block []byte) error

	// WriteIndex finishes the TSM write streams and writes the index.
	WriteIndex() error

	// Flushes flushes all pending changes to the underlying file resources.
	Flush() error

	// Close closes any underlying file resources.
	Close() error

	// Size returns the current size in bytes of the file.
	Size() uint32
}

// IndexWriter writes a TSMIndex.
type IndexWriter interface {
	// Add records a new block entry for a key in the index.
	Add(key []byte, blockType byte, minTime, maxTime int64, offset int64, size uint32)

	// Entries returns all index entries for a key.
	Entries(key []byte) []IndexEntry

	// Keys returns the unique set of keys in the index.
	Keys() [][]byte

	// KeyCount returns the count of unique keys in the index.
	KeyCount() int

	// Size returns the size of a the current index in bytes.
	Size() uint32

	// MarshalBinary returns a byte slice encoded version of the index.
	MarshalBinary() ([]byte, error)

	// WriteTo writes the index contents to a writer.
	WriteTo(w io.Writer) (int64, error)

	Close() error
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

// UnmarshalBinary decodes an IndexEntry from a byte slice.
func (e *IndexEntry) UnmarshalBinary(b []byte) error {
	if len(b) < indexEntrySize {
		return fmt.Errorf("unmarshalBinary: short buf: %v < %v", len(b), indexEntrySize)
	}
	e.MinTime = int64(binary.BigEndian.Uint64(b[:8]))
	e.MaxTime = int64(binary.BigEndian.Uint64(b[8:16]))
	e.Offset = int64(binary.BigEndian.Uint64(b[16:24]))
	e.Size = binary.BigEndian.Uint32(b[24:28])
	return nil
}

// AppendTo writes a binary-encoded version of IndexEntry to b, allocating
// and returning a new slice, if necessary.
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

// Contains returns true if this IndexEntry may contain values for the given time.
// The min and max times are inclusive.
func (e *IndexEntry) Contains(t int64) bool {
	return e.MinTime <= t && e.MaxTime >= t
}

// OverlapsTimeRange returns true if the given time ranges are completely within the entry's time bounds.
func (e *IndexEntry) OverlapsTimeRange(min, max int64) bool {
	return e.MinTime <= max && e.MaxTime >= min
}

// String returns a string representation of the entry.
func (e *IndexEntry) String() string {
	return fmt.Sprintf("min=%s max=%s ofs=%d siz=%d",
		time.Unix(0, e.MinTime).UTC(), time.Unix(0, e.MaxTime).UTC(), e.Offset, e.Size)
}

// NewIndexWriter returns a new IndexWriter.
func NewIndexWriter() IndexWriter {
	return &directIndex{}
}

// NewIndexWriter returns a new IndexWriter.
func NewDiskIndexWriter(f *os.File) IndexWriter {
	return &directIndex{fd: f, w: bufio.NewWriterSize(f, 1024*1024)}
}

// indexBlock represent an index information for a series within a TSM file.
type indexBlock struct {
	key     []byte
	entries *indexEntries
}

// directIndex is a simple in-memory index implementation for a TSM file.  The full index
// must fit in memory.
type directIndex struct {
	size   uint32
	blocks []indexBlock
	fd     *os.File
	w      *bufio.Writer
}

func (d *directIndex) Add(key []byte, blockType byte, minTime, maxTime int64, offset int64, size uint32) {
	// Is this the first block being added?
	if len(d.blocks) == 0 {
		// size of the key stored in the index
		d.size += uint32(2 + len(key))
		// size of the count of entries stored in the index
		d.size += indexCountSize

		d.blocks = append(d.blocks, indexBlock{
			key: key,
			entries: &indexEntries{
				Type: blockType,
				entries: []IndexEntry{IndexEntry{
					MinTime: minTime,
					MaxTime: maxTime,
					Offset:  offset,
					Size:    size,
				}}},
		})

		// size of the encoded index entry
		d.size += indexEntrySize
		return
	}

	// Find the last block so we can see if were still adding to the same series key.
	block := d.blocks[len(d.blocks)-1]
	cmp := bytes.Compare(block.key, key)
	if cmp == 0 {
		// The last block is still this key
		block.entries.entries = append(block.entries.entries, IndexEntry{
			MinTime: minTime,
			MaxTime: maxTime,
			Offset:  offset,
			Size:    size,
		})

		// size of the encoded index entry
		d.size += indexEntrySize

	} else if cmp < 0 {
		if d.w != nil {
			d.flush(d.w)
		}
		// We have a new key that is greater than the last one so we need to add
		// a new index block section.

		// size of the key stored in the index
		d.size += uint32(2 + len(key))
		// size of the count of entries stored in the index
		d.size += indexCountSize

		d.blocks = append(d.blocks, indexBlock{
			key: key,
			entries: &indexEntries{
				Type: blockType,
				entries: []IndexEntry{IndexEntry{
					MinTime: minTime,
					MaxTime: maxTime,
					Offset:  offset,
					Size:    size,
				}}},
		})

		// size of the encoded index entry
		d.size += indexEntrySize
	} else {
		// Keys can't be added out of order.
		panic(fmt.Sprintf("keys must be added in sorted order: %s < %s", string(key), string(d.blocks[len(d.blocks)-1].key)))
	}
}

func (d *directIndex) entries(key []byte) []IndexEntry {
	if len(d.blocks) == 0 {
		return nil
	}

	if bytes.Equal(d.blocks[len(d.blocks)-1].key, key) {
		return d.blocks[len(d.blocks)-1].entries.entries
	}

	i := sort.Search(len(d.blocks), func(i int) bool { return bytes.Compare(d.blocks[i].key, key) >= 0 })
	if i < len(d.blocks) && bytes.Equal(d.blocks[i].key, key) {
		return d.blocks[i].entries.entries
	}

	return nil
}

func (d *directIndex) Entries(key []byte) []IndexEntry {
	return d.entries(key)
}

func (d *directIndex) Entry(key []byte, t int64) *IndexEntry {
	entries := d.entries(key)
	for _, entry := range entries {
		if entry.Contains(t) {
			return &entry
		}
	}
	return nil
}

func (d *directIndex) Keys() [][]byte {
	keys := make([][]byte, 0, len(d.blocks))
	for _, v := range d.blocks {
		keys = append(keys, v.key)
	}
	return keys
}

func (d *directIndex) KeyCount() int {
	return len(d.blocks)
}

func (d *directIndex) WriteTo(w io.Writer) (int64, error) {
	if d.w == nil {
		return d.flush(w)
	}

	if _, err := d.flush(d.w); err != nil {
		return 0, err
	}

	if err := d.w.Flush(); err != nil {
		return 0, err
	}

	if _, err := d.fd.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	return io.Copy(w, bufio.NewReader(d.fd))
}

func (d *directIndex) flush(w io.Writer) (int64, error) {
	var (
		n   int
		err error
		buf [5]byte
		N   int64
	)

	// For each key, individual entries are sorted by time
	for _, ie := range d.blocks {
		key := ie.key
		entries := ie.entries

		if entries.Len() > maxIndexEntries {
			return N, fmt.Errorf("key '%s' exceeds max index entries: %d > %d", key, entries.Len(), maxIndexEntries)
		}

		if !sort.IsSorted(entries) {
			sort.Sort(entries)
		}

		binary.BigEndian.PutUint16(buf[0:2], uint16(len(key)))
		buf[2] = entries.Type
		binary.BigEndian.PutUint16(buf[3:5], uint16(entries.Len()))

		// Append the key length and key
		if n, err = w.Write(buf[0:2]); err != nil {
			return int64(n) + N, fmt.Errorf("write: writer key length error: %v", err)
		}
		N += int64(n)

		if n, err = w.Write(key); err != nil {
			return int64(n) + N, fmt.Errorf("write: writer key error: %v", err)
		}
		N += int64(n)

		// Append the block type and count
		if n, err = w.Write(buf[2:5]); err != nil {
			return int64(n) + N, fmt.Errorf("write: writer block type and count error: %v", err)
		}
		N += int64(n)

		// Append each index entry for all blocks for this key
		var n64 int64
		if n64, err = entries.WriteTo(w); err != nil {
			return n64 + N, fmt.Errorf("write: writer entries error: %v", err)
		}
		N += n64

	}

	d.blocks = d.blocks[:0]

	return N, nil

}

func (d *directIndex) MarshalBinary() ([]byte, error) {
	var b bytes.Buffer
	if _, err := d.WriteTo(&b); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (d *directIndex) Size() uint32 {
	return d.size
}

func (d *directIndex) Close() error {
	if d.w == nil {
		return nil
	}

	// Flush anything remaining in the index
	if err := d.w.Flush(); err != nil {
		return err
	}
	if err := d.fd.Close(); err != nil {
		return nil
	}
	return os.Remove(d.fd.Name())
}

// tsmWriter writes keys and values in the TSM format
type tsmWriter struct {
	wrapped io.Writer
	w       *bufio.Writer
	index   IndexWriter
	n       int64
}

// NewTSMWriter returns a new TSMWriter writing to w.
func NewTSMWriter(w io.Writer) (TSMWriter, error) {
	var index IndexWriter
	if fw, ok := w.(*os.File); ok && !strings.HasSuffix(fw.Name(), "01.tsm.tmp") {
		f, err := os.Create(strings.TrimSuffix(fw.Name(), ".tsm.tmp") + ".idx.tmp")
		if err != nil {
			return nil, err
		}
		index = NewDiskIndexWriter(f)
	} else {
		index = NewIndexWriter()
	}

	return &tsmWriter{wrapped: w, w: bufio.NewWriterSize(w, 1024*1024), index: index}, nil
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

// Write writes a new block containing key and values.
func (t *tsmWriter) Write(key []byte, values Values) error {
	if len(key) > maxKeyLength {
		return ErrMaxKeyLengthExceeded
	}

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

// WriteBlock writes block for the given key and time range to the TSM file.  If the write
// exceeds max entries for a given key, ErrMaxBlocksExceeded is returned.  This indicates
// that the index is now full for this key and no future writes to this key will succeed.
func (t *tsmWriter) WriteBlock(key []byte, minTime, maxTime int64, block []byte) error {
	if len(key) > maxKeyLength {
		return ErrMaxKeyLengthExceeded
	}

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

	if len(t.index.Entries(key)) >= maxIndexEntries {
		return ErrMaxBlocksExceeded
	}

	return nil
}

// WriteIndex writes the index section of the file.  If there are no index entries to write,
// this returns ErrNoValues.
func (t *tsmWriter) WriteIndex() error {
	indexPos := t.n

	if t.index.KeyCount() == 0 {
		return ErrNoValues
	}

	// Write the index
	if _, err := t.index.WriteTo(t.w); err != nil {
		return err
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(indexPos))

	// Write the index index position
	_, err := t.w.Write(buf[:])
	return err
}

func (t *tsmWriter) Flush() error {
	if err := t.w.Flush(); err != nil {
		return err
	}

	if f, ok := t.wrapped.(*os.File); ok {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (t *tsmWriter) Close() error {
	if err := t.Flush(); err != nil {
		return err
	}

	if err := t.index.Close(); err != nil {
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

// verifyVersion verifies that the reader's bytes are a TSM byte
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
