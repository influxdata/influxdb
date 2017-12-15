package tsdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/mmap"
	"github.com/influxdata/influxdb/pkg/rhh"
)

// ErrSeriesOverflow is returned when too many series are added to a series writer.
var ErrSeriesOverflow = errors.New("series overflow")

// SeriesIDSize is the size in bytes of a series key ID.
const SeriesIDSize = 8

// Series flag constants.
const (
	SeriesFileFlagSize      = 1
	SeriesFileTombstoneFlag = 0x01
)

// MaxSeriesFileHashSize is the maximum number of series in a single hash map.
const MaxSeriesFileHashSize = (1 << 20 * SeriesMapLoadFactor) / 100 // (1MB * 90) / 100 == ~943K

// SeriesMapThreshold is the number of series IDs to hold in the in-memory
// series map before compacting and rebuilding the on-disk representation.
const SeriesMapThreshold = 1 << 25 // ~33M ids * 8 bytes per id == 256MB

// SeriesFile represents the section of the index that holds series data.
type SeriesFile struct {
	mu   sync.RWMutex
	path string
	data []byte
	file *os.File
	w    *bufio.Writer
	size int64

	seriesMap           *seriesMap
	compactingSeriesMap *seriesMap

	// MaxSize is the maximum size of the file.
	MaxSize int64
}

// NewSeriesFile returns a new instance of SeriesFile.
func NewSeriesFile(path string) *SeriesFile {
	return &SeriesFile{
		path:    path,
		MaxSize: DefaultMaxSeriesFileSize,
	}
}

// Open memory maps the data file at the file's path.
func (f *SeriesFile) Open() error {
	// Create the parent directories if they don't exist.
	if err := os.MkdirAll(filepath.Join(filepath.Dir(f.path)), 0700); err != nil {
		return err
	}

	// Open file handler for appending.
	file, err := os.OpenFile(f.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	f.file = file

	// Ensure header byte exists.
	f.size = 0
	if fi, err := file.Stat(); err != nil {
		return err
	} else if fi.Size() > 0 {
		f.size = fi.Size()
	} else {
		if _, err := f.file.Write([]byte{0}); err != nil {
			return err
		}
		f.size = 1
	}

	// Wrap file write a bufferred writer.
	f.w = bufio.NewWriter(f.file)

	// Memory map file data.
	data, err := mmap.Map(f.path, f.MaxSize)
	if err != nil {
		return fmt.Errorf("open series file: %v", err)
	}
	f.data = data

	// Load series map.
	m := newSeriesMap(f.path+SeriesMapFileSuffix, f)
	if err := m.open(); err != nil {
		return err
	}
	f.seriesMap = m

	return nil
}

// Close unmaps the data file.
func (f *SeriesFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.data != nil {
		if err := mmap.Unmap(f.data); err != nil {
			return err
		}
		f.data = nil
	}

	if f.file != nil {
		if err := f.file.Close(); err != nil {
			return err
		}
		f.file = nil
	}

	if f.seriesMap != nil {
		if err := f.seriesMap.close(); err != nil {
			return err
		}
		f.seriesMap = nil
	}

	return nil
}

// Path returns the path to the file.
func (f *SeriesFile) Path() string { return f.path }

// CreateSeriesListIfNotExists creates a list of series in bulk if they don't exist. Returns the offset of the series.
func (f *SeriesFile) CreateSeriesListIfNotExists(names [][]byte, tagsSlice []models.Tags, buf []byte) (offsets []uint64, err error) {
	var createRequired bool

	type byteRange struct {
		offset, size uint64
	}
	newKeyRanges := make([]byteRange, 0, len(names))

	// Find existing series under read-only lock.
	f.mu.RLock()
	offsets = make([]uint64, len(names))
	for i := range names {
		offset := f.offset(names[i], tagsSlice[i], buf)
		if offset == 0 {
			createRequired = true
		}
		offsets[i] = offset
	}
	f.mu.RUnlock()

	// Return immediately if no series need to be created.
	if !createRequired {
		return offsets, nil
	}

	// Obtain write lock to create new series.
	f.mu.Lock()
	defer f.mu.Unlock()

	// Track offsets of duplicate series.
	newOffsets := make(map[string]uint64, len(offsets))

	for i := range names {
		// Skip series that have already been created.
		if offset := offsets[i]; offset != 0 {
			continue
		}

		// Generate series key.
		buf = AppendSeriesKey(buf[:0], names[i], tagsSlice[i])

		// Re-attempt lookup under write lock.
		if offsets[i] = newOffsets[string(buf)]; offsets[i] != 0 {
			continue
		} else if offsets[i] = f.offset(names[i], tagsSlice[i], buf); offsets[i] != 0 {
			continue
		}

		// Append flag byte.
		if _, err := f.w.Write([]byte{0}); err != nil {
			return nil, err
		}
		f.size += SeriesFileFlagSize

		// Append series to the end of the file.
		offset := uint64(f.size)
		if _, err := f.w.Write(buf); err != nil {
			return nil, err
		}
		newOffsets[string(buf)] = offset

		// Move current offset to the end.
		sz := int64(len(buf))
		f.size += sz

		// Append new key to be added to hash map after flush.
		offsets[i] = offset
		newKeyRanges = append(newKeyRanges, byteRange{offset, uint64(sz)})
	}

	// Flush writer.
	if err := f.w.Flush(); err != nil {
		return nil, err
	}

	// Add keys to hash map(s).
	for _, keyRange := range newKeyRanges {
		key := f.data[keyRange.offset : keyRange.offset+keyRange.size]

		f.seriesMap.inmem.Put(key, keyRange.offset)

		if f.compactingSeriesMap != nil {
			f.compactingSeriesMap.inmem.Put(key, keyRange.offset)
		}
	}

	// Begin compaction if in-memory map is past threshold.
	if f.seriesMap.inmem.Len() >= SeriesMapThreshold {
		if err := f.compactSeriesMap(); err != nil {
			return nil, err
		}
	}

	return offsets, nil
}

// DeleteSeriesID flags a series as permanently deleted.
// If the series is reintroduced later then it must create a new offset.
func (f *SeriesFile) DeleteSeriesID(offset uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Already tombstoned, ignore.
	if _, ok := f.seriesMap.tombstones[offset]; ok {
		return nil
	}

	// Write tombstone entry.
	if _, err := f.w.Write([]byte{SeriesFileTombstoneFlag}); err != nil {
		return err
	} else if err := binary.Write(f.w, binary.BigEndian, offset); err != nil {
		return err
	} else if err := f.w.Flush(); err != nil {
		return err
	}
	f.size += SeriesFileTombstoneFlag + 8

	// Mark tombstone in memory.
	f.seriesMap.tombstones[offset] = struct{}{}

	return nil
}

// IsDeleted returns true if the ID has been deleted before.
func (f *SeriesFile) IsDeleted(offset uint64) bool {
	f.mu.RLock()
	_, ok := f.seriesMap.tombstones[offset]
	f.mu.RUnlock()
	return ok
}

// Offset returns the byte offset of the series within the block.
func (f *SeriesFile) Offset(name []byte, tags models.Tags, buf []byte) (offset uint64) {
	f.mu.RLock()
	offset = f.offset(name, tags, buf)
	f.mu.RUnlock()
	return offset
}

func (f *SeriesFile) offset(name []byte, tags models.Tags, buf []byte) uint64 {
	offset := f.seriesMap.offset(AppendSeriesKey(buf[:0], name, tags))
	if offset == 0 {
		return 0
	} else if _, ok := f.seriesMap.tombstones[offset]; ok {
		return 0
	}
	return offset
}

// SeriesKey returns the series key for a given offset.
func (f *SeriesFile) SeriesKey(offset uint64) []byte {
	if offset == 0 {
		return nil
	}

	buf := f.data[offset:]
	v, n := binary.Uvarint(buf)
	return buf[:n+int(v)]
}

// Series returns the parsed series name and tags for an offset.
func (f *SeriesFile) Series(offset uint64) ([]byte, models.Tags) {
	key := f.SeriesKey(offset)
	if key == nil {
		return nil, nil
	}
	return ParseSeriesKey(key)
}

// HasSeries return true if the series exists.
func (f *SeriesFile) HasSeries(name []byte, tags models.Tags, buf []byte) bool {
	return f.Offset(name, tags, buf) > 0
}

// SeriesCount returns the number of series.
func (f *SeriesFile) SeriesCount() uint64 {
	f.mu.RLock()
	n := uint64(f.seriesMap.n + f.seriesMap.inmem.Len())
	f.mu.RUnlock()
	return n
}

// SeriesIterator returns an iterator over all the series.
func (f *SeriesFile) SeriesIDIterator() SeriesIDIterator {
	return &seriesFileIterator{
		offset: 1,
		data:   f.data[1:f.size],
	}
}

// Backup writes the series file to w.
func (f *SeriesFile) Backup(w io.Writer) error {
	dir, err := ioutil.TempDir("", "influxd-series-file")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// Create a hard link to the current series file.
	tmpFilename := filepath.Join(dir, SeriesFileName)
	if err := func() error {
		f.mu.RLock()
		defer f.mu.RUnlock()

		if err := os.Link(f.path, tmpFilename); err != nil {
			return fmt.Errorf("error creating series file hard link: %q", err)
		}
		return nil
	}(); err != nil {
		return err
	}

	// Read hard linked file.
	file, err := os.Open(tmpFilename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Copy to outgoing writer.
	if _, err := io.Copy(w, file); err != nil {
		return err
	}

	return nil
}

// Restore appends new data from w to the end of the series file.
func (f *SeriesFile) Restore(r io.Reader) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Ensure the beginning of the file matches the restore.
	if h0, err := hashReader(bytes.NewReader(f.data[:f.size])); err != nil {
		return err
	} else if h1, err := hashReader(io.LimitReader(r, f.size)); err != nil {
		return err
	} else if !bytes.Equal(h0, h1) {
		return fmt.Errorf("restoring series file doesn't match first %d bytes", f.size)
	}

	// Copy the rest of the reader to the end of the series file.
	// We need to keep the mmapped so we're not able copy elsewhere and swap.
	// If an error occurs then we'll attempt to truncate to the original size.
	size := f.size
	if _, err := io.Copy(f.file, r); err != nil {
		f.file.Truncate(size)
		return err
	}

	// Reopen series map.
	if err := f.seriesMap.close(); err != nil {
		return err
	}
	f.seriesMap = newSeriesMap(f.path+SeriesMapFileSuffix, f)
	if err := f.seriesMap.open(); err != nil {
		return err
	}

	return nil
}

func (f *SeriesFile) compactSeriesMap() error {
	// TEMP: Compaction should occur in parallel.

	// Encode to a new buffer.
	buf := encodeSeriesMap(f.data[:f.size], f.seriesMap.n+f.seriesMap.inmem.Len())

	// Open temporary file.
	path := f.seriesMap.path
	compactionPath := path + ".compacting"
	file, err := os.Create(compactionPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write map to disk & close.
	if _, err := file.Write(buf); err != nil {
		return err
	} else if err := file.Close(); err != nil {
		return err
	}

	// Close series map.
	if err := f.seriesMap.close(); err != nil {
		return err
	}

	// Swap map to new location.
	if err := os.Rename(compactionPath, path); err != nil {
		return err
	}

	// Re-open series map.
	f.seriesMap = newSeriesMap(path, f)
	if err := f.seriesMap.open(); err != nil {
		return err
	}

	return nil
}

// seriesFileIterator is an iterator over a series ids in a series list.
type seriesFileIterator struct {
	data   []byte
	offset uint64
}

// Next returns the next series element.
func (itr *seriesFileIterator) Next() (SeriesIDElem, error) {
	for {
		if len(itr.data) == 0 {
			return SeriesIDElem{}, nil
		}

		// Read flag.
		flag := itr.data[0]
		itr.data = itr.data[1:]
		itr.offset++

		switch flag {
		case SeriesFileTombstoneFlag:
			itr.data = itr.data[8:] // skip
			itr.offset += 8
		default:
			var key []byte
			key, itr.data = ReadSeriesKey(itr.data)

			elem := SeriesIDElem{SeriesID: itr.offset}
			itr.offset += uint64(len(key))
			return elem, nil
		}
	}
}

func (itr *seriesFileIterator) Close() error { return nil }

// AppendSeriesKey serializes name and tags to a byte slice.
// The total length is prepended as a uvarint.
func AppendSeriesKey(dst []byte, name []byte, tags models.Tags) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	origLen := len(dst)

	// The tag count is variable encoded, so we need to know ahead of time what
	// the size of the tag count value will be.
	tcBuf := make([]byte, binary.MaxVarintLen64)
	tcSz := binary.PutUvarint(tcBuf, uint64(len(tags)))

	// Size of name/tags. Does not include total length.
	size := 0 + //
		2 + // size of measurement
		len(name) + // measurement
		tcSz + // size of number of tags
		(4 * len(tags)) + // length of each tag key and value
		tags.Size() // size of tag keys/values

	// Variable encode length.
	totalSz := binary.PutUvarint(buf, uint64(size))

	// If caller doesn't provide a buffer then pre-allocate an exact one.
	if dst == nil {
		dst = make([]byte, 0, size+totalSz)
	}

	// Append total length.
	dst = append(dst, buf[:totalSz]...)

	// Append name.
	binary.BigEndian.PutUint16(buf, uint16(len(name)))
	dst = append(dst, buf[:2]...)
	dst = append(dst, name...)

	// Append tag count.
	dst = append(dst, tcBuf[:tcSz]...)

	// Append tags.
	for _, tag := range tags {
		binary.BigEndian.PutUint16(buf, uint16(len(tag.Key)))
		dst = append(dst, buf[:2]...)
		dst = append(dst, tag.Key...)

		binary.BigEndian.PutUint16(buf, uint16(len(tag.Value)))
		dst = append(dst, buf[:2]...)
		dst = append(dst, tag.Value...)
	}

	// Verify that the total length equals the encoded byte count.
	if got, exp := len(dst)-origLen, size+totalSz; got != exp {
		panic(fmt.Sprintf("series key encoding does not match calculated total length: actual=%d, exp=%d, key=%x", got, exp, dst))
	}

	return dst
}

// ReadSeriesKey returns the series key from the beginning of the buffer.
func ReadSeriesKey(data []byte) (key, remainder []byte) {
	sz, n := binary.Uvarint(data)
	return data[:int(sz)+n], data[int(sz)+n:]
}

func ReadSeriesKeyLen(data []byte) (sz int, remainder []byte) {
	sz64, i := binary.Uvarint(data)
	return int(sz64), data[i:]
}

func ReadSeriesKeyMeasurement(data []byte) (name, remainder []byte) {
	n, data := binary.BigEndian.Uint16(data), data[2:]
	return data[:n], data[n:]
}

func ReadSeriesKeyTagN(data []byte) (n int, remainder []byte) {
	n64, i := binary.Uvarint(data)
	return int(n64), data[i:]
}

func ReadSeriesKeyTag(data []byte) (key, value, remainder []byte) {
	n, data := binary.BigEndian.Uint16(data), data[2:]
	key, data = data[:n], data[n:]

	n, data = binary.BigEndian.Uint16(data), data[2:]
	value, data = data[:n], data[n:]
	return key, value, data
}

// ParseSeriesKey extracts the name & tags from a series key.
func ParseSeriesKey(data []byte) (name []byte, tags models.Tags) {
	_, data = ReadSeriesKeyLen(data)
	name, data = ReadSeriesKeyMeasurement(data)

	tagN, data := ReadSeriesKeyTagN(data)
	tags = make(models.Tags, tagN)
	for i := 0; i < tagN; i++ {
		var key, value []byte
		key, value, data = ReadSeriesKeyTag(data)
		tags[i] = models.Tag{Key: key, Value: value}
	}

	return name, tags
}

func CompareSeriesKeys(a, b []byte) int {
	// Handle 'nil' keys.
	if len(a) == 0 && len(b) == 0 {
		return 0
	} else if len(a) == 0 {
		return -1
	} else if len(b) == 0 {
		return 1
	}

	// Read total size.
	_, a = ReadSeriesKeyLen(a)
	_, b = ReadSeriesKeyLen(b)

	// Read names.
	name0, a := ReadSeriesKeyMeasurement(a)
	name1, b := ReadSeriesKeyMeasurement(b)

	// Compare names, return if not equal.
	if cmp := bytes.Compare(name0, name1); cmp != 0 {
		return cmp
	}

	// Read tag counts.
	tagN0, a := ReadSeriesKeyTagN(a)
	tagN1, b := ReadSeriesKeyTagN(b)

	// Compare each tag in order.
	for i := 0; ; i++ {
		// Check for EOF.
		if i == tagN0 && i == tagN1 {
			return 0
		} else if i == tagN0 {
			return -1
		} else if i == tagN1 {
			return 1
		}

		// Read keys.
		var key0, key1, value0, value1 []byte
		key0, value0, a = ReadSeriesKeyTag(a)
		key1, value1, b = ReadSeriesKeyTag(b)

		// Compare keys & values.
		if cmp := bytes.Compare(key0, key1); cmp != 0 {
			return cmp
		} else if cmp := bytes.Compare(value0, value1); cmp != 0 {
			return cmp
		}
	}
}

type seriesKeys [][]byte

func (a seriesKeys) Len() int      { return len(a) }
func (a seriesKeys) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a seriesKeys) Less(i, j int) bool {
	return CompareSeriesKeys(a[i], a[j]) == -1
}

const (
	SeriesMapFileSuffix = "map"

	SeriesMapLoadFactor = 90

	SeriesMapCountSize     = 8
	SeriesMapMaxOffsetSize = 8
	SeriesMapHeaderSize    = SeriesMapCountSize + SeriesMapMaxOffsetSize

	SeriesMapElemSize = 8 + 8 // hash + value
)

// seriesMap represents a read-only hash map of series offsets.
type seriesMap struct {
	path       string
	sfile      *SeriesFile
	inmem      *rhh.HashMap
	tombstones map[uint64]struct{}

	n         int64
	maxOffset uint64
	capacity  int64
	data      []byte
	mask      int64
}

func newSeriesMap(path string, sfile *SeriesFile) *seriesMap {
	return &seriesMap{
		path:       path,
		sfile:      sfile,
		tombstones: make(map[uint64]struct{}),
	}
}

func (m *seriesMap) open() error {
	// Memory map file data.
	data, err := mmap.Map(m.path, 0)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	m.data = data

	// Read header if available.
	if len(m.data) > 0 {
		buf := data
		m.n, buf = int64(binary.BigEndian.Uint64(buf)), buf[SeriesMapCountSize:]
		m.maxOffset, buf = uint64(binary.BigEndian.Uint64(buf)), buf[SeriesMapMaxOffsetSize:]
		m.capacity = int64(len(buf) / SeriesMapElemSize)
		m.mask = int64(m.capacity - 1)
	} else {
		m.n, m.maxOffset = 0, 1
	}

	// Index all data created after the on-disk hash map.
	inmem := rhh.NewHashMap(rhh.DefaultOptions)
	tombstones := make(map[uint64]struct{})
	for b, offset := m.sfile.data[m.maxOffset:m.sfile.size], m.maxOffset; len(b) > 0; {
		// Read flag.
		flag := b[0]
		b, offset = b[1:], offset+1

		switch flag {
		case SeriesFileTombstoneFlag:
			seriesID := binary.BigEndian.Uint64(b[:8])
			b = b[8:]
			tombstones[seriesID] = struct{}{}
		default:
			var key []byte
			key, b = ReadSeriesKey(b)
			inmem.Put(key, offset)
			offset += uint64(len(key))
		}
	}
	m.inmem = inmem
	m.tombstones = tombstones

	return nil
}

func (m *seriesMap) close() error {
	if m.data != nil {
		if err := mmap.Unmap(m.data); err != nil {
			return err
		}
		m.data = nil
	}
	return nil
}

// offset finds the series key's offset in either the on-disk or in-memory hash maps.
func (m *seriesMap) offset(key []byte) uint64 {
	offset, _ := m.inmem.Get(key).(uint64)
	if _, ok := m.tombstones[offset]; ok {
		return 0
	} else if offset != 0 {
		return offset
	}
	return m.onDiskOffset(key)
}

func (m *seriesMap) onDiskOffset(key []byte) uint64 {
	if len(m.data) == 0 {
		return 0
	}

	hash := rhh.HashKey(key)
	for d, pos := int64(0), hash&m.mask; ; d, pos = d+1, (pos+1)&m.mask {
		elem := m.data[SeriesMapHeaderSize+(pos*SeriesMapElemSize):]
		elem = elem[:SeriesMapElemSize]

		h := int64(binary.BigEndian.Uint64(elem[:8]))
		if h == 0 || d > rhh.Dist(h, pos, m.capacity) {
			return 0
		} else if h == hash {
			if v := binary.BigEndian.Uint64(elem[8:]); bytes.Equal(m.sfile.SeriesKey(v), key) {
				return v
			}
		}
	}
}

// encodeSeriesMap encodes series file data into a series map.
func encodeSeriesMap(src []byte, n int64) []byte {
	capacity := (n * 100) / SeriesMapLoadFactor
	capacity = pow2(capacity)

	// Build output buffer with count and max offset at the beginning.
	buf := make([]byte, SeriesMapHeaderSize+(capacity*SeriesMapElemSize))
	binary.BigEndian.PutUint64(buf[0:8], uint64(n))
	binary.BigEndian.PutUint64(buf[8:16], uint64(len(src)))

	// Loop over all series in data. Offset starts at 1.
	for b, offset := src[1:], uint64(1); len(b) > 0; {
		var key []byte
		key, b = ReadSeriesKey(b)

		insertSeriesMap(src, buf, key, offset, capacity)
		offset += uint64(len(key))
	}

	return buf
}

func insertSeriesMap(src, buf, key []byte, val uint64, capacity int64) {
	mask := int64(capacity - 1)
	hash := rhh.HashKey(key)

	// Continue searching until we find an empty slot or lower probe distance.
	for dist, pos := int64(0), hash&mask; ; dist, pos = dist+1, (pos+1)&mask {
		elem := buf[SeriesMapHeaderSize+(pos*SeriesMapElemSize):]
		elem = elem[:SeriesMapElemSize]

		h := int64(binary.BigEndian.Uint64(elem[:8]))
		v := binary.BigEndian.Uint64(elem[8:])
		k, _ := ReadSeriesKey(src[v:])

		// Empty slot found or matching key, insert and exit.
		if h == 0 || bytes.Equal(key, k) {
			binary.BigEndian.PutUint64(elem[:8], uint64(hash))
			binary.BigEndian.PutUint64(elem[8:], val)
			return
		}

		// If the existing elem has probed less than us, then swap places with
		// existing elem, and keep going to find another slot for that elem.
		if d := rhh.Dist(h, pos, capacity); d < dist {
			// Insert current values.
			binary.BigEndian.PutUint64(elem[:8], uint64(hash))
			binary.BigEndian.PutUint64(elem[8:], val)

			// Swap with values in that position.
			hash, key, val = h, k, v

			// Update current distance.
			dist = d
		}
	}
}

// pow2 returns the number that is the next highest power of 2.
// Returns v if it is a power of 2.
func pow2(v int64) int64 {
	for i := int64(2); i < 1<<62; i *= 2 {
		if i >= v {
			return i
		}
	}
	panic("unreachable")
}

// hashReader generates an xxhash from the contents of r.
func hashReader(r io.Reader) ([]byte, error) {
	h := xxhash.New()
	if _, err := io.Copy(h, r); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}
