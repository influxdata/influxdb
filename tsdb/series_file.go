package tsdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
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

const SeriesFileVersion = 1

// Series flag constants.
const (
	SeriesFileFlagSize      = 1
	SeriesFileInsertFlag    = 0x00
	SeriesFileTombstoneFlag = 0x01
)

// SeriesMapThreshold is the number of series IDs to hold in the in-memory
// series map before compacting and rebuilding the on-disk representation.
const SeriesMapThreshold = 1 << 25 // ~33M ids * 8 bytes per id == 256MB

// SeriesFile represents the section of the index that holds series data.
type SeriesFile struct {
	mu   sync.RWMutex
	path string

	mmap []byte        // entire mmapped file
	data []byte        // active part of mmap file
	file *os.File      // write file handle
	w    *bufio.Writer // bufferred file handle
	size int64         // current file size
	seq  uint64        // series id sequence

	log         []byte
	keyIDMap    *seriesKeyIDMap
	idOffsetMap *seriesIDOffsetMap
	walOffset   int64
	tombstones  map[uint64]struct{}

	// MaxSize is the maximum size of the file.
	MaxSize int64
}

// NewSeriesFile returns a new instance of SeriesFile.
func NewSeriesFile(path string) *SeriesFile {
	return &SeriesFile{
		path:       path,
		tombstones: make(map[uint64]struct{}),

		MaxSize: DefaultMaxSeriesFileSize,
	}
}

// Open memory maps the data file at the file's path.
func (f *SeriesFile) Open() error {
	// Create the parent directories if they don't exist.
	if err := os.MkdirAll(filepath.Join(filepath.Dir(f.path)), 0777); err != nil {
		return err
	}

	// Open components.
	if err := func() (err error) {
		// Open file handler for appending.
		if f.file, err = os.OpenFile(f.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666); err != nil {
			return err
		}

		// Read file size.
		// If file is empty then write an empty header.
		fi, err := f.file.Stat()
		if err != nil {
			return err
		} else if fi.Size() > 0 {
			f.size = fi.Size()
		} else {
			hdr := NewSeriesFileHeader()
			if f.size, err = hdr.WriteTo(f.file); err != nil {
				return err
			}
		}
		f.w = bufio.NewWriter(f.file)

		// Memory map file data.
		if f.mmap, err = mmap.Map(f.path, f.MaxSize); err != nil {
			return err
		}
		f.data = f.mmap[:f.size]

		// Read header.
		hdr, err := ReadSeriesFileHeader(f.data)
		if err != nil {
			return err
		}

		// Subslice log & maps.
		f.log = f.data[hdr.Log.Offset : hdr.Log.Offset+hdr.Log.Size]
		f.keyIDMap = newSeriesKeyIDMap(f.data, f.data[hdr.KeyIDMap.Offset:hdr.KeyIDMap.Offset+hdr.KeyIDMap.Size])
		f.idOffsetMap = newSeriesIDOffsetMap(f.data, f.data[hdr.IDOffsetMap.Offset:hdr.IDOffsetMap.Offset+hdr.IDOffsetMap.Size])
		f.walOffset = hdr.WAL.Offset

		// Replay post-compaction log.
		for off := f.walOffset; off < f.size; {
			flag, id, key, sz := ReadSeriesFileLogEntry(f.data[off:])

			switch flag {
			case SeriesFileInsertFlag:
				f.keyIDMap.insert(key, id)
				f.idOffsetMap.insert(id, off+SeriesFileLogInsertEntryHeader)

			case SeriesFileTombstoneFlag:
				f.tombstones[id] = struct{}{}

			default:
				return fmt.Errorf("tsdb.SeriesFile.Open(): unknown log entry flag: %d", flag)
			}

			off += sz
		}

		return nil
	}(); err != nil {
		f.Close()
		return err
	}

	return nil
}

// Close unmaps the data file.
func (f *SeriesFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mmap != nil {
		mmap.Unmap(f.mmap)
		f.mmap = nil
		f.data = nil
	}
	if f.file != nil {
		f.file.Close()
		f.file = nil
	}
	f.w = nil

	if f.keyIDMap != nil {
		f.keyIDMap.Close()
		f.keyIDMap = nil
	}
	if f.idOffsetMap != nil {
		f.idOffsetMap.Close()
		f.idOffsetMap = nil
	}

	f.log = nil
	f.tombstones = nil

	return nil
}

// Path returns the path to the file.
func (f *SeriesFile) Path() string { return f.path }

// CreateSeriesListIfNotExists creates a list of series in bulk if they don't exist. Returns the offset of the series.
func (f *SeriesFile) CreateSeriesListIfNotExists(names [][]byte, tagsSlice []models.Tags, buf []byte) (ids []uint64, err error) {
	f.mu.RLock()
	ids, ok := f.findIDListByNameTags(names, tagsSlice, buf)
	if ok {
		f.mu.RUnlock()
		return ids, nil
	}
	f.mu.RUnlock()

	type keyRange struct {
		id     uint64
		offset int64
		size   int64
	}
	newKeyRanges := make([]keyRange, 0, len(names))

	// Obtain write lock to create new series.
	f.mu.Lock()
	defer f.mu.Unlock()

	// Track offsets of duplicate series.
	newIDs := make(map[string]uint64, len(ids))

	for i := range names {
		// Skip series that have already been created.
		if id := ids[i]; id != 0 {
			continue
		}

		// Generate series key.
		buf = AppendSeriesKey(buf[:0], names[i], tagsSlice[i])

		// Re-attempt lookup under write lock.
		if ids[i] = newIDs[string(buf)]; ids[i] != 0 {
			continue
		} else if ids[i] = f.findIDByNameTags(names[i], tagsSlice[i], buf); ids[i] != 0 {
			continue
		}

		// Write to series log and save offset.
		id, offset, err := f.insert(buf)
		if err != nil {
			return nil, err
		}

		// Append new key to be added to hash map after flush.
		ids[i] = id
		newIDs[string(buf)] = id
		newKeyRanges = append(newKeyRanges, keyRange{id, offset, f.size - offset})
	}

	// Flush log writes so we can access data in mmap.
	if err := f.w.Flush(); err != nil {
		return nil, err
	}

	// Add keys to hash map(s).
	for _, keyRange := range newKeyRanges {
		key := f.data[keyRange.offset : keyRange.offset+keyRange.size]
		f.keyIDMap.insert(key, keyRange.id)
		f.idOffsetMap.insert(keyRange.id, keyRange.offset)
	}

	return ids, nil
}

func (f *SeriesFile) findIDByNameTags(name []byte, tags models.Tags, buf []byte) uint64 {
	id := f.keyIDMap.get(AppendSeriesKey(buf[:0], name, tags))
	if _, ok := f.tombstones[id]; ok {
		return 0
	}
	return id
}

func (f *SeriesFile) findIDListByNameTags(names [][]byte, tagsSlice []models.Tags, buf []byte) (ids []uint64, ok bool) {
	ids, ok = make([]uint64, len(names)), true
	for i := range names {
		id := f.findIDByNameTags(names[i], tagsSlice[i], buf)
		if id == 0 {
			ok = false
			continue
		}
		ids[i] = id
	}
	return ids, ok
}

// DeleteSeriesID flags a series as permanently deleted.
// If the series is reintroduced later then it must create a new id.
func (f *SeriesFile) DeleteSeriesID(id uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Already tombstoned, ignore.
	if _, ok := f.tombstones[id]; ok {
		return nil
	}

	// Write tombstone entry.
	var buf bytes.Buffer
	buf.WriteByte(SeriesFileTombstoneFlag)
	binary.Write(&buf, binary.BigEndian, id)

	bufN := buf.Len()
	if _, err := buf.WriteTo(f.w); err != nil {
		return err
	} else if err := f.w.Flush(); err != nil {
		return err
	}
	f.size += int64(bufN)
	f.data = f.data[:f.size]

	// Mark tombstone in memory.
	f.tombstones[id] = struct{}{}

	return nil
}

// IsDeleted returns true if the ID has been deleted before.
func (f *SeriesFile) IsDeleted(id uint64) bool {
	f.mu.RLock()
	_, v := f.tombstones[id]
	f.mu.RUnlock()
	return v
}

// SeriesKey returns the series key for a given id.
func (f *SeriesFile) SeriesKey(id uint64) []byte {
	if id == 0 {
		return nil
	}
	f.mu.RLock()
	key := f.seriesKeyByOffset(f.idOffsetMap.get(id))
	f.mu.RUnlock()
	return key
}

// Series returns the parsed series name and tags for an offset.
func (f *SeriesFile) Series(id uint64) ([]byte, models.Tags) {
	key := f.SeriesKey(id)
	if key == nil {
		return nil, nil
	}
	return ParseSeriesKey(key)
}

// SeriesID return the series id for the series.
func (f *SeriesFile) SeriesID(name []byte, tags models.Tags, buf []byte) uint64 {
	f.mu.RLock()
	id := f.keyIDMap.get(AppendSeriesKey(buf[:0], name, tags))
	f.mu.RUnlock()
	return id
}

// HasSeries return true if the series exists.
func (f *SeriesFile) HasSeries(name []byte, tags models.Tags, buf []byte) bool {
	f.mu.RLock()
	v := f.keyIDMap.get(AppendSeriesKey(buf[:0], name, tags)) > 0
	f.mu.RUnlock()
	return v
}

// SeriesCount returns the number of series.
func (f *SeriesFile) SeriesCount() uint64 {
	f.mu.RLock()
	n := f.idOffsetMap.count()
	f.mu.RUnlock()
	return n
}

// SeriesIterator returns an iterator over all the series.
func (f *SeriesFile) SeriesIDIterator() SeriesIDIterator {
	var ids []uint64
	ids = append(ids, ReadSeriesFileLogIDs(f.log)...)
	ids = append(ids, ReadSeriesFileLogIDs(f.data[f.walOffset:])...)

	sort.Slice(ids, func(i, j int) bool {
		keyi := f.SeriesKey(ids[i])
		keyj := f.SeriesKey(ids[j])
		return CompareSeriesKeys(keyi, keyj) == -1
	})

	return NewSeriesIDSliceIterator(ids)
}

func (f *SeriesFile) insert(key []byte) (id uint64, offset int64, err error) {
	id = f.seq + 1

	var buf bytes.Buffer
	buf.WriteByte(SeriesFileInsertFlag)      // flag
	binary.Write(&buf, binary.BigEndian, id) // new id

	// Save offset position.
	offset = f.size + int64(buf.Len())

	// Write series key.
	buf.Write(key)

	// Write buffer to underlying writer.
	bufN := buf.Len()
	if _, err := buf.WriteTo(f.w); err != nil {
		return 0, 0, err
	}
	f.seq++
	f.size += int64(bufN)
	f.data = f.data[:f.size]
	return id, offset, nil
}

func (f *SeriesFile) seriesKeyByOffset(offset int64) []byte {
	if offset == 0 || f.data == nil {
		return nil
	}
	key, _ := ReadSeriesKey(f.data[offset:])
	return key
}

const SeriesFileLogInsertEntryHeader = 1 + 8 // flag + id

func ReadSeriesFileLogEntry(data []byte) (flag uint8, id uint64, key []byte, sz int64) {
	flag, data = uint8(data[1]), data[1:]
	id, data = binary.BigEndian.Uint64(data), data[8:]
	switch flag {
	case SeriesFileInsertFlag:
		key, _ = ReadSeriesKey(data)
	}
	return flag, id, key, int64(SeriesFileLogInsertEntryHeader + len(key))
}

func ReadSeriesFileLogIDs(data []byte) []uint64 {
	var ids []uint64
	for len(data) > 0 {
		flag, id, _, sz := ReadSeriesFileLogEntry(data)
		if flag == SeriesFileInsertFlag {
			ids = append(ids, id)
		}
		data = data[sz:]
	}
	return ids
}

const SeriesFileMagic = uint32(0x49465346) // "IFSF"

var ErrInvalidSeriesFile = errors.New("invalid series file")

const SeriesFileHeaderSize = 0 +
	4 + 1 + // magic + version
	8 + 8 + // log
	8 + 8 + // key/id map
	8 + 8 + // id/offset map
	8 // wall offset

// SeriesFileHeader represents the version & position information of a series file.
type SeriesFileHeader struct {
	Version uint8

	Log struct {
		Offset int64
		Size   int64
	}

	KeyIDMap struct {
		Offset int64
		Size   int64
	}

	IDOffsetMap struct {
		Offset int64
		Size   int64
	}

	WAL struct {
		Offset int64
	}
}

// NewSeriesFileHeader returns a new instance of SeriesFileHeader.
func NewSeriesFileHeader() SeriesFileHeader {
	hdr := SeriesFileHeader{Version: SeriesFileVersion}
	hdr.Log.Offset = SeriesFileHeaderSize
	hdr.KeyIDMap.Offset = SeriesFileHeaderSize
	hdr.IDOffsetMap.Offset = SeriesFileHeaderSize
	hdr.WAL.Offset = SeriesFileHeaderSize
	return hdr
}

// ReadSeriesFileHeader returns the header from data.
func ReadSeriesFileHeader(data []byte) (hdr SeriesFileHeader, err error) {
	r := bytes.NewReader(data)
	if len(data) == 0 {
		return NewSeriesFileHeader(), nil
	}

	// Read magic number & version.
	var magic uint32
	if err := binary.Read(r, binary.BigEndian, &magic); err != nil {
		return hdr, err
	} else if magic != SeriesFileMagic {
		return hdr, ErrInvalidSeriesFile
	}
	if err := binary.Read(r, binary.BigEndian, &hdr.Version); err != nil {
		return hdr, err
	}

	// Read log position.
	if err := binary.Read(r, binary.BigEndian, &hdr.Log.Offset); err != nil {
		return hdr, err
	} else if err := binary.Read(r, binary.BigEndian, &hdr.Log.Size); err != nil {
		return hdr, err
	}

	// Read key/id map position.
	if err := binary.Read(r, binary.BigEndian, &hdr.KeyIDMap.Offset); err != nil {
		return hdr, err
	} else if err := binary.Read(r, binary.BigEndian, &hdr.KeyIDMap.Size); err != nil {
		return hdr, err
	}

	// Read offset/id map position.
	if err := binary.Read(r, binary.BigEndian, &hdr.IDOffsetMap.Offset); err != nil {
		return hdr, err
	} else if err := binary.Read(r, binary.BigEndian, &hdr.IDOffsetMap.Size); err != nil {
		return hdr, err
	}

	// Read WAL offset.
	if err := binary.Read(r, binary.BigEndian, &hdr.WAL.Offset); err != nil {
		return hdr, err
	}

	return hdr, nil
}

// WriteTo writes the trailer to w.
func (hdr *SeriesFileHeader) WriteTo(w io.Writer) (n int64, err error) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, SeriesFileMagic)
	binary.Write(&buf, binary.BigEndian, hdr.Version)
	binary.Write(&buf, binary.BigEndian, hdr.Log.Offset)
	binary.Write(&buf, binary.BigEndian, hdr.Log.Size)
	binary.Write(&buf, binary.BigEndian, hdr.KeyIDMap.Offset)
	binary.Write(&buf, binary.BigEndian, hdr.KeyIDMap.Size)
	binary.Write(&buf, binary.BigEndian, hdr.IDOffsetMap.Offset)
	binary.Write(&buf, binary.BigEndian, hdr.IDOffsetMap.Size)
	binary.Write(&buf, binary.BigEndian, hdr.WAL.Offset)
	return buf.WriteTo(w)
}

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
	seriesKeyIDMapHeaderSize = 16 // count + capacity
	seriesKeyIDMapElemSize   = 16 // offset + id
	seriesKeyIDMapLoadFactor = 90
)

// seriesKeyIDMap represents a fixed hash map of key-to-id.
type seriesKeyIDMap struct {
	src   []byte       // series key data
	data  []byte       // rhh map data
	inmem *rhh.HashMap // offset-to-id
}

func (m *seriesKeyIDMap) Close() error {
	m.inmem = nil
	return nil
}

func newSeriesKeyIDMap(src, data []byte) *seriesKeyIDMap {
	return &seriesKeyIDMap{
		src:   src,
		data:  data,
		inmem: rhh.NewHashMap(rhh.DefaultOptions),
	}
}

func (m *seriesKeyIDMap) count() uint64 {
	n := uint64(m.inmem.Len())
	if len(m.data) > 0 {
		n += binary.BigEndian.Uint64(m.data[:8])
	}
	return n
}

func (m *seriesKeyIDMap) insert(key []byte, id uint64) {
	m.inmem.Put(key, id)
}

func (m *seriesKeyIDMap) get(key []byte) uint64 {
	if v := m.inmem.Get(key); v != nil {
		if id, _ := v.(uint64); id != 0 {
			return id
		}
	}
	if len(m.data) == 0 {
		return 0
	}

	capacity := int64(binary.BigEndian.Uint64(m.data[8:]))
	mask := capacity - 1

	hash := rhh.HashKey(key)
	for d, pos := int64(0), hash&mask; ; d, pos = d+1, (pos+1)&mask {
		elem := m.data[seriesKeyIDMapHeaderSize+(pos*seriesKeyIDMapElemSize):]
		elemOffset := binary.BigEndian.Uint64(elem[:8])

		if elemOffset == 0 {
			return 0
		}

		elemKey, _ := ReadSeriesKey(m.src[elemOffset:])
		elemHash := rhh.HashKey(elemKey)
		if d > rhh.Dist(elemHash, pos, capacity) {
			return 0
		} else if elemHash == hash && bytes.Equal(elemKey, key) {
			return binary.BigEndian.Uint64(elem[8:])
		}
	}
}

const (
	seriesIDOffsetMapHeaderSize = 16 // count + capacity
	seriesIDOffsetMapElemSize   = 16 // id + offset
	seriesIDOffsetMapLoadFactor = 90
)

// seriesIDOffsetMap represents a fixed hash map of id-to-offset.
type seriesIDOffsetMap struct {
	src   []byte           // series key data
	data  []byte           // rhh map data
	inmem map[uint64]int64 // id-to-offset
}

func newSeriesIDOffsetMap(src, data []byte) *seriesIDOffsetMap {
	return &seriesIDOffsetMap{
		src:   src,
		data:  data,
		inmem: make(map[uint64]int64),
	}
}

func (m *seriesIDOffsetMap) Close() error {
	m.inmem = nil
	return nil
}

func (m *seriesIDOffsetMap) count() uint64 {
	n := uint64(len(m.inmem))
	if len(m.data) > 0 {
		n += binary.BigEndian.Uint64(m.data[:8])
	}
	return n
}

func (m *seriesIDOffsetMap) insert(id uint64, offset int64) {
	m.inmem[id] = offset
}

func (m *seriesIDOffsetMap) get(id uint64) int64 {
	if offset := m.inmem[id]; offset != 0 {
		return offset
	} else if len(m.data) == 0 {
		return 0
	}

	capacity := int64(binary.BigEndian.Uint64(m.data[8:]))
	mask := capacity - 1

	hash := rhh.HashUint64(id)
	for d, pos := int64(0), hash&mask; ; d, pos = d+1, (pos+1)&mask {
		elem := m.data[seriesIDOffsetMapHeaderSize+(pos*seriesIDOffsetMapElemSize):]
		elemID := binary.BigEndian.Uint64(elem[:8])

		if elemID == id {
			return int64(binary.BigEndian.Uint64(elem[8:]))
		} else if elemID == 0 || d > rhh.Dist(rhh.HashUint64(elemID), pos, capacity) {
			return 0
		}
	}
}

/*
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
*/

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
