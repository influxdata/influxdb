package tsi1

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/mmap"
	"github.com/influxdata/influxdb/pkg/rhh"
)

// ErrSeriesOverflow is returned when too many series are added to a series writer.
var ErrSeriesOverflow = errors.New("series overflow")

// Series list field size constants.
const SeriesIDSize = 4

// Series flag constants.
const (
	// Marks the series as having been deleted.
	SeriesTombstoneFlag = 0x01

	// Marks the following bytes as a hash index.
	// These bytes should be skipped by an iterator.
	SeriesHashIndexFlag = 0x02
)

const DefaultMaxSeriesFileSize = 32 * (1 << 30) // 32GB

// MaxSeriesFileHashSize is the maximum number of series in a single hash.
const MaxSeriesFileHashSize = (1048576 * LoadFactor) / 100

// SeriesFile represents the section of the index that holds series data.
type SeriesFile struct {
	mu   sync.RWMutex
	path string
	data []byte
	file *os.File
	w    *bufio.Writer

	n       uint32
	hashMap *rhh.HashMap

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
	// Open file handler for appending.
	file, err := os.OpenFile(f.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	f.file = file

	// Ensure header byte exists.
	f.n = 0
	if fi, err := file.Stat(); err != nil {
		return err
	} else if fi.Size() > 0 {
		f.n = uint32(fi.Size())
	} else {
		if _, err := f.file.Write([]byte{0}); err != nil {
			return err
		}
		f.n = 1
	}

	// Wrap file write a bufferred writer.
	f.w = bufio.NewWriter(f.file)

	// Memory map file data.
	data, err := mmap.Map(f.path, f.MaxSize)
	if err != nil {
		return err
	}
	f.data = data

	// Index all series.
	if err := f.reindex(); err != nil {
		return err
	}

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

	f.hashMap = nil

	return nil
}

// Path returns the path to the file.
func (f *SeriesFile) Path() string { return f.path }

// reindex iterates over all series and builds a hash map index.
func (f *SeriesFile) reindex() error {
	m := rhh.NewHashMap(rhh.DefaultOptions)

	// Series data begins with an offset of 1.
	data := f.data[1:f.n]
	offset := uint32(1)

	for len(data) > 0 {
		var key []byte
		key, data = ReadSeriesKey(data)

		m.Put(key, offset)
		offset += uint32(len(key))
	}

	f.hashMap = m

	return nil
}

// CreateSeriesListIfNotExists creates a list of series in bulk if they don't exist. Returns the offset of the series.
func (f *SeriesFile) CreateSeriesListIfNotExists(names [][]byte, tagsSlice []models.Tags, buf []byte) (offsets []uint32, err error) {
	var createRequired bool

	type byteRange struct {
		offset, size uint32
	}
	newKeyRanges := make([]byteRange, 0, len(names))

	// Find existing series under read-only lock.
	f.mu.RLock()
	offsets = make([]uint32, len(names))
	for i := range names {
		offsets[i] = f.offset(names[i], tagsSlice[i], buf)
		if offsets[i] == 0 {
			createRequired = true
		}
	}
	f.mu.RUnlock()

	// Return immediately if no series need to be created.
	if !createRequired {
		return offsets, nil
	}

	// Obtain write lock to create new series.
	f.mu.Lock()
	defer f.mu.Unlock()

	for i := range names {
		// Skip series that have already been created.
		if offsets[i] != 0 {
			continue
		}

		// Re-attempt lookup under write lock.
		if offsets[i] = f.offset(names[i], tagsSlice[i], buf); offsets[i] != 0 {
			continue
		}

		// Save current file offset.
		offset := f.n

		// Append series to the end of the file.
		buf = AppendSeriesKey(buf[:0], names[i], tagsSlice[i])
		if _, err := f.w.Write(buf); err != nil {
			return nil, err
		}

		// Move current offset to the end.
		sz := uint32(len(buf))
		f.n += sz

		// Append new key to be added to hash map after flush.
		offsets[i] = offset
		newKeyRanges = append(newKeyRanges, byteRange{offset, sz})
	}

	// Flush writer.
	if err := f.w.Flush(); err != nil {
		return nil, err
	}

	// Add keys to hash map.
	for _, keyRange := range newKeyRanges {
		f.hashMap.Put(f.data[keyRange.offset:keyRange.offset+keyRange.size], keyRange.offset)
	}

	return offsets, nil
}

// Offset returns the byte offset of the series within the block.
func (f *SeriesFile) Offset(name []byte, tags models.Tags, buf []byte) (offset uint32) {
	f.mu.RLock()
	offset = f.offset(name, tags, buf)
	f.mu.RUnlock()
	return offset
}

func (f *SeriesFile) offset(name []byte, tags models.Tags, buf []byte) uint32 {
	offset, _ := f.hashMap.Get(AppendSeriesKey(buf[:0], name, tags)).(uint32)
	return offset
}

// SeriesKey returns the series key for a given offset.
func (f *SeriesFile) SeriesKey(offset uint32) []byte {
	if offset == 0 {
		return nil
	}

	buf := f.data[offset:]
	v, n := binary.Uvarint(buf)
	return buf[:n+int(v)]
}

// Series returns the parsed series name and tags for an offset.
func (f *SeriesFile) Series(offset uint32) ([]byte, models.Tags) {
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
func (f *SeriesFile) SeriesCount() uint32 {
	return uint32(f.hashMap.Len())
}

// SeriesIterator returns an iterator over all the series.
func (f *SeriesFile) SeriesIDIterator() SeriesIDIterator {
	return &seriesFileIterator{
		offset: 1,
		data:   f.data[1:f.n],
	}
}

// seriesFileIterator is an iterator over a series ids in a series list.
type seriesFileIterator struct {
	data   []byte
	offset uint32
}

// Next returns the next series element.
func (itr *seriesFileIterator) Next() SeriesIDElem {
	if len(itr.data) == 0 {
		return SeriesIDElem{}
	}

	var key []byte
	key, itr.data = ReadSeriesKey(itr.data)

	elem := SeriesIDElem{SeriesID: itr.offset}
	itr.offset += uint32(len(key))
	return elem
}

// AppendSeriesKey serializes name and tags to a byte slice.
// The total length is prepended as a uvarint.
func AppendSeriesKey(dst []byte, name []byte, tags models.Tags) []byte {
	buf := make([]byte, binary.MaxVarintLen32)
	origLen := len(dst)

	// The tag count is variable encoded, so we need to know ahead of time what
	// the size of the tag count value will be.
	tcBuf := make([]byte, binary.MaxVarintLen32)
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
