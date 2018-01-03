package tsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/rhh"
	"go.uber.org/zap"
)

// SeriesIDSize is the size in bytes of a series key ID.
const SeriesIDSize = 8

// DefaultSeriesFileCompactThreshold is the number of series IDs to hold in the in-memory
// series map before compacting and rebuilding the on-disk representation.
const DefaultSeriesFileCompactThreshold = 1 << 20 // 1M

// SeriesFile represents the section of the index that holds series data.
type SeriesFile struct {
	mu   sync.RWMutex
	wg   sync.WaitGroup
	path string

	segments []*SeriesSegment
	index    *SeriesIndex
	seq      uint64 // series id sequence

	compacting bool

	CompactThreshold int

	Logger *zap.Logger
}

// NewSeriesFile returns a new instance of SeriesFile.
func NewSeriesFile(path string) *SeriesFile {
	return &SeriesFile{
		path:             path,
		CompactThreshold: DefaultSeriesFileCompactThreshold,
		Logger:           zap.NewNop(),
	}
}

// Open memory maps the data file at the file's path.
func (f *SeriesFile) Open() error {
	// Create path if it doesn't exist.
	if err := os.MkdirAll(filepath.Join(f.path), 0777); err != nil {
		return err
	}

	// Open components.
	if err := func() (err error) {
		if err := f.openSegments(); err != nil {
			return err
		}

		// Init last segment for writes.
		if err := f.activeSegment().InitForWrite(); err != nil {
			return err
		}

		f.index = NewSeriesIndex(f.IndexPath())
		if err := f.index.Open(); err != nil {
			return err
		} else if f.index.Recover(f.segments); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		f.Close()
		return err
	}

	return nil
}

func (f *SeriesFile) openSegments() error {
	fis, err := ioutil.ReadDir(f.path)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		segmentID, err := ParseSeriesSegmentFilename(fi.Name())
		if err != nil {
			continue
		}

		segment := NewSeriesSegment(segmentID, filepath.Join(f.path, fi.Name()))
		if err := segment.Open(); err != nil {
			return err
		}
		f.segments = append(f.segments, segment)
	}

	// Find max series id by searching segments in reverse order.
	for i := len(f.segments) - 1; i >= 0; i-- {
		if f.seq = f.segments[i].MaxSeriesID(); f.seq > 0 {
			break
		}
	}

	// Create initial segment if none exist.
	if len(f.segments) == 0 {
		segment, err := CreateSeriesSegment(0, filepath.Join(f.path, "0000"))
		if err != nil {
			return err
		}
		f.segments = append(f.segments, segment)
	}

	return nil
}

// Close unmaps the data file.
func (f *SeriesFile) Close() (err error) {
	f.wg.Wait()

	f.mu.Lock()
	defer f.mu.Unlock()

	for _, s := range f.segments {
		if e := s.Close(); e != nil && err == nil {
			err = e
		}
	}
	f.segments = nil

	if f.index != nil {
		if e := f.index.Close(); e != nil && err == nil {
			err = e
		}
	}
	f.index = nil

	return err
}

// Path returns the path to the file.
func (f *SeriesFile) Path() string { return f.path }

// Path returns the path to the series index.
func (f *SeriesFile) IndexPath() string { return filepath.Join(f.path, "index") }

// CreateSeriesListIfNotExists creates a list of series in bulk if they don't exist. Returns the offset of the series.
func (f *SeriesFile) CreateSeriesListIfNotExists(names [][]byte, tagsSlice []models.Tags, buf []byte) (ids []uint64, err error) {
	f.mu.RLock()
	ids, ok := f.index.FindIDListByNameTags(f.segments, names, tagsSlice, buf)
	if ok {
		f.mu.RUnlock()
		return ids, nil
	}
	f.mu.RUnlock()

	type keyRange struct {
		id     uint64
		offset int64
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
		} else if ids[i] = f.index.FindIDByNameTags(f.segments, names[i], tagsSlice[i], buf); ids[i] != 0 {
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
		newKeyRanges = append(newKeyRanges, keyRange{id, offset})
	}

	// Flush active segment writes so we can access data in mmap.
	if segment := f.activeSegment(); segment != nil {
		if err := segment.Flush(); err != nil {
			return nil, err
		}
	}

	// Add keys to hash map(s).
	for _, keyRange := range newKeyRanges {
		f.index.Insert(f.seriesKeyByOffset(keyRange.offset), keyRange.id, keyRange.offset)
	}

	// Check if we've crossed the compaction threshold.
	if !f.compacting && f.CompactThreshold != 0 && f.index.InMemCount() >= uint64(f.CompactThreshold) {
		f.compacting = true
		logger := f.Logger.With(zap.String("path", f.path))
		logger.Info("beginning series file compaction")

		startTime := time.Now()
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()

			if err := NewSeriesFileCompactor().Compact(f); err != nil {
				logger.With(zap.Error(err)).Error("series file compaction failed")
			}

			logger.With(zap.Duration("elapsed", time.Since(startTime))).Info("completed series file compaction")

			// Clear compaction flag.
			f.mu.Lock()
			f.compacting = false
			f.mu.Unlock()
		}()
	}

	return ids, nil
}

// DeleteSeriesID flags a series as permanently deleted.
// If the series is reintroduced later then it must create a new id.
func (f *SeriesFile) DeleteSeriesID(id uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Already tombstoned, ignore.
	if f.index.IsDeleted(id) {
		return nil
	}

	// Write tombstone entry.
	_, err := f.writeLogEntry(AppendSeriesEntry(nil, SeriesEntryTombstoneFlag, id, nil))
	if err != nil {
		return err
	}

	// Mark tombstone in memory.
	f.index.Delete(id)

	return nil
}

// IsDeleted returns true if the ID has been deleted before.
func (f *SeriesFile) IsDeleted(id uint64) bool {
	f.mu.RLock()
	v := f.index.IsDeleted(id)
	f.mu.RUnlock()
	return v
}

// SeriesKey returns the series key for a given id.
func (f *SeriesFile) SeriesKey(id uint64) []byte {
	if id == 0 {
		return nil
	}
	f.mu.RLock()
	key := f.seriesKeyByOffset(f.index.FindOffsetByID(id))
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
	id := f.index.FindIDBySeriesKey(f.segments, AppendSeriesKey(buf[:0], name, tags))
	f.mu.RUnlock()
	return id
}

// HasSeries return true if the series exists.
func (f *SeriesFile) HasSeries(name []byte, tags models.Tags, buf []byte) bool {
	return f.SeriesID(name, tags, buf) > 0
}

// SeriesCount returns the number of series.
func (f *SeriesFile) SeriesCount() uint64 {
	f.mu.RLock()
	n := f.index.Count()
	f.mu.RUnlock()
	return n
}

// SeriesIterator returns an iterator over all the series.
func (f *SeriesFile) SeriesIDIterator() SeriesIDIterator {
	var ids []uint64
	for _, segment := range f.segments {
		ids = segment.AppendSeriesIDs(ids)
	}
	return NewSeriesIDSliceIterator(ids)
}

// activeSegment returns the last segment.
func (f *SeriesFile) activeSegment() *SeriesSegment {
	if len(f.segments) == 0 {
		return nil
	}
	return f.segments[len(f.segments)-1]
}

func (f *SeriesFile) insert(key []byte) (id uint64, offset int64, err error) {
	id = f.seq + 1

	offset, err = f.writeLogEntry(AppendSeriesEntry(nil, SeriesEntryInsertFlag, id, key))
	if err != nil {
		return 0, 0, err
	}

	f.seq++
	return id, offset, nil
}

// writeLogEntry appends an entry to the end of the active segment.
// If there is no more room in the segment then a new segment is added.
func (f *SeriesFile) writeLogEntry(data []byte) (offset int64, err error) {
	segment := f.activeSegment()
	if segment == nil || !segment.CanWrite(data) {
		if segment, err = f.createSegment(); err != nil {
			return 0, err
		}
	}
	return segment.WriteLogEntry(data)
}

// createSegment appends a new segment
func (f *SeriesFile) createSegment() (*SeriesSegment, error) {
	// Close writer for active segment, if one exists.
	if segment := f.activeSegment(); segment != nil {
		if err := segment.CloseForWrite(); err != nil {
			return nil, err
		}
	}

	// Generate a new sequential segment identifier.
	var id uint16
	if len(f.segments) > 0 {
		id = f.segments[len(f.segments)-1].ID() + 1
	}
	filename := fmt.Sprintf("%04x", id)

	// Generate new empty segment.
	segment, err := CreateSeriesSegment(id, filepath.Join(f.path, filename))
	if err != nil {
		return nil, err
	}
	f.segments = append(f.segments, segment)

	// Allow segment to write.
	if err := segment.InitForWrite(); err != nil {
		return nil, err
	}

	return segment, nil
}

func (f *SeriesFile) seriesKeyByOffset(offset int64) []byte {
	if offset == 0 {
		return nil
	}

	segmentID, pos := SplitSeriesOffset(offset)
	for _, segment := range f.segments {
		if segment.ID() != segmentID {
			continue
		}

		key, _ := ReadSeriesKey(segment.Slice(pos + SeriesEntryHeaderSize))
		return key
	}

	return nil
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

// SeriesFileCompactor represents an object reindexes a series file and optionally compacts segments.
type SeriesFileCompactor struct{}

// NewSeriesFileCompactor returns a new instance of SeriesFileCompactor.
func NewSeriesFileCompactor() *SeriesFileCompactor {
	return &SeriesFileCompactor{}
}

// Compact rebuilds the series file index.
func (c *SeriesFileCompactor) Compact(f *SeriesFile) error {
	// Snapshot the partitions and index so we can check tombstones and replay at the end under lock.
	f.mu.RLock()
	segments := CloneSeriesSegments(f.segments)
	index := f.index.Clone()
	seriesN := f.index.Count()
	f.mu.RUnlock()

	// Compact index to a temporary location.
	indexPath := index.path + ".compacting"
	if err := c.compactIndexTo(index, seriesN, segments, indexPath); err != nil {
		return err
	}

	// Swap compacted index under lock & replay since compaction.
	if err := func() error {
		f.mu.Lock()
		defer f.mu.Unlock()

		// Reopen index with new file.
		if err := f.index.Close(); err != nil {
			return err
		} else if err := os.Rename(indexPath, index.path); err != nil {
			return err
		} else if err := f.index.Open(); err != nil {
			return err
		}

		// Replay new entries.
		if err := f.index.Recover(f.segments); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	return nil
}

func (c *SeriesFileCompactor) compactIndexTo(index *SeriesIndex, seriesN uint64, segments []*SeriesSegment, path string) error {
	hdr := NewSeriesIndexHeader()
	hdr.Count = seriesN
	hdr.Capacity = pow2((int64(hdr.Count) * 100) / SeriesIndexLoadFactor)

	// Allocate space for maps.
	keyIDMap := make([]byte, (hdr.Capacity * SeriesIndexElemSize))
	idOffsetMap := make([]byte, (hdr.Capacity * SeriesIndexElemSize))

	// Reindex all partitions.
	for _, segment := range segments {
		if err := segment.ForEachEntry(func(flag uint8, id uint64, offset int64, key []byte) error {
			// Only process insert entries.
			switch flag {
			case SeriesEntryInsertFlag: // fallthrough
			case SeriesEntryTombstoneFlag:
				return nil
			default:
				return fmt.Errorf("unexpected series file log entry flag: %d", flag)
			}

			// Ignore entry if tombstoned.
			if index.IsDeleted(id) {
				return nil
			}

			// Save highest id/offset to header.
			hdr.MaxSeriesID, hdr.MaxOffset = id, offset

			// Insert into maps.
			c.insertIDOffsetMap(idOffsetMap, hdr.Capacity, id, offset)
			return c.insertKeyIDMap(keyIDMap, hdr.Capacity, segments, key, offset, id)
		}); err != nil {
			return err
		}
	}

	// Open file handler.
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Calculate map positions.
	hdr.KeyIDMap.Offset, hdr.KeyIDMap.Size = SeriesIndexHeaderSize, int64(len(keyIDMap))
	hdr.IDOffsetMap.Offset, hdr.IDOffsetMap.Size = hdr.KeyIDMap.Offset+hdr.KeyIDMap.Size, int64(len(idOffsetMap))

	// Write header.
	if _, err := hdr.WriteTo(f); err != nil {
		return err
	}

	// Write maps.
	if _, err := f.Write(keyIDMap); err != nil {
		return err
	} else if _, err := f.Write(idOffsetMap); err != nil {
		return err
	}

	// Sync & close.
	if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	return nil
}

func (c *SeriesFileCompactor) insertKeyIDMap(dst []byte, capacity int64, segments []*SeriesSegment, key []byte, offset int64, id uint64) error {
	mask := capacity - 1
	hash := rhh.HashKey(key)

	// Continue searching until we find an empty slot or lower probe distance.
	for dist, pos := int64(0), hash&mask; ; dist, pos = dist+1, (pos+1)&mask {
		elem := dst[(pos * SeriesIndexElemSize):]

		// If empty slot found or matching offset, insert and exit.
		elemOffset := int64(binary.BigEndian.Uint64(elem[:8]))
		elemID := binary.BigEndian.Uint64(elem[8:])
		if elemOffset == 0 || elemOffset == offset {
			binary.BigEndian.PutUint64(elem[:8], uint64(offset))
			binary.BigEndian.PutUint64(elem[8:], id)
			return nil
		}

		// Read key at position & hash.
		elemKey := ReadSeriesKeyFromSegments(segments, elemOffset)
		elemHash := rhh.HashKey(elemKey)

		// If the existing elem has probed less than us, then swap places with
		// existing elem, and keep going to find another slot for that elem.
		if d := rhh.Dist(elemHash, pos, capacity); d < dist {

			// Insert current values.
			binary.BigEndian.PutUint64(elem[:8], uint64(offset))
			binary.BigEndian.PutUint64(elem[8:], id)

			// Swap with values in that position.
			hash, key, offset, id = elemHash, elemKey, elemOffset, elemID

			// Update current distance.
			dist = d
		}
	}
}

func (c *SeriesFileCompactor) insertIDOffsetMap(dst []byte, capacity int64, id uint64, offset int64) {
	mask := capacity - 1
	hash := rhh.HashUint64(id)

	// Continue searching until we find an empty slot or lower probe distance.
	for i, dist, pos := int64(0), int64(0), hash&mask; ; i, dist, pos = i+1, dist+1, (pos+1)&mask {
		elem := dst[(pos * SeriesIndexElemSize):]

		// If empty slot found or matching id, insert and exit.
		elemID := binary.BigEndian.Uint64(elem[:8])
		elemOffset := int64(binary.BigEndian.Uint64(elem[8:]))
		if elemOffset == 0 || elemOffset == offset {
			binary.BigEndian.PutUint64(elem[:8], id)
			binary.BigEndian.PutUint64(elem[8:], uint64(offset))
			return
		}

		// Hash key.
		elemHash := rhh.HashUint64(elemID)

		// If the existing elem has probed less than us, then swap places with
		// existing elem, and keep going to find another slot for that elem.
		if d := rhh.Dist(elemHash, pos, capacity); d < dist {
			// Insert current values.
			binary.BigEndian.PutUint64(elem[:8], id)
			binary.BigEndian.PutUint64(elem[8:], uint64(offset))

			// Swap with values in that position.
			hash, id, offset = elemHash, elemID, elemOffset

			// Update current distance.
			dist = d
		}

		if i > capacity {
			panic("rhh map full")
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
