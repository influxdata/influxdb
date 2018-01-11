package tsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/mmap"
	"github.com/influxdata/influxdb/pkg/rhh"
)

const (
	SeriesIndexVersion = 1
	SeriesIndexMagic   = "SIDX"
)

const (
	SeriesIndexElemSize   = 16 // offset + id
	SeriesIndexLoadFactor = 90 // rhh load factor

	SeriesIndexHeaderSize = 0 +
		4 + 1 + // magic + version
		8 + 8 + // max series + max offset
		8 + 8 + // count + capacity
		8 + 8 + // key/id map offset & size
		8 + 8 + // id/offset map offset & size
		0
)

var ErrInvalidSeriesIndex = errors.New("invalid series index")

// SeriesIndex represents an index of key-to-id & id-to-offset mappings.
type SeriesIndex struct {
	path string

	count    uint64
	capacity int64
	mask     int64

	maxSeriesID uint64
	maxOffset   int64

	data         []byte // mmap data
	keyIDData    []byte // key/id mmap data
	idOffsetData []byte // id/offset mmap data

	// In-memory data since rebuild.
	keyIDMap    *rhh.HashMap
	idOffsetMap map[uint64]int64
	tombstones  map[uint64]struct{}
}

func NewSeriesIndex(path string) *SeriesIndex {
	return &SeriesIndex{
		path: path,
	}
}

// Open memory-maps the index file.
func (idx *SeriesIndex) Open() (err error) {
	// Map data file, if it exists.
	if err := func() error {
		if _, err := os.Stat(idx.path); err != nil && !os.IsNotExist(err) {
			return err
		} else if err == nil {
			if idx.data, err = mmap.Map(idx.path, 0); err != nil {
				return err
			}

			hdr, err := ReadSeriesIndexHeader(idx.data)
			if err != nil {
				return err
			}
			idx.count, idx.capacity, idx.mask = hdr.Count, hdr.Capacity, hdr.Capacity-1
			idx.maxSeriesID, idx.maxOffset = hdr.MaxSeriesID, hdr.MaxOffset

			idx.keyIDData = idx.data[hdr.KeyIDMap.Offset : hdr.KeyIDMap.Offset+hdr.KeyIDMap.Size]
			idx.idOffsetData = idx.data[hdr.IDOffsetMap.Offset : hdr.IDOffsetMap.Offset+hdr.IDOffsetMap.Size]
		}
		return nil
	}(); err != nil {
		idx.Close()
		return err
	}

	idx.keyIDMap = rhh.NewHashMap(rhh.DefaultOptions)
	idx.idOffsetMap = make(map[uint64]int64)
	idx.tombstones = make(map[uint64]struct{})
	return nil
}

// Close unmaps the index file.
func (idx *SeriesIndex) Close() (err error) {
	if idx.data != nil {
		err = mmap.Unmap(idx.data)
	}
	idx.keyIDData = nil
	idx.idOffsetData = nil

	idx.keyIDMap = nil
	idx.idOffsetMap = nil
	idx.tombstones = nil
	return err
}

// Recover rebuilds the in-memory index for all new entries.
func (idx *SeriesIndex) Recover(segments []*SeriesSegment) error {
	// Allocate new in-memory maps.
	idx.keyIDMap = rhh.NewHashMap(rhh.DefaultOptions)
	idx.idOffsetMap = make(map[uint64]int64)
	idx.tombstones = make(map[uint64]struct{})

	// Process all entries since the maximum offset in the on-disk index.
	minSegmentID, _ := SplitSeriesOffset(idx.maxOffset)
	for _, segment := range segments {
		if segment.ID() < minSegmentID {
			continue
		}

		if err := segment.ForEachEntry(func(flag uint8, id uint64, offset int64, key []byte) error {
			if offset <= idx.maxOffset {
				return nil
			}
			idx.execEntry(flag, id, offset, key)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// Count returns the number of series in the index.
func (idx *SeriesIndex) Count() uint64 {
	return idx.OnDiskCount() + idx.InMemCount()
}

// OnDiskCount returns the number of series in the on-disk index.
func (idx *SeriesIndex) OnDiskCount() uint64 { return idx.count }

// InMemCount returns the number of series in the in-memory index.
func (idx *SeriesIndex) InMemCount() uint64 { return uint64(len(idx.idOffsetMap)) }

func (idx *SeriesIndex) Insert(key []byte, id uint64, offset int64) {
	idx.execEntry(SeriesEntryInsertFlag, id, offset, key)
}

// Delete marks the series id as deleted.
func (idx *SeriesIndex) Delete(id uint64) {
	idx.execEntry(SeriesEntryTombstoneFlag, id, 0, nil)
}

// IsDeleted returns true if series id has been deleted.
func (idx *SeriesIndex) IsDeleted(id uint64) bool {
	_, ok := idx.tombstones[id]
	return ok
}

func (idx *SeriesIndex) execEntry(flag uint8, id uint64, offset int64, key []byte) {
	switch flag {
	case SeriesEntryInsertFlag:
		idx.keyIDMap.Put(key, id)
		idx.idOffsetMap[id] = offset

		if id > idx.maxSeriesID {
			idx.maxSeriesID = id
		}
		if offset > idx.maxOffset {
			idx.maxOffset = offset
		}

	case SeriesEntryTombstoneFlag:
		idx.tombstones[id] = struct{}{}

	default:
		panic("unreachable")
	}
}

func (idx *SeriesIndex) FindIDBySeriesKey(segments []*SeriesSegment, key []byte) uint64 {
	if v := idx.keyIDMap.Get(key); v != nil {
		if id, _ := v.(uint64); id != 0 && !idx.IsDeleted(id) {
			return id
		}
	}
	if len(idx.data) == 0 {
		return 0
	}

	hash := rhh.HashKey(key)
	for d, pos := int64(0), hash&idx.mask; ; d, pos = d+1, (pos+1)&idx.mask {
		elem := idx.keyIDData[(pos * SeriesIndexElemSize):]
		elemOffset := int64(binary.BigEndian.Uint64(elem[:8]))

		if elemOffset == 0 {
			return 0
		}

		elemKey := ReadSeriesKeyFromSegments(segments, elemOffset+SeriesEntryHeaderSize)
		elemHash := rhh.HashKey(elemKey)
		if d > rhh.Dist(elemHash, pos, idx.capacity) {
			return 0
		} else if elemHash == hash && bytes.Equal(elemKey, key) {
			id := binary.BigEndian.Uint64(elem[8:])
			if idx.IsDeleted(id) {
				return 0
			}
			return id
		}
	}
}

func (idx *SeriesIndex) FindIDByNameTags(segments []*SeriesSegment, name []byte, tags models.Tags, buf []byte) uint64 {
	id := idx.FindIDBySeriesKey(segments, AppendSeriesKey(buf[:0], name, tags))
	if _, ok := idx.tombstones[id]; ok {
		return 0
	}
	return id
}

func (idx *SeriesIndex) FindIDListByNameTags(segments []*SeriesSegment, names [][]byte, tagsSlice []models.Tags, buf []byte) (ids []uint64, ok bool) {
	ids, ok = make([]uint64, len(names)), true
	for i := range names {
		id := idx.FindIDByNameTags(segments, names[i], tagsSlice[i], buf)
		if id == 0 {
			ok = false
			continue
		}
		ids[i] = id
	}
	return ids, ok
}

func (idx *SeriesIndex) FindOffsetByID(id uint64) int64 {
	if offset := idx.idOffsetMap[id]; offset != 0 {
		return offset
	} else if len(idx.data) == 0 {
		return 0
	}

	hash := rhh.HashUint64(id)
	for d, pos := int64(0), hash&idx.mask; ; d, pos = d+1, (pos+1)&idx.mask {
		elem := idx.idOffsetData[(pos * SeriesIndexElemSize):]
		elemID := binary.BigEndian.Uint64(elem[:8])

		if elemID == id {
			return int64(binary.BigEndian.Uint64(elem[8:]))
		} else if elemID == 0 || d > rhh.Dist(rhh.HashUint64(elemID), pos, idx.capacity) {
			return 0
		}
	}
}

// Clone returns a copy of idx for use during compaction. In-memory maps are not cloned.
func (idx *SeriesIndex) Clone() *SeriesIndex {
	tombstones := make(map[uint64]struct{}, len(idx.tombstones))
	for id := range idx.tombstones {
		tombstones[id] = struct{}{}
	}

	return &SeriesIndex{
		path:         idx.path,
		count:        idx.count,
		capacity:     idx.capacity,
		mask:         idx.mask,
		maxSeriesID:  idx.maxSeriesID,
		maxOffset:    idx.maxOffset,
		data:         idx.data,
		keyIDData:    idx.keyIDData,
		idOffsetData: idx.idOffsetData,
		tombstones:   tombstones,
	}
}

// SeriesIndexHeader represents the header of a series index.
type SeriesIndexHeader struct {
	Version uint8

	MaxSeriesID uint64
	MaxOffset   int64

	Count    uint64
	Capacity int64

	KeyIDMap struct {
		Offset int64
		Size   int64
	}

	IDOffsetMap struct {
		Offset int64
		Size   int64
	}
}

// NewSeriesIndexHeader returns a new instance of SeriesIndexHeader.
func NewSeriesIndexHeader() SeriesIndexHeader {
	return SeriesIndexHeader{Version: SeriesIndexVersion}
}

// ReadSeriesIndexHeader returns the header from data.
func ReadSeriesIndexHeader(data []byte) (hdr SeriesIndexHeader, err error) {
	r := bytes.NewReader(data)

	// Read magic number.
	magic := make([]byte, len(SeriesIndexMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return hdr, err
	} else if !bytes.Equal([]byte(SeriesIndexMagic), magic) {
		return hdr, ErrInvalidSeriesIndex
	}

	// Read version.
	if err := binary.Read(r, binary.BigEndian, &hdr.Version); err != nil {
		return hdr, err
	}

	// Read max offset.
	if err := binary.Read(r, binary.BigEndian, &hdr.MaxSeriesID); err != nil {
		return hdr, err
	} else if err := binary.Read(r, binary.BigEndian, &hdr.MaxOffset); err != nil {
		return hdr, err
	}

	// Read count & capacity.
	if err := binary.Read(r, binary.BigEndian, &hdr.Count); err != nil {
		return hdr, err
	} else if err := binary.Read(r, binary.BigEndian, &hdr.Capacity); err != nil {
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
	return hdr, nil
}

// WriteTo writes the header to w.
func (hdr *SeriesIndexHeader) WriteTo(w io.Writer) (n int64, err error) {
	var buf bytes.Buffer
	buf.WriteString(SeriesIndexMagic)
	binary.Write(&buf, binary.BigEndian, hdr.Version)
	binary.Write(&buf, binary.BigEndian, hdr.MaxSeriesID)
	binary.Write(&buf, binary.BigEndian, hdr.MaxOffset)
	binary.Write(&buf, binary.BigEndian, hdr.Count)
	binary.Write(&buf, binary.BigEndian, hdr.Capacity)
	binary.Write(&buf, binary.BigEndian, hdr.KeyIDMap.Offset)
	binary.Write(&buf, binary.BigEndian, hdr.KeyIDMap.Size)
	binary.Write(&buf, binary.BigEndian, hdr.IDOffsetMap.Offset)
	binary.Write(&buf, binary.BigEndian, hdr.IDOffsetMap.Size)
	return buf.WriteTo(w)
}
