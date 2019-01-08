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
	"github.com/prometheus/client_golang/prometheus"
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

	maxSeriesID SeriesID
	maxOffset   int64

	// metrics stores a shard instance of some Prometheus metrics. metrics
	// must be set before Open is called.
	rhhMetrics        *rhh.Metrics
	rhhLabels         prometheus.Labels
	rhhMetricsEnabled bool

	data         []byte // mmap data
	keyIDData    []byte // key/id mmap data
	idOffsetData []byte // id/offset mmap data

	// In-memory data since rebuild.
	keyIDMap    *rhh.HashMap
	idOffsetMap map[SeriesID]int64
	tombstones  map[SeriesID]struct{}
}

func NewSeriesIndex(path string) *SeriesIndex {
	return &SeriesIndex{
		path:              path,
		rhhMetricsEnabled: true,
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

	options := rhh.DefaultOptions
	options.Metrics = idx.rhhMetrics
	options.Labels = idx.rhhLabels
	options.MetricsEnabled = idx.rhhMetricsEnabled

	idx.keyIDMap = rhh.NewHashMap(options)
	idx.idOffsetMap = make(map[SeriesID]int64)
	idx.tombstones = make(map[SeriesID]struct{})
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
	options := rhh.DefaultOptions
	options.Metrics = idx.rhhMetrics
	options.Labels = idx.rhhLabels

	idx.keyIDMap = rhh.NewHashMap(options)
	idx.idOffsetMap = make(map[SeriesID]int64)
	idx.tombstones = make(map[SeriesID]struct{})

	// Process all entries since the maximum offset in the on-disk index.
	minSegmentID, _ := SplitSeriesOffset(idx.maxOffset)
	for _, segment := range segments {
		if segment.ID() < minSegmentID {
			continue
		}

		if err := segment.ForEachEntry(func(flag uint8, id SeriesIDTyped, offset int64, key []byte) error {
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

// OnDiskSize returns the on-disk size of the index in bytes.
func (idx *SeriesIndex) OnDiskSize() uint64 { return uint64(len(idx.data)) }

// InMemSize returns the heap size of the index in bytes. The returned value is
// an estimation and does not include include all allocated memory.
func (idx *SeriesIndex) InMemSize() uint64 {
	n := len(idx.idOffsetMap)
	return uint64(2*8*n) + uint64(len(idx.tombstones)*8)
}

func (idx *SeriesIndex) Insert(key []byte, id SeriesIDTyped, offset int64) {
	idx.execEntry(SeriesEntryInsertFlag, id, offset, key)
}

// Delete marks the series id as deleted.
func (idx *SeriesIndex) Delete(id SeriesID) {
	// NOTE: WithType(0) kinda sucks here, but we know it will be masked off.
	idx.execEntry(SeriesEntryTombstoneFlag, id.WithType(0), 0, nil)
}

// IsDeleted returns true if series id has been deleted.
func (idx *SeriesIndex) IsDeleted(id SeriesID) bool {
	if _, ok := idx.tombstones[id]; ok {
		return true
	}
	return idx.FindOffsetByID(id) == 0
}

func (idx *SeriesIndex) execEntry(flag uint8, id SeriesIDTyped, offset int64, key []byte) {
	untypedID := id.SeriesID()
	switch flag {
	case SeriesEntryInsertFlag:
		idx.keyIDMap.PutQuiet(key, id)
		idx.idOffsetMap[untypedID] = offset

		if untypedID.Greater(idx.maxSeriesID) {
			idx.maxSeriesID = untypedID
		}
		if offset > idx.maxOffset {
			idx.maxOffset = offset
		}

	case SeriesEntryTombstoneFlag:
		idx.tombstones[untypedID] = struct{}{}

	default:
		panic("unreachable")
	}
}

func (idx *SeriesIndex) FindIDBySeriesKey(segments []*SeriesSegment, key []byte) SeriesIDTyped {
	if v := idx.keyIDMap.Get(key); v != nil {
		if id, _ := v.(SeriesIDTyped); !id.IsZero() && !idx.IsDeleted(id.SeriesID()) {
			return id
		}
	}
	if len(idx.data) == 0 {
		return SeriesIDTyped{}
	}

	hash := rhh.HashKey(key)
	for d, pos := int64(0), hash&idx.mask; ; d, pos = d+1, (pos+1)&idx.mask {
		elem := idx.keyIDData[(pos * SeriesIndexElemSize):]
		elemOffset := int64(binary.BigEndian.Uint64(elem[:8]))

		if elemOffset == 0 {
			return SeriesIDTyped{}
		}

		elemKey := ReadSeriesKeyFromSegments(segments, elemOffset+SeriesEntryHeaderSize)
		elemHash := rhh.HashKey(elemKey)
		if d > rhh.Dist(elemHash, pos, idx.capacity) {
			return SeriesIDTyped{}
		} else if elemHash == hash && bytes.Equal(elemKey, key) {
			id := NewSeriesIDTyped(binary.BigEndian.Uint64(elem[8:]))
			if idx.IsDeleted(id.SeriesID()) {
				return SeriesIDTyped{}
			}
			return id
		}
	}
}

func (idx *SeriesIndex) FindIDByNameTags(segments []*SeriesSegment, name []byte, tags models.Tags, buf []byte) SeriesIDTyped {
	id := idx.FindIDBySeriesKey(segments, AppendSeriesKey(buf[:0], name, tags))
	if _, ok := idx.tombstones[id.SeriesID()]; ok {
		return SeriesIDTyped{}
	}
	return id
}

func (idx *SeriesIndex) FindIDListByNameTags(segments []*SeriesSegment, names [][]byte, tagsSlice []models.Tags, buf []byte) (ids []SeriesIDTyped, ok bool) {
	ids, ok = make([]SeriesIDTyped, len(names)), true
	for i := range names {
		id := idx.FindIDByNameTags(segments, names[i], tagsSlice[i], buf)
		if id.IsZero() {
			ok = false
			continue
		}
		ids[i] = id
	}
	return ids, ok
}

func (idx *SeriesIndex) FindOffsetByID(id SeriesID) int64 {
	if offset := idx.idOffsetMap[id]; offset != 0 {
		return offset
	} else if len(idx.data) == 0 {
		return 0
	}

	hash := rhh.HashUint64(id.RawID())
	for d, pos := int64(0), hash&idx.mask; ; d, pos = d+1, (pos+1)&idx.mask {
		elem := idx.idOffsetData[(pos * SeriesIndexElemSize):]
		elemID := NewSeriesID(binary.BigEndian.Uint64(elem[:8]))

		if elemID == id {
			return int64(binary.BigEndian.Uint64(elem[8:]))
		} else if elemID.IsZero() || d > rhh.Dist(rhh.HashUint64(elemID.RawID()), pos, idx.capacity) {
			return 0
		}
	}
}

// Clone returns a copy of idx for use during compaction. In-memory maps are not cloned.
func (idx *SeriesIndex) Clone() *SeriesIndex {
	tombstones := make(map[SeriesID]struct{}, len(idx.tombstones))
	for id := range idx.tombstones {
		tombstones[id] = struct{}{}
	}

	idOffsetMap := make(map[SeriesID]int64)
	for k, v := range idx.idOffsetMap {
		idOffsetMap[k] = v
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
		idOffsetMap:  idOffsetMap,
	}
}

// SeriesIndexHeader represents the header of a series index.
type SeriesIndexHeader struct {
	Version uint8

	MaxSeriesID SeriesID
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
	if err := binary.Read(r, binary.BigEndian, &hdr.MaxSeriesID.ID); err != nil {
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
