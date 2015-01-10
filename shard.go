package influxdb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/boltdb/bolt"
)

// ShardGroup represents a group of shards created for a single time range.
type ShardGroup struct {
	ID        uint64    `json:"id,omitempty"`
	StartTime time.Time `json:"startTime,omitempty"`
	EndTime   time.Time `json:"endTime,omitempty"`
	Shards    []*Shard  `json:"shards,omitempty"`
}

// close closes all shards.
func (g *ShardGroup) close() {
	for _, sh := range g.Shards {
		_ = sh.close()
	}
}

// Shard represents the physical storage for a given time range.
type Shard struct {
	ID          uint64   `json:"id,omitempty"`
	DataNodeIDs []uint64 `json:"nodeIDs,omitempty"` // owners

	store *bolt.DB
}

// newShardGroup returns a new initialized ShardGroup instance.
func newShardGroup() *ShardGroup { return &ShardGroup{} }

// Duration returns the duration between the shard group's start and end time.
func (g *ShardGroup) Duration() time.Duration { return g.EndTime.Sub(g.StartTime) }

// newShard returns a new initialized Shard instance.
func newShard() *Shard { return &Shard{} }

// open initializes and opens the shard's store.
func (s *Shard) open(path string) error {
	// Return an error if the shard is already open.
	if s.store != nil {
		return errors.New("shard already open")
	}

	// Open store on shard.
	store, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	s.store = store

	// Initialize store.
	if err := s.store.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte("values"))
		return nil
	}); err != nil {
		_ = s.close()
		return fmt.Errorf("init: %s", err)
	}

	return nil
}

// close shuts down the shard's store.
func (s *Shard) close() error {
	if s.store == nil {
		return nil
	}
	return s.store.Close()
}

// HasDataNodeID return true if the data node owns the shard.
func (s *Shard) HasDataNodeID(id uint64) bool {
	for _, dataNodeID := range s.DataNodeIDs {
		if dataNodeID == id {
			return true
		}
	}
	return false
}

// writeSeries writes series data to a shard.
func (s *Shard) writeSeries(overwrite bool, data []byte) error {
	// Extract the series id and timestamp from the header.
	// Everything after the header is the marshalled value.
	seriesID, timestamp := unmarshalPointHeader(data)
	values := data[pointHeaderSize:]

	// Write series to the shard store.
	return s.store.Update(func(tx *bolt.Tx) error {
		// Create a bucket for the series.
		b, err := tx.CreateBucketIfNotExists(u64tob(seriesID))
		if err != nil {
			return err
		}

		// Insert the values by timestamp.
		key := u64tob(timestamp.UnixNano())
		if err := b.Put(key, value); err != nil {
			return err
		}

		return nil
	})
}

func (s *Shard) deleteSeries(name string) error {
	panic("not yet implemented") // TODO
}

// Shards represents a list of shards.
type Shards []*Shard

// pointHeaderSize represents the size of a point header, in bytes.
const pointHeaderSize = 4 + 12 // seriesID + timestamp

// marshalPointHeader encodes a series id, timestamp, & flagset into a byte slice.
func marshalPointHeader(seriesID uint32, timestamp int64, flag pointHeaderFlag) []byte {
	b := make([]byte, 13)
	binary.BigEndian.PutUint32(b[0:4], seriesID)
	binary.BigEndian.PutUint64(b[4:12], uint64(timestamp))
	b[13] = byte(flag)
	return b
}

// unmarshalPointHeader decodes a byte slice into a series id, timestamp & flagset.
func unmarshalPointHeader(b []byte) (seriesID uint32, timestamp int64, flag pointHeaderFlag) {
	seriesID = binary.BigEndian.Uint32(b[0:4])
	timestamp = binary.BigEndian.Uint64(b[4:12])
	flag = pointHeaderFlag(b[13])
	return
}

type pointHeaderFlag uint8

const (
	pointHeaderRawFlag = writeSeriesFlag(0x01)
)

// marshalValues encodes a set of field ids and values to a byte slice.
func marshalValues(values map[uint8]interface{}) []byte {
	// Sort fields for consistency.
	fieldIDs := make([]uint8, 0, len(values))
	for fieldID := range values {
		fieldIDs = append(fieldIDs, fieldID)
	}
	sort.Sort(uint8Slice(fieldIDs))

	// Allocate byte slice and write field count.
	b := make([]byte, 1, 10)
	b[0] = byte(len(values))

	// Write out each field.
	for _, fieldID := range fieldIDs {
		// Create a temporary buffer for this field.
		buf := make([]byte, 9)
		buf[0] = fieldID

		// Encode value after field id.
		// TODO: Support non-float types.
		switch v := values[fieldID].(type) {
		case float64:
			binary.BigEndian.PutUint64(b[1:9], math.Float64bits(v))
		default:
			panic(fmt.Sprintf("unsupported value type: %T", v))
		}

		// Append temp buffer to the end.
		b = append(b, buf...)
	}

	return b
}

type uint8Slice []uint8

func (p uint8Slice) Len() int           { return len(p) }
func (p uint8Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint8Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
