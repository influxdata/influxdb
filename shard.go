package influxdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

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

// ShardBySeriesID returns the shard that a series is assigned to in the group.
func (g *ShardGroup) ShardBySeriesID(seriesID uint32) *Shard {
	return g.Shards[int(seriesID)%len(g.Shards)]
}

// Shard represents the logical storage for a given time range.
// The instance on a local server may contain the raw data in "store" if the
// shard is assigned to the server's data node id.
type Shard struct {
	ID          uint64   `json:"id,omitempty"`
	DataNodeIDs []uint64 `json:"nodeIDs,omitempty"` // owners

	store *bolt.DB
}

// newShardGroup returns a new initialized ShardGroup instance.
func newShardGroup() *ShardGroup { return &ShardGroup{} }

// Duration returns the duration between the shard group's start and end time.
func (g *ShardGroup) Duration() time.Duration { return g.EndTime.Sub(g.StartTime) }

// dropSeries will delete all data with the seriesID
func (g *ShardGroup) dropSeries(seriesID uint32) error {
	for _, s := range g.Shards {
		err := s.dropSeries(seriesID)
		if err != nil {
			return err
		}
	}
	return nil
}

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

// readSeries reads encoded series data from a shard.
func (s *Shard) readSeries(seriesID uint32, timestamp int64) (values []byte, err error) {
	err = s.store.View(func(tx *bolt.Tx) error {
		// Find series bucket.
		b := tx.Bucket(u32tob(seriesID))
		if b == nil {
			return nil
		}

		// Retrieve encoded series data.
		values = b.Get(u64tob(uint64(timestamp)))
		return nil
	})
	return
}

// writeSeries writes series batch to a shard.
func (s *Shard) writeSeries(batch []byte) error {
	return s.store.Update(func(tx *bolt.Tx) error {
		for {
			if pointHeaderSize > len(batch) {
				return ErrInvalidPointBuffer
			}
			seriesID, payloadLength, timestamp := unmarshalPointHeader(batch[:pointHeaderSize])
			batch = batch[pointHeaderSize:]

			if payloadLength > uint32(len(batch)) {
				return ErrInvalidPointBuffer
			}
			data := batch[:payloadLength]

			// Create a bucket for the series.
			b, err := tx.CreateBucketIfNotExists(u32tob(seriesID))
			if err != nil {
				return err
			}

			// Insert the values by timestamp.
			if err := b.Put(u64tob(uint64(timestamp)), data); err != nil {
				return err
			}

			// Push the buffer forward and check if we're done.
			batch = batch[payloadLength:]
			if len(batch) == 0 {
				break
			}
		}

		return nil
	})
}

func (s *Shard) dropSeries(seriesID uint32) error {
	if s.store == nil {
		return nil
	}
	return s.store.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket(u32tob(seriesID))
		if err != bolt.ErrBucketNotFound {
			return err
		}
		return nil
	})
}

// Shards represents a list of shards.
type Shards []*Shard

// pointHeaderSize represents the size of a point header, in bytes.
const pointHeaderSize = 4 + 4 + 8 // seriesID + payload length + timestamp

// marshalPointHeader encodes a series id, payload length, timestamp, & flagset into a byte slice.
func marshalPointHeader(seriesID uint32, payloadLength uint32, timestamp int64) []byte {
	b := make([]byte, pointHeaderSize)
	binary.BigEndian.PutUint32(b[0:4], seriesID)
	binary.BigEndian.PutUint32(b[4:8], payloadLength)
	binary.BigEndian.PutUint64(b[8:16], uint64(timestamp))
	return b
}

// unmarshalPointHeader decodes a byte slice into a series id, timestamp & flagset.
func unmarshalPointHeader(b []byte) (seriesID uint32, payloadLength uint32, timestamp int64) {
	seriesID = binary.BigEndian.Uint32(b[0:4])
	payloadLength = binary.BigEndian.Uint32(b[4:8])
	timestamp = int64(binary.BigEndian.Uint64(b[8:16]))
	return
}

type uint8Slice []uint8

func (p uint8Slice) Len() int           { return len(p) }
func (p uint8Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint8Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
