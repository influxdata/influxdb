package influxdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/messaging"
)

// ShardGroup represents a group of shards created for a single time range.
type ShardGroup struct {
	ID        uint64    `json:"id,omitempty"`
	StartTime time.Time `json:"startTime,omitempty"`
	EndTime   time.Time `json:"endTime,omitempty"`
	Shards    []*Shard  `json:"shards,omitempty"`
}

// newShardGroup returns a new initialized ShardGroup instance.
func newShardGroup(t time.Time, d time.Duration) *ShardGroup {
	sg := ShardGroup{}
	sg.StartTime = t.Truncate(d).UTC()
	sg.EndTime = sg.StartTime.Add(d).UTC()

	return &sg
}

func (sg *ShardGroup) initialize(index uint64, shardN, replicaN int, db *database, rp *RetentionPolicy, nodes []*DataNode, meta *metastore) error {
	sg.Shards = make([]*Shard, shardN)

	// Persist to metastore if a shard was created.
	return meta.mustUpdate(index, func(tx *metatx) error {
		// Generate an ID for the group.
		sg.ID = tx.nextShardGroupID()

		// Generate an ID for each shard.
		for i := range sg.Shards {
			sg.Shards[i] = newShard(tx.nextShardID())
		}

		// Assign data nodes to shards via round robin.
		// Start from a repeatably "random" place in the node list.
		nodeIndex := int(index % uint64(len(nodes)))
		for _, sh := range sg.Shards {
			for i := 0; i < replicaN; i++ {
				node := nodes[nodeIndex%len(nodes)]
				sh.DataNodeIDs = append(sh.DataNodeIDs, node.ID)
				nodeIndex++
			}
		}

		// Retention policy has a new shard group, so update the policy.
		rp.shardGroups = append(rp.shardGroups, sg)

		return tx.saveDatabase(db)
	})
}

func (sg *ShardGroup) close(id uint64) error {
	for _, shard := range sg.Shards {
		// Ignore shards not on this server.
		if !shard.HasDataNodeID(id) {
			continue
		}

		path := shard.store.Path()
		shard.close()
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("shard id %d, path %s : %s", shard.ID, path, err)
		}
	}
	return nil
}

// ShardBySeriesID returns the shard that a series is assigned to in the group.
func (sg *ShardGroup) ShardBySeriesID(seriesID uint64) *Shard {
	return sg.Shards[int(seriesID)%len(sg.Shards)]
}

// Duration returns the duration between the shard group's start and end time.
func (sg *ShardGroup) Duration() time.Duration { return sg.EndTime.Sub(sg.StartTime) }

// Contains return whether the shard group contains data for the time between min and max
func (sg *ShardGroup) Contains(min, max time.Time) bool {
	return timeBetweenInclusive(sg.StartTime, min, max) ||
		timeBetweenInclusive(sg.EndTime, min, max) ||
		(sg.StartTime.Before(min) && sg.EndTime.After(max))
}

// dropSeries will delete all data with the seriesID
func (sg *ShardGroup) dropSeries(seriesIDs ...uint64) error {
	for _, s := range sg.Shards {
		err := s.dropSeries(seriesIDs...)
		if err != nil {
			return err
		}
	}
	return nil
}

// shardtx represents a shard transaction.
type shardtx struct {
	*bolt.Tx
}

// Shard represents the logical storage for a given time range.
// The instance on a local server may contain the raw data in "store" if the
// shard is assigned to the server's data node id.
type Shard struct {
	ID          uint64   `json:"id,omitempty"`
	DataNodeIDs []uint64 `json:"nodeIDs,omitempty"` // owners

	mu    sync.RWMutex
	index uint64        // highest replicated index
	store *bolt.DB      // underlying data store
	conn  MessagingConn // streaming connection to broker

	stats *Stats // In-memory stats

	wg      sync.WaitGroup // pending goroutines
	closing chan struct{}  // close notification
}

// newShard returns a new initialized Shard instance.
func newShard(id uint64) *Shard {
	s := &Shard{}
	s.ID = id
	s.stats = NewStats(fmt.Sprintf("shard %d", id))
	s.stats.Inc("initialize")

	return s
}

// open initializes and opens the shard's store.
func (s *Shard) open(path string, conn MessagingConn) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stats == nil {
		s.stats = NewStats(fmt.Sprintf("shard %d", s.ID))
	}
	s.stats.Inc("open")

	// Return an error if the shard is already open.
	if s.store != nil {
		s.stats.Inc("errAlreadyOpen")
		return errors.New("shard already open")
	}

	// Open store on shard.
	store, err := bolt.Open(path, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		s.stats.Inc("errBoltOpenFailure")
		return err
	}
	s.store = store

	// Initialize store.
	if err := s.store.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte("meta"))
		_, _ = tx.CreateBucketIfNotExists([]byte("values"))

		// Find highest replicated index.
		s.index = shardMetaIndex(tx)

		// Open connection.
		if err := conn.Open(s.index, true); err != nil {
			return fmt.Errorf("open shard conn: id=%d, idx=%d, err=%s", s.ID, s.index, err)
		}

		return nil
	}); err != nil {
		s.stats.Inc("errBoltStoreUpdateFailure")
		_ = s.close()
		return fmt.Errorf("init: %s", err)
	}

	// Start importing from connection.
	s.closing = make(chan struct{})
	s.wg.Add(1)
	go s.processor(conn, s.closing)

	return nil
}

// shardMetaIndex returns the index from the "meta" bucket on a transaction.
func shardMetaIndex(tx *bolt.Tx) uint64 {
	var index uint64
	if buf := tx.Bucket([]byte("meta")).Get([]byte("index")); len(buf) > 0 {
		index = btou64(buf)
	}
	return index
}

// view executes a function in the context of a read-only transaction.
func (s *Shard) view(fn func(*shardtx) error) error {
	return s.store.View(func(tx *bolt.Tx) error { return fn(&shardtx{tx}) })
}

// mustView executes a function in the context of a read-only transaction.
// Panics if system error occurs. Return error from the fn for validation errors.
func (s *Shard) mustView(fn func(*shardtx) error) (err error) {
	if e := s.view(func(tx *shardtx) error {
		err = fn(tx)
		return nil
	}); e != nil {
		panic("shard view: " + e.Error())
	}
	return
}

// Index returns the highest Raft index processed by this shard. Shard RLock
// held during execution.
func (s *Shard) Index() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index
}

// close shuts down the shard's store.
func (s *Shard) close() error {
	// Wait for goroutines to stop.
	if s.closing != nil {
		close(s.closing)
		s.closing = nil
	}

	s.wg.Wait()

	if s.store != nil {
		_ = s.store.Close()
	}
	if s.stats != nil {
		s.stats.Inc("close")
	}
	return nil
}

// sync returns after a given index has been reached.
func (s *Shard) sync(index uint64) error {
	for {
		// Check if index has occurred.
		s.mu.RLock()
		i := s.index
		s.mu.RUnlock()
		if i >= index {
			return nil
		}

		// Otherwise wait momentarily and check again.
		time.Sleep(1 * time.Millisecond)
	}
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
func (s *Shard) readSeries(seriesID uint64, timestamp int64) (values []byte, err error) {
	err = s.store.View(func(tx *bolt.Tx) error {
		// Find series bucket.
		b := tx.Bucket(u64tob(seriesID))
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
func (s *Shard) writeSeries(index uint64, batch []byte) error {
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
			b, err := tx.CreateBucketIfNotExists(u64tob(seriesID))
			if err != nil {
				return err
			}

			// Insert the values by timestamp.
			if err := b.Put(u64tob(uint64(timestamp)), data); err != nil {
				return err
			}
			s.stats.Add("shardBytes", int64(len(data))+8) // Payload plus timestamp
			s.stats.Inc("shardWrite")

			// Push the buffer forward and check if we're done.
			batch = batch[payloadLength:]
			if len(batch) == 0 {
				break
			}
		}

		// Set index.
		if err := tx.Bucket([]byte("meta")).Put([]byte("index"), u64tob(index)); err != nil {
			return fmt.Errorf("write shard index: %s", err)
		}

		return nil
	})
}

func (s *Shard) dropSeries(seriesIDs ...uint64) error {
	if s.store == nil {
		return nil
	}
	return s.store.Update(func(tx *bolt.Tx) error {
		for _, seriesID := range seriesIDs {
			err := tx.DeleteBucket(u64tob(seriesID))
			if err != bolt.ErrBucketNotFound {
				return err
			}
		}
		return nil
	})
}

// processor runs in a separate goroutine and processes all incoming broker messages.
func (s *Shard) processor(conn MessagingConn, closing <-chan struct{}) {
	defer s.wg.Done()

	for {
		// Read incoming message.
		// Exit if the connection has been closed or if shard is closing.
		var ok bool
		var m *messaging.Message
		select {
		case m, ok = <-conn.C():
			if !ok {
				return
			}
		case <-closing:
			return
		}

		// Ignore any writes that are from an old index.
		s.mu.RLock()
		i := s.index
		s.mu.RUnlock()
		if m.Index < i {
			continue
		}

		// Handle write series separately so we don't lock server during shard writes.
		switch m.Type {
		case writeRawSeriesMessageType:
			s.stats.Inc("writeSeriesMessageRx")
			if err := s.writeSeries(m.Index, m.Data); err != nil {
				panic(fmt.Errorf("apply shard: id=%d, idx=%d, err=%s", s.ID, m.Index, err))
			}
		default:
			panic(fmt.Sprintf("invalid shard message type: %d", m.Type))
		}

		// Track last index.
		s.mu.Lock()
		s.index = m.Index
		s.mu.Unlock()

		// Update the connection with the high index.
		conn.SetIndex(m.Index)
	}
}

// Shards represents a list of shards.
type Shards []*Shard

// pointHeaderSize represents the size of a point header, in bytes.
const pointHeaderSize = 8 + 4 + 8 // seriesID + payload length + timestamp

// marshalPointHeader encodes a series id, payload length, timestamp, & flagset into a byte slice.
func marshalPointHeader(seriesID uint64, payloadLength uint32, timestamp int64) []byte {
	b := make([]byte, pointHeaderSize)
	binary.BigEndian.PutUint64(b[0:8], seriesID)
	binary.BigEndian.PutUint32(b[8:12], payloadLength)
	binary.BigEndian.PutUint64(b[12:20], uint64(timestamp))
	return b
}

// unmarshalPointHeader decodes a byte slice into a series id, timestamp & flagset.
func unmarshalPointHeader(b []byte) (seriesID uint64, payloadLength uint32, timestamp int64) {
	seriesID = binary.BigEndian.Uint64(b[0:8])
	payloadLength = binary.BigEndian.Uint32(b[8:12])
	timestamp = int64(binary.BigEndian.Uint64(b[12:20]))
	return
}

type uint8Slice []uint8

func (p uint8Slice) Len() int           { return len(p) }
func (p uint8Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint8Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
