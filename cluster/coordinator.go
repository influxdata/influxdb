package cluster

import (
	"errors"
	"os"
	"sync"
	"time"

	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

const DefaultWriteTimeout = 5 * time.Second

// ConsistencyLevel represent a required replication criteria before a write can
// be returned as successful
type ConsistencyLevel int

const (
	// ConsistencyLevelAny allows for hinted hand off, potentially no write happened yet
	ConsistencyLevelAny ConsistencyLevel = iota

	// ConsistencyLevelOne requires at least one data node acknowledged a write
	ConsistencyLevelOne

	// ConsistencyLevelOne requires a quorum of data nodes to acknowledge a write
	ConsistencyLevelQuorum

	// ConsistencyLevelAll requires all data nodes to acknowledge a write
	ConsistencyLevelAll
)

var (
	// ErrTimeout is returned when a write times out
	ErrTimeout = errors.New("timeout")

	// ErrPartialWrite is returned when a write partially succeeds but does
	// not meet the requested consistency level
	ErrPartialWrite = errors.New("partial write")

	// ErrWriteFailed is returned when no writes succeeded
	ErrWriteFailed = errors.New("write failed")
)

// Coordinator handle queries and writes across multiple local and remote
// data nodes.
type Coordinator struct {
	nodeID  uint64
	mu      sync.RWMutex
	closing chan struct{}

	MetaStore interface {
		RetentionPolicy(database, policy string) (*meta.RetentionPolicyInfo, error)
		CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	}

	Store interface {
		CreateShard(database, retentionPolicy string, shardID uint64) error
		WriteToShard(shardID uint64, points []tsdb.Point) error
	}

	ClusterWriter interface {
		Write(shardID, ownerID uint64, points []tsdb.Point) error
	}
}

func NewCoordinator(localID uint64) *Coordinator {
	return &Coordinator{
		nodeID:  localID,
		closing: make(chan struct{}),
	}
}

// ShardMapping contains a mapping of a shards to a points.
type ShardMapping struct {
	Points map[uint64][]tsdb.Point    // The points associated with a shard ID
	Shards map[uint64]*meta.ShardInfo // The shards that have been mapped, keyed by shard ID
}

// NewShardMapping creates an empty ShardMapping
func NewShardMapping() *ShardMapping {
	return &ShardMapping{
		Points: map[uint64][]tsdb.Point{},
		Shards: map[uint64]*meta.ShardInfo{},
	}
}

// MapPoint maps a point to shard
func (s *ShardMapping) MapPoint(shardInfo *meta.ShardInfo, p tsdb.Point) {
	points, ok := s.Points[shardInfo.ID]
	if !ok {
		s.Points[shardInfo.ID] = []tsdb.Point{p}
	} else {
		s.Points[shardInfo.ID] = append(points, p)
	}
	s.Shards[shardInfo.ID] = shardInfo
}

func (c *Coordinator) Open() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing == nil {
		c.closing = make(chan struct{})
	}
	return nil
}

func (c *Coordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing != nil {
		close(c.closing)
		c.closing = nil
	}
	return nil
}

// MapShards maps the points contained in wp to a ShardMapping.  If a point
// maps to a shard group or shard that does not currently exist, it will be
// created before returning the mapping.
func (c *Coordinator) MapShards(wp *WritePointsRequest) (*ShardMapping, error) {

	// Stub out the MapShards call to return a single node/shard setup
	if os.Getenv("INFLUXDB_ALPHA1") != "" {
		sm := NewShardMapping()
		sh := &meta.ShardInfo{
			ID:       uint64(1),
			OwnerIDs: []uint64{uint64(1)},
		}
		for _, p := range wp.Points {
			sm.MapPoint(sh, p)
		}
		return sm, nil
	}

	// holds the start time ranges for required shard groups
	timeRanges := map[time.Time]*meta.ShardGroupInfo{}

	rp, err := c.MetaStore.RetentionPolicy(wp.Database, wp.RetentionPolicy)
	if err != nil {
		return nil, err
	}

	for _, p := range wp.Points {
		timeRanges[p.Time().Truncate(rp.ShardGroupDuration)] = nil
	}

	// holds all the shard groups and shards that are required for writes
	for t := range timeRanges {
		sg, err := c.MetaStore.CreateShardGroupIfNotExists(wp.Database, wp.RetentionPolicy, t)
		if err != nil {
			return nil, err
		}
		timeRanges[t] = sg
	}

	mapping := NewShardMapping()
	for _, p := range wp.Points {
		sg := timeRanges[p.Time().Truncate(rp.ShardGroupDuration)]
		sh := sg.ShardFor(p.HashID())
		mapping.MapPoint(&sh, p)
	}
	return mapping, nil
}

// WritePoints is coordinates multiple writes across local and remote data nodes
// according the request consistency level
func (c *Coordinator) WritePoints(p *WritePointsRequest) error {
	shardMappings, err := c.MapShards(p)
	if err != nil {
		return err
	}

	// Write each shard in it's own goroutine and return as soon
	// as one fails.
	ch := make(chan error, len(shardMappings.Points))
	for shardID, points := range shardMappings.Points {
		go func(shard *meta.ShardInfo, database, retentionPolicy string, points []tsdb.Point) {
			ch <- c.writeToShard(shard, p.Database, p.RetentionPolicy, p.ConsistencyLevel, points)
		}(shardMappings.Shards[shardID], p.Database, p.RetentionPolicy, points)
	}

	for range shardMappings.Points {
		select {
		case <-c.closing:
			return ErrWriteFailed
		case err := <-ch:
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// writeToShards writes points to a shard and ensures a write consistency level has been met.  If the write
// partially succceds, ErrPartialWrite is returned.
func (c *Coordinator) writeToShard(shard *meta.ShardInfo, database, retentionPolicy string,
	consistency ConsistencyLevel, points []tsdb.Point) error {
	// The required number of writes to achieve the requested consistency level
	required := len(shard.OwnerIDs)
	switch consistency {
	case ConsistencyLevelAny, ConsistencyLevelOne:
		required = 1
	case ConsistencyLevelQuorum:
		required = required/2 + 1
	}

	// response channel for each shard writer go routine
	ch := make(chan error, len(shard.OwnerIDs))

	for _, nodeID := range shard.OwnerIDs {
		go func(shardID, nodeID uint64, points []tsdb.Point) {
			if c.nodeID == nodeID {
				err := c.Store.WriteToShard(shardID, points)
				// If we've written to shard that should exist on the current node, but the store has
				// not actually created this shard, tell it to create it and retry the write
				if err == tsdb.ErrShardNotFound {
					err = c.Store.CreateShard(database, retentionPolicy, shardID)
					if err != nil {
						ch <- err
						return
					}
					err = c.Store.WriteToShard(shardID, points)
				}
				ch <- err

				// FIXME: When ClusterWriter is implemented, this should never be nil
			} else if c.ClusterWriter != nil {
				ch <- c.ClusterWriter.Write(shardID, nodeID, points)
			} else {
				ch <- ErrWriteFailed
			}
		}(shard.ID, nodeID, points)
	}

	var wrote int
	timeout := time.After(defaultWriteTimeout)
	for range shard.OwnerIDs {
		select {
		case <-c.closing:
			return ErrWriteFailed
		case <-timeout:
			// return timeout error to caller
			return ErrTimeout
		case err := <-ch:
			// If the write returned an error, continue to the next response
			if err != nil {
				continue
			}

			wrote += 1

		}
	}

	// We wrote the required consistency level
	if wrote >= required {
		return nil
	}

	if wrote > 0 {
		return ErrPartialWrite
	}

	return ErrWriteFailed
}
