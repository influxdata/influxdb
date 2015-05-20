package influxdb

import (
	"errors"
	"sync"
	"time"

	"github.com/influxdb/influxdb/data"
	"github.com/influxdb/influxdb/meta"
)

const defaultWriteTimeout = 5 * time.Second

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
	mu      sync.RWMutex
	closing chan struct{}

	MetaStore    meta.Store
	shardWriters []ShardWriter
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		closing: make(chan struct{}),
	}
}

// ShardMapping contains a mapping of a shards to a points.
type ShardMapping struct {
	Points map[uint64][]data.Point   // The points associated with a shard ID
	Shards map[uint64]meta.ShardInfo // The shards that have been mapped, keyed by shard ID
}

// NewShardMapping creates an empty ShardMapping
func NewShardMapping() *ShardMapping {
	return &ShardMapping{
		Points: map[uint64][]data.Point{},
		Shards: map[uint64]meta.ShardInfo{},
	}
}

// MapPoint maps a point to shard
func (s *ShardMapping) MapPoint(shardInfo meta.ShardInfo, p data.Point) {
	points, ok := s.Points[shardInfo.ID]
	if !ok {
		s.Points[shardInfo.ID] = []data.Point{p}
	} else {
		s.Points[shardInfo.ID] = append(points, p)
	}
	s.Shards[shardInfo.ID] = shardInfo
}

// ShardWriter provides the ability to write a slice of points to s given shard ID.
// It should return the number of times the set of points was written or an error
// if the write failed.
type ShardWriter interface {
	WriteShard(shardID uint64, points []data.Point) (int, error)
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

func (c *Coordinator) AddShardWriter(s ShardWriter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shardWriters = append(c.shardWriters, s)
}

// MapShards maps the points contained in wp to a ShardMapping.  If a point
// maps to a shard group or shard that does not currently exist, it will be
// created before returning the mapping.
func (c *Coordinator) MapShards(wp *WritePointsRequest) (*ShardMapping, error) {

	// holds the start time ranges for required shard groups
	timeRanges := map[time.Time]*meta.ShardGroupInfo{}

	rp, err := c.MetaStore.RetentionPolicy(wp.Database, wp.RetentionPolicy)
	if err != nil {
		return nil, err
	}

	for _, p := range wp.Points {
		timeRanges[p.Time.Truncate(rp.ShardGroupDuration)] = nil
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
		sg := timeRanges[p.Time.Truncate(rp.ShardGroupDuration)]
		sh := sg.ShardFor(p)
		mapping.MapPoint(sh, p)
	}
	return mapping, nil
}

// Write is coordinates multiple writes across local and remote data nodes
// according the request consistency level
func (c *Coordinator) Write(p *WritePointsRequest) error {
	shardMappings, err := c.MapShards(p)
	if err != nil {
		return err
	}

	// Write each shard in it's own goroutine and return as soon
	// as one fails.
	ch := make(chan error, len(shardMappings.Points))
	for shardID, points := range shardMappings.Points {
		go func(shard meta.ShardInfo, points []data.Point) {
			ch <- c.writeToShards(shard, p.ConsistencyLevel, points)
		}(shardMappings.Shards[shardID], points)
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
func (c *Coordinator) writeToShards(shard meta.ShardInfo, consistency ConsistencyLevel, points []data.Point) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Abort early if there are no writer configured yet
	if len(c.shardWriters) == 0 {
		return ErrWriteFailed
	}

	// The required number of writes to achieve the requested consistency level
	required := len(shard.OwnerIDs)
	switch consistency {
	case ConsistencyLevelAny, ConsistencyLevelOne:
		required = 1
	case ConsistencyLevelQuorum:
		required = required/2 + 1
	}

	// holds the response to the ShardWriter.Write calls
	type result struct {
		wrote int
		err   error
	}

	// response channel for each shard writer go routine
	ch := make(chan result, len(c.shardWriters))

	for _, w := range c.shardWriters {
		// write to each ShardWriter (local and remote), in parallel
		go func(w ShardWriter, shardID uint64, points []data.Point) {
			wrote, err := w.WriteShard(shardID, points)
			ch <- result{wrote, err}
		}(w, shard.ID, points)
	}

	var wrote int
	timeout := time.After(defaultWriteTimeout)
	for range c.shardWriters {
		select {
		case <-c.closing:
			return ErrWriteFailed
		case <-timeout:
			// return timeout error to caller
			return ErrTimeout
		case res := <-ch:
			wrote += res.wrote

			// If the write returned an error, continue to the next response
			if res.err != nil {
				continue
			}

			// We wrote the required consistency level
			if wrote >= required {
				return nil
			}
		}
	}

	if wrote > 0 {
		return ErrPartialWrite
	}

	return ErrWriteFailed
}

func (c *Coordinator) Execute(q *QueryRequest) (chan *Result, error) {
	return nil, nil
}
