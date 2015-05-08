package influxdb

import (
	"errors"
	"time"

	"github.com/influxdb/influxdb/data"
	"github.com/influxdb/influxdb/meta"
)

const defaultReadTimeout = 5 * time.Second

var ErrTimeout = errors.New("timeout")

// Coordinator handle queries and writes across multiple local and remote
// data nodes.
type Coordinator struct {
	MetaStore meta.Store
	DataNode  data.Node
}

// ShardMapping contiains a mapping of a shardIDs to a points
type ShardMapping map[uint64][]Point

func (c *Coordinator) MapShards(wp *WritePointsRequest) (ShardMapping, error) {

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
		g, err := c.MetaStore.CreateShardGroupIfNotExists(wp.Database, wp.RetentionPolicy, t)
		if err != nil {
			return nil, err
		}
		timeRanges[t] = g
	}

	shardMapping := make(ShardMapping)
	for _, p := range wp.Points {
		g, ok := timeRanges[p.Time.Truncate(rp.ShardGroupDuration)]

		sid := p.SeriesID()
		shardInfo := g.Shards[sid%uint64(len(g.Shards))]
		points, ok := shardMapping[shardInfo.ID]
		if !ok {
			shardMapping[shardInfo.ID] = []Point{p}
		} else {
			shardMapping[shardInfo.ID] = append(points, p)
		}
	}
	return shardMapping, nil
}

// Write is coordinates multiple writes across local and remote data nodes
// according the request consistency level
func (c *Coordinator) Write(p *WritePointsRequest) error {

	// FIXME: use the consistency level specified by the WritePointsRequest
	pol := newConsistencyPolicyN(1)

	_, err := c.MapShards(p)
	if err != nil {
		return err
	}

	// FIXME: build set of local and remote point writers
	ws := []PointsWriter{}

	type result struct {
		writerID int
		err      error
	}
	ch := make(chan result, len(ws))
	for i, w := range ws {
		go func(id int, w PointsWriter) {
			err := w.Write(p)
			ch <- result{id, err}
		}(i, w)
	}
	timeout := time.After(defaultReadTimeout)
	for range ws {
		select {
		case <-timeout:
			// return timeout error to caller
			return ErrTimeout
		case res := <-ch:
			if !pol.IsDone(res.writerID, res.err) {
				continue
			}
			if res.err != nil {
				return res.err
			}
			return nil
		}

	}
	panic("unreachable or bad policy impl")
}

func (c *Coordinator) Execute(q *QueryRequest) (chan *Result, error) {
	return nil, nil
}

// remoteWriter is a PointWriter for a remote data node
type remoteWriter struct {
	//ShardInfo []ShardInfo
	//DataNodes DataNodes
}

func (w *remoteWriter) Write(p *WritePointsRequest) error {
	return nil
}
