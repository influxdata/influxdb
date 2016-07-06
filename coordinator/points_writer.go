package coordinator

import (
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/expvar"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
)

// The statistics generated by the "write" mdoule
const (
	statWriteReq           = "req"
	statPointWriteReq      = "pointReq"
	statPointWriteReqLocal = "pointReqLocal"
	statWriteOK            = "writeOk"
	statWriteDrop          = "writeDrop"
	statWriteTimeout       = "writeTimeout"
	statWriteErr           = "writeError"
	statSubWriteOK         = "subWriteOk"
	statSubWriteDrop       = "subWriteDrop"
)

var (
	// ErrTimeout is returned when a write times out.
	ErrTimeout = errors.New("timeout")

	// ErrPartialWrite is returned when a write partially succeeds but does
	// not meet the requested consistency level.
	ErrPartialWrite = errors.New("partial write")

	// ErrWriteFailed is returned when no writes succeeded.
	ErrWriteFailed = errors.New("write failed")
)

// PointsWriter handles writes across multiple local and remote data nodes.
type PointsWriter struct {
	mu           sync.RWMutex
	closing      chan struct{}
	WriteTimeout time.Duration
	Logger       *log.Logger

	Node *influxdb.Node

	MetaClient interface {
		Database(name string) (di *meta.DatabaseInfo)
		RetentionPolicy(database, policy string) (*meta.RetentionPolicyInfo, error)
		CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
		ShardOwner(shardID uint64) (string, string, *meta.ShardGroupInfo)
	}

	TSDBStore interface {
		CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error
		WriteToShard(shardID uint64, points []models.Point) error
	}

	ShardWriter interface {
		WriteShard(shardID, ownerID uint64, points []models.Point) error
	}

	Subscriber interface {
		Points() chan<- *WritePointsRequest
	}
	subPoints chan<- *WritePointsRequest

	statMap *expvar.Map
}

// WritePointsRequest represents a request to write point data to the cluster
type WritePointsRequest struct {
	Database        string
	RetentionPolicy string
	Points          []models.Point
}

// AddPoint adds a point to the WritePointRequest with field key 'value'
func (w *WritePointsRequest) AddPoint(name string, value interface{}, timestamp time.Time, tags map[string]string) {
	pt, err := models.NewPoint(
		name, tags, map[string]interface{}{"value": value}, timestamp,
	)
	if err != nil {
		return
	}
	w.Points = append(w.Points, pt)
}

// NewPointsWriter returns a new instance of PointsWriter for a node.
func NewPointsWriter() *PointsWriter {
	return &PointsWriter{
		closing:      make(chan struct{}),
		WriteTimeout: DefaultWriteTimeout,
		Logger:       log.New(os.Stderr, "[write] ", log.LstdFlags),
		statMap:      influxdb.NewStatistics("write", "write", nil),
	}
}

// ShardMapping contains a mapping of a shards to a points.
type ShardMapping struct {
	Points map[uint64][]models.Point  // The points associated with a shard ID
	Shards map[uint64]*meta.ShardInfo // The shards that have been mapped, keyed by shard ID
}

// NewShardMapping creates an empty ShardMapping
func NewShardMapping() *ShardMapping {
	return &ShardMapping{
		Points: map[uint64][]models.Point{},
		Shards: map[uint64]*meta.ShardInfo{},
	}
}

// MapPoint maps a point to shard
func (s *ShardMapping) MapPoint(shardInfo *meta.ShardInfo, p models.Point) {
	points, ok := s.Points[shardInfo.ID]
	if !ok {
		s.Points[shardInfo.ID] = []models.Point{p}
	} else {
		s.Points[shardInfo.ID] = append(points, p)
	}
	s.Shards[shardInfo.ID] = shardInfo
}

// Open opens the communication channel with the point writer
func (w *PointsWriter) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closing = make(chan struct{})
	if w.Subscriber != nil {
		w.subPoints = w.Subscriber.Points()
	}
	return nil
}

// Close closes the communication channel with the point writer
func (w *PointsWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closing != nil {
		close(w.closing)
	}
	if w.subPoints != nil {
		// 'nil' channels always block so this makes the
		// select statement in WritePoints hit its default case
		// dropping any in-flight writes.
		w.subPoints = nil
	}
	return nil
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (w *PointsWriter) SetLogOutput(lw io.Writer) {
	w.Logger = log.New(lw, "[write] ", log.LstdFlags)
}

// MapShards maps the points contained in wp to a ShardMapping.  If a point
// maps to a shard group or shard that does not currently exist, it will be
// created before returning the mapping.
func (w *PointsWriter) MapShards(wp *WritePointsRequest) (*ShardMapping, error) {

	// holds the start time ranges for required shard groups
	timeRanges := map[time.Time]*meta.ShardGroupInfo{}

	rp, err := w.MetaClient.RetentionPolicy(wp.Database, wp.RetentionPolicy)
	if err != nil {
		return nil, err
	} else if rp == nil {
		return nil, influxdb.ErrRetentionPolicyNotFound(wp.RetentionPolicy)
	}

	// Find the minimum time for a point if the retention policy has a shard
	// group duration. We will automatically drop any points before this time.
	// There is a chance of a time on the edge of the shard group duration to
	// sneak through even after it has been removed, but the circumstances are
	// rare enough and don't matter enough that we don't account for this
	// edge case.
	min := time.Unix(0, models.MinNanoTime)
	if rp.Duration > 0 {
		min = time.Now().Add(-rp.Duration)
	}

	for _, p := range wp.Points {
		if p.Time().Before(min) {
			continue
		}
		timeRanges[p.Time().Truncate(rp.ShardGroupDuration)] = nil
	}

	// holds all the shard groups and shards that are required for writes
	for t := range timeRanges {
		sg, err := w.MetaClient.CreateShardGroup(wp.Database, wp.RetentionPolicy, t)
		if err != nil {
			return nil, err
		}
		timeRanges[t] = sg
	}

	mapping := NewShardMapping()
	for _, p := range wp.Points {
		sg, ok := timeRanges[p.Time().Truncate(rp.ShardGroupDuration)]
		if !ok {
			w.statMap.Add(statWriteDrop, 1)
			continue
		}
		sh := sg.ShardFor(p.HashID())
		mapping.MapPoint(&sh, p)
	}
	return mapping, nil
}

// WritePointsInto is a copy of WritePoints that uses a tsdb structure instead of
// a cluster structure for information. This is to avoid a circular dependency
func (w *PointsWriter) WritePointsInto(p *IntoWriteRequest) error {
	return w.WritePoints(p.Database, p.RetentionPolicy, models.ConsistencyLevelOne, p.Points)
}

// WritePoints writes across multiple local and remote data nodes according the consistency level.
func (w *PointsWriter) WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	w.statMap.Add(statWriteReq, 1)
	w.statMap.Add(statPointWriteReq, int64(len(points)))

	if retentionPolicy == "" {
		db := w.MetaClient.Database(database)
		if db == nil {
			return influxdb.ErrDatabaseNotFound(database)
		}
		retentionPolicy = db.DefaultRetentionPolicy
	}

	shardMappings, err := w.MapShards(&WritePointsRequest{Database: database, RetentionPolicy: retentionPolicy, Points: points})
	if err != nil {
		return err
	}

	// Write each shard in it's own goroutine and return as soon
	// as one fails.
	ch := make(chan error, len(shardMappings.Points))
	for shardID, points := range shardMappings.Points {
		go func(shard *meta.ShardInfo, database, retentionPolicy string, points []models.Point) {
			ch <- w.writeToShard(shard, database, retentionPolicy, points)
		}(shardMappings.Shards[shardID], database, retentionPolicy, points)
	}

	// Send points to subscriptions if possible.
	ok := false
	// We need to lock just in case the channel is about to be nil'ed
	w.mu.RLock()
	select {
	case w.subPoints <- &WritePointsRequest{Database: database, RetentionPolicy: retentionPolicy, Points: points}:
		ok = true
	default:
	}
	w.mu.RUnlock()
	if ok {
		w.statMap.Add(statSubWriteOK, 1)
	} else {
		w.statMap.Add(statSubWriteDrop, 1)
	}

	timeout := time.NewTimer(w.WriteTimeout)
	defer timeout.Stop()
	for range shardMappings.Points {
		select {
		case <-w.closing:
			return ErrWriteFailed
		case <-timeout.C:
			w.statMap.Add(statWriteTimeout, 1)
			// return timeout error to caller
			return ErrTimeout
		case err := <-ch:
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// writeToShards writes points to a shard.
func (w *PointsWriter) writeToShard(shard *meta.ShardInfo, database, retentionPolicy string, points []models.Point) error {
	w.statMap.Add(statPointWriteReqLocal, int64(len(points)))

	err := w.TSDBStore.WriteToShard(shard.ID, points)
	if err == nil {
		w.statMap.Add(statWriteOK, 1)
		return nil
	}

	// If we've written to shard that should exist on the current node, but the store has
	// not actually created this shard, tell it to create it and retry the write
	if err == tsdb.ErrShardNotFound {
		err = w.TSDBStore.CreateShard(database, retentionPolicy, shard.ID, true)
		if err != nil {
			w.Logger.Printf("write failed for shard %d: %v", shard.ID, err)
			w.statMap.Add(statWriteErr, 1)
			return err
		}
	}
	err = w.TSDBStore.WriteToShard(shard.ID, points)
	if err != nil {
		w.Logger.Printf("write failed for shard %d: %v", shard.ID, err)
		w.statMap.Add(statWriteErr, 1)
		return err
	}

	w.statMap.Add(statWriteOK, 1)
	return nil
}
