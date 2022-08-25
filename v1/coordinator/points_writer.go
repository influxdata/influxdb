package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	influxdb "github.com/influxdata/influxdb/v2/v1"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	// ErrTimeout is returned when a write times out.
	ErrTimeout = errors.New("timeout")

	// ErrWriteFailed is returned when no writes succeeded.
	ErrWriteFailed = errors.New("write failed")
)

// PointsWriter handles writes across multiple local and remote data nodes.
type PointsWriter struct {
	mu           sync.RWMutex
	closing      chan struct{}
	WriteTimeout time.Duration
	Logger       *zap.Logger

	Node *influxdb.Node

	MetaClient interface {
		Database(name string) (di *meta.DatabaseInfo)
		RetentionPolicy(database, policy string) (*meta.RetentionPolicyInfo, error)
		CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	}

	TSDBStore interface {
		CreateShard(ctx context.Context, database, retentionPolicy string, shardID uint64, enabled bool) error
		WriteToShard(ctx context.Context, shardID uint64, points []models.Point) error
	}

	stats *engineWriteMetrics
}

// WritePointsRequest represents a request to write point data to the cluster.
type WritePointsRequest struct {
	Database        string
	RetentionPolicy string
	Points          []models.Point
}

// AddPoint adds a point to the WritePointRequest with field key 'value'
func (w *WritePointsRequest) AddPoint(name string, value interface{}, timestamp time.Time, tags map[string]string) {
	pt, err := models.NewPoint(
		name, models.NewTags(tags), map[string]interface{}{"value": value}, timestamp,
	)
	if err != nil {
		return
	}
	w.Points = append(w.Points, pt)
}

// NewPointsWriter returns a new instance of PointsWriter for a node.
func NewPointsWriter(writeTimeout time.Duration, path string) *PointsWriter {
	return &PointsWriter{
		closing:      make(chan struct{}),
		WriteTimeout: writeTimeout,
		Logger:       zap.NewNop(),
		stats:        newEngineWriteMetrics(path),
	}
}

// ShardMapping contains a mapping of shards to points.
type ShardMapping struct {
	n       int
	Points  map[uint64][]models.Point  // The points associated with a shard ID
	Shards  map[uint64]*meta.ShardInfo // The shards that have been mapped, keyed by shard ID
	Dropped []models.Point             // Points that were dropped
}

// NewShardMapping creates an empty ShardMapping.
func NewShardMapping(n int) *ShardMapping {
	return &ShardMapping{
		n:      n,
		Points: map[uint64][]models.Point{},
		Shards: map[uint64]*meta.ShardInfo{},
	}
}

// MapPoint adds the point to the ShardMapping, associated with the given shardInfo.
func (s *ShardMapping) MapPoint(shardInfo *meta.ShardInfo, p models.Point) {
	if cap(s.Points[shardInfo.ID]) < s.n {
		s.Points[shardInfo.ID] = make([]models.Point, 0, s.n)
	}
	s.Points[shardInfo.ID] = append(s.Points[shardInfo.ID], p)
	s.Shards[shardInfo.ID] = shardInfo
}

// Open opens the communication channel with the point writer.
func (w *PointsWriter) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closing = make(chan struct{})
	return nil
}

// Close closes the communication channel with the point writer.
func (w *PointsWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closing != nil {
		close(w.closing)
	}
	return nil
}

// WithLogger sets the Logger on w.
func (w *PointsWriter) WithLogger(log *zap.Logger) {
	w.Logger = log.With(zap.String("service", "write"))
}

var globalPointsWriteMetrics *writeMetrics = newWriteMetrics()

type writeMetrics struct {
	// labels: type: requested,ok,dropped,err
	pointsWriteRequested *prometheus.HistogramVec
	pointsWriteOk        *prometheus.HistogramVec
	pointsWriteDropped   *prometheus.HistogramVec
	pointsWriteErr       *prometheus.HistogramVec
	timeout              *prometheus.CounterVec
}

// PrometheusCollectors returns all prometheus metrics for the tsm1 package.
func PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		globalPointsWriteMetrics.pointsWriteRequested,
		globalPointsWriteMetrics.pointsWriteOk,
		globalPointsWriteMetrics.pointsWriteDropped,
		globalPointsWriteMetrics.pointsWriteErr,
		globalPointsWriteMetrics.timeout,
	}
}

const namespace = "storage"
const writerSubsystem = "writer"

func newWriteMetrics() *writeMetrics {
	labels := []string{"path"}
	writeBuckets := []float64{10, 100, 1000, 10000, 100000}
	return &writeMetrics{
		pointsWriteRequested: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: writerSubsystem,
			Name:      "req_points",
			Help:      "Histogram of number of points requested to be written",
			Buckets:   writeBuckets,
		}, labels),
		pointsWriteOk: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: writerSubsystem,
			Name:      "ok_points",
			Help:      "Histogram of number of points in successful shard write requests",
			Buckets:   writeBuckets,
		}, labels),
		pointsWriteDropped: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: writerSubsystem,
			Name:      "dropped_points",
			Help:      "Histogram of number of points dropped due to partial writes",
			Buckets:   writeBuckets,
		}, labels),
		pointsWriteErr: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: writerSubsystem,
			Name:      "err_points",
			Help:      "Histogram of number of points in errored shard write requests",
			Buckets:   writeBuckets,
		}, labels),
		timeout: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   writerSubsystem,
			Name:        "timeouts",
			Help:        "Number of shard write request timeouts",
			ConstLabels: nil,
		}, labels),
	}
}

type engineWriteMetrics struct {
	pointsWriteRequested prometheus.Observer
	pointsWriteOk        prometheus.Observer
	pointsWriteDropped   prometheus.Observer
	pointsWriteErr       prometheus.Observer
	timeout              prometheus.Counter
}

func newEngineWriteMetrics(path string) *engineWriteMetrics {
	return &engineWriteMetrics{
		pointsWriteRequested: globalPointsWriteMetrics.pointsWriteRequested.With(prometheus.Labels{"path": path}),
		pointsWriteOk:        globalPointsWriteMetrics.pointsWriteOk.With(prometheus.Labels{"path": path}),
		pointsWriteDropped:   globalPointsWriteMetrics.pointsWriteDropped.With(prometheus.Labels{"path": path}),
		pointsWriteErr:       globalPointsWriteMetrics.pointsWriteErr.With(prometheus.Labels{"path": path}),
		timeout:              globalPointsWriteMetrics.timeout.With(prometheus.Labels{"path": path}),
	}
}

// MapShards maps the points contained in wp to a ShardMapping.  If a point
// maps to a shard group or shard that does not currently exist, it will be
// created before returning the mapping.
func (w *PointsWriter) MapShards(wp *WritePointsRequest) (*ShardMapping, error) {
	rp, err := w.MetaClient.RetentionPolicy(wp.Database, wp.RetentionPolicy)
	if err != nil {
		return nil, err
	} else if rp == nil {
		return nil, influxdb.ErrRetentionPolicyNotFound(wp.RetentionPolicy)
	}

	// Holds all the shard groups and shards that are required for writes.
	list := sgList{items: make(meta.ShardGroupInfos, 0, 8)}
	min := time.Unix(0, models.MinNanoTime)
	if rp.Duration > 0 {
		min = time.Now().Add(-rp.Duration)
	}

	for _, p := range wp.Points {
		// Either the point is outside the scope of the RP, or we already have
		// a suitable shard group for the point.
		if p.Time().Before(min) || list.Covers(p.Time()) {
			continue
		}

		// No shard groups overlap with the point's time, so we will create
		// a new shard group for this point.
		sg, err := w.MetaClient.CreateShardGroup(wp.Database, wp.RetentionPolicy, p.Time())
		if err != nil {
			return nil, err
		}

		if sg == nil {
			return nil, errors.New("nil shard group")
		}
		list.Add(*sg)
	}

	mapping := NewShardMapping(len(wp.Points))
	for _, p := range wp.Points {
		sg := list.ShardGroupAt(p.Time())
		if sg == nil {
			// We didn't create a shard group because the point was outside the
			// scope of the RP.
			mapping.Dropped = append(mapping.Dropped, p)
			continue
		}

		sh := sg.ShardFor(p)
		mapping.MapPoint(&sh, p)
	}

	return mapping, nil
}

// sgList is a wrapper around a meta.ShardGroupInfos where we can also check
// if a given time is covered by any of the shard groups in the list.
type sgList struct {
	items meta.ShardGroupInfos

	// needsSort indicates if items has been modified without a sort operation.
	needsSort bool

	// earliest is the last begin time of any item in items.
	earliest time.Time

	// latest is the greatest end time of any item in items.
	latest time.Time
}

func (l sgList) Covers(t time.Time) bool {
	if len(l.items) == 0 {
		return false
	}
	return l.ShardGroupAt(t) != nil
}

// ShardGroupAt attempts to find a shard group that could contain a point
// at the given time.
//
// Shard groups are sorted first according to end time, and then according
// to start time. Therefore, if there are multiple shard groups that match
// this point's time they will be preferred in this order:
//
//   - a shard group with the earliest end time;
//   - (assuming identical end times) the shard group with the earliest start time.
func (l sgList) ShardGroupAt(t time.Time) *meta.ShardGroupInfo {
	if l.items.Len() == 0 {
		return nil
	}

	// find the earliest shardgroup that could contain this point using binary search.
	if l.needsSort {
		sort.Sort(l.items)
		l.needsSort = false
	}
	idx := sort.Search(l.items.Len(), func(i int) bool { return l.items[i].EndTime.After(t) })

	// Check if sort.Search actually found the proper shard. It feels like we should also
	// be checking l.items[idx].EndTime, but sort.Search was looking at that field for us.
	if idx == l.items.Len() || t.Before(l.items[idx].StartTime) {
		// This could mean we are looking for a time not in the list, or we have
		// overlaping shards. Overlapping shards do not work with binary searches
		// on 1d arrays. You have to use an interval tree, but that's a lot of
		// work for what is hopefully a rare event. Instead, we'll check if t
		// should be in l, and perform a linear search if it is. This way we'll
		// do the correct thing, it may just take a little longer. If we don't
		// do this, then we may non-silently drop writes we should have accepted.

		if t.Before(l.earliest) || t.After(l.latest) {
			// t is not in range, we can avoid going through the linear search.
			return nil
		}

		// Oh no, we've probably got overlapping shards. Perform a linear search.
		for idx = 0; idx < l.items.Len(); idx++ {
			if l.items[idx].Contains(t) {
				// Found it!
				break
			}
		}
		if idx == l.items.Len() {
			// We did not find a shard which contained t. This is very strange.
			return nil
		}
	}

	return &l.items[idx]
}

// Add appends a shard group to the list, updating the earliest/latest times of the list if needed.
func (l *sgList) Add(sgi meta.ShardGroupInfo) {
	l.items = append(l.items, sgi)
	l.needsSort = true

	// Update our earliest and latest times for l.items
	if l.earliest.IsZero() || l.earliest.After(sgi.StartTime) {
		l.earliest = sgi.StartTime
	}
	if l.latest.IsZero() || l.latest.Before(sgi.EndTime) {
		l.latest = sgi.EndTime
	}
}

// WritePoints writes the data to the underlying storage. consistencyLevel and user are only used for clustered scenarios
func (w *PointsWriter) WritePoints(
	ctx context.Context,
	database, retentionPolicy string,
	consistencyLevel models.ConsistencyLevel,
	user meta.User,
	points []models.Point,
) error {
	return w.WritePointsPrivileged(ctx, database, retentionPolicy, consistencyLevel, points)
}

// WritePointsPrivileged writes the data to the underlying storage, consistencyLevel is only used for clustered scenarios
func (w *PointsWriter) WritePointsPrivileged(
	ctx context.Context,
	database, retentionPolicy string,
	consistencyLevel models.ConsistencyLevel,
	points []models.Point,
) error {
	w.stats.pointsWriteRequested.Observe(float64(len(points)))

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

	// Write each shard in it's own goroutine and return as soon as one fails.
	ch := make(chan error, len(shardMappings.Points))
	for shardID, points := range shardMappings.Points {
		go func(shard *meta.ShardInfo, database, retentionPolicy string, points []models.Point) {
			err := w.writeToShard(ctx, shard, database, retentionPolicy, points)
			if err == nil {
				w.stats.pointsWriteOk.Observe(float64(len(points)))
			} else {
				w.stats.pointsWriteErr.Observe(float64(len(points)))
			}
			if err == tsdb.ErrShardDeletion {
				err = tsdb.PartialWriteError{Reason: fmt.Sprintf("shard %d is pending deletion", shard.ID), Dropped: len(points)}
			}
			ch <- err
		}(shardMappings.Shards[shardID], database, retentionPolicy, points)
	}

	if len(shardMappings.Dropped) > 0 {
		w.stats.pointsWriteDropped.Observe(float64(len(shardMappings.Dropped)))
		err = tsdb.PartialWriteError{Reason: "points beyond retention policy", Dropped: len(shardMappings.Dropped)}
	}
	timeout := time.NewTimer(w.WriteTimeout)
	defer timeout.Stop()
	for range shardMappings.Points {
		select {
		case <-w.closing:
			return ErrWriteFailed
		case <-timeout.C:
			w.stats.timeout.Inc()
			// return timeout error to caller
			return ErrTimeout
		case err := <-ch:
			if err != nil {
				return err
			}
		}
	}
	return err
}

// writeToShards writes points to a shard.
func (w *PointsWriter) writeToShard(ctx context.Context, shard *meta.ShardInfo, database, retentionPolicy string, points []models.Point) error {
	err := w.TSDBStore.WriteToShard(ctx, shard.ID, points)
	if err == nil {
		return nil
	}

	// Except tsdb.ErrShardNotFound no error can be handled here
	if err != tsdb.ErrShardNotFound {
		return err
	}

	// If we've written to shard that should exist on the current node, but the store has
	// not actually created this shard, tell it to create it and retry the write
	if err = w.TSDBStore.CreateShard(ctx, database, retentionPolicy, shard.ID, true); err != nil {
		w.Logger.Warn("Write failed creating shard", zap.Uint64("shard", shard.ID), zap.Error(err))
		return err
	}

	if err = w.TSDBStore.WriteToShard(ctx, shard.ID, points); err != nil {
		w.Logger.Info("Write failed", zap.Uint64("shard", shard.ID), zap.Error(err))
		return err
	}

	return nil
}
