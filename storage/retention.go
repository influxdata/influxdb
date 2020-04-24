package storage

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/v1/tsdb/engine/tsm1"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	bucketAPITimeout = 10 * time.Second
)

// A Deleter implementation is capable of deleting data from a storage engine.
type Deleter interface {
	DeleteBucketRange(ctx context.Context, orgID, bucketID influxdb.ID, min, max int64) error
}

// A Snapshotter implementation can take snapshots of the entire engine.
type Snapshotter interface {
	WriteSnapshot(ctx context.Context, status tsm1.CacheStatus) error
}

// A BucketFinder is responsible for providing access to buckets via a filter.
type BucketFinder interface {
	FindBuckets(context.Context, influxdb.BucketFilter, ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error)
}

// ErrServiceClosed is returned when the service is unavailable.
var ErrServiceClosed = errors.New("service is currently closed")

// The retentionEnforcer periodically removes data that is outside of the retention
// period of the bucket associated with the data.
type retentionEnforcer struct {
	// Engine provides access to data stored on the engine
	Engine Deleter

	Snapshotter Snapshotter

	// BucketService provides an API for retrieving buckets associated with
	// organisations.
	BucketService BucketFinder

	logger *zap.Logger

	tracker *retentionTracker
}

// newRetentionEnforcer returns a new enforcer that ensures expired data is
// deleted every interval period. Setting interval to 0 is equivalent to
// disabling the service.
func newRetentionEnforcer(engine Deleter, snapshotter Snapshotter, bucketService BucketFinder) *retentionEnforcer {
	return &retentionEnforcer{
		Engine:        engine,
		Snapshotter:   snapshotter,
		BucketService: bucketService,
		logger:        zap.NewNop(),
		tracker:       newRetentionTracker(newRetentionMetrics(nil), nil),
	}
}

// SetDefaultMetricLabels sets the default labels for the retention metrics.
func (s *retentionEnforcer) SetDefaultMetricLabels(defaultLabels prometheus.Labels) {
	if s == nil {
		return // Not initialized
	}

	mmu.Lock()
	if rms == nil {
		rms = newRetentionMetrics(defaultLabels)
	}
	mmu.Unlock()

	s.tracker = newRetentionTracker(rms, defaultLabels)
}

// WithLogger sets the logger l on the service. It must be called before any run calls.
func (s *retentionEnforcer) WithLogger(l *zap.Logger) {
	if s == nil {
		return // Not initialised
	}
	s.logger = l.With(zap.String("component", "retention_enforcer"))
}

// run periodically expires (deletes) all data that's fallen outside of the
// retention period for the associated bucket.
func (s *retentionEnforcer) run() {
	if s == nil {
		return // Not initialized
	}

	span, ctx := tracing.StartSpanFromContext(context.Background())
	defer span.Finish()

	log, logEnd := logger.NewOperation(ctx, s.logger, "Data retention check", "data_retention_check")
	defer logEnd()

	now := time.Now().UTC()
	buckets, err := s.getBucketInformation(ctx)
	if err != nil {
		log.Error("Unable to determine bucket information", zap.Error(err))
	} else {
		s.expireData(ctx, buckets, now)
	}
	s.tracker.CheckDuration(time.Since(now), err == nil)
}

// expireData runs a delete operation on the storage engine.
//
// Any series data that (1) belongs to a bucket in the provided list and
// (2) falls outside the bucket's indicated retention period will be deleted.
func (s *retentionEnforcer) expireData(ctx context.Context, buckets []*influxdb.Bucket, now time.Time) {
	logger, logEnd := logger.NewOperation(ctx, s.logger, "Data deletion", "data_deletion",
		zap.Int("buckets", len(buckets)))
	defer logEnd()

	// Snapshot to clear the cache to reduce write contention.
	if err := s.Snapshotter.WriteSnapshot(ctx, tsm1.CacheStatusRetention); err != nil && err != tsm1.ErrSnapshotInProgress {
		logger.Warn("Unable to snapshot cache before retention", zap.Error(err))
	}

	var skipInf, skipInvalid int
	for _, b := range buckets {
		bucketFields := []zapcore.Field{
			zap.String("org_id", b.OrgID.String()),
			zap.String("bucket_id", b.ID.String()),
			zap.Duration("retention_period", b.RetentionPeriod),
			zap.String("system_type", b.Type.String()),
		}

		if b.RetentionPeriod == 0 {
			logger.Debug("Skipping bucket with infinite retention", bucketFields...)
			skipInf++
			continue
		} else if !b.OrgID.Valid() || !b.ID.Valid() {
			skipInvalid++
			logger.Warn("Skipping bucket with invalid fields", bucketFields...)
			continue
		}

		min := int64(math.MinInt64)
		max := now.Add(-b.RetentionPeriod).UnixNano()

		span, ctx := tracing.StartSpanFromContext(ctx)
		span.LogKV(
			"bucket_id", b.ID,
			"org_id", b.OrgID,
			"system_type", b.Type,
			"retention_period", b.RetentionPeriod,
			"retention_policy", b.RetentionPolicyName,
			"from", time.Unix(0, min).UTC(),
			"to", time.Unix(0, max).UTC(),
		)

		err := s.Engine.DeleteBucketRange(ctx, b.OrgID, b.ID, min, max)
		if err != nil {
			logger.Info("Unable to delete bucket range",
				append(bucketFields, zap.Time("min", time.Unix(0, min)), zap.Time("max", time.Unix(0, max)), zap.Error(err))...)
			tracing.LogError(span, err)
		}
		s.tracker.IncChecks(err == nil)
		span.Finish()
	}

	if skipInf > 0 || skipInvalid > 0 {
		logger.Info("Skipped buckets", zap.Int("infinite_retention_total", skipInf), zap.Int("invalid_total", skipInvalid))
	}
}

// getBucketInformation returns a slice of buckets to run retention on.
func (s *retentionEnforcer) getBucketInformation(ctx context.Context) ([]*influxdb.Bucket, error) {
	ctx, cancel := context.WithTimeout(ctx, bucketAPITimeout)
	defer cancel()

	buckets, _, err := s.BucketService.FindBuckets(ctx, influxdb.BucketFilter{})
	return buckets, err
}

//
// metrics tracker
//

type retentionTracker struct {
	metrics *retentionMetrics
	labels  prometheus.Labels
}

func newRetentionTracker(metrics *retentionMetrics, defaultLabels prometheus.Labels) *retentionTracker {
	return &retentionTracker{metrics: metrics, labels: defaultLabels}
}

// Labels returns a copy of labels for use with index cache metrics.
func (t *retentionTracker) Labels() prometheus.Labels {
	l := make(map[string]string, len(t.labels))
	for k, v := range t.labels {
		l[k] = v
	}
	return l
}

// IncChecks signals that a check happened for some bucket.
func (t *retentionTracker) IncChecks(success bool) {
	labels := t.Labels()

	if success {
		labels["status"] = "ok"
	} else {
		labels["status"] = "error"
	}

	t.metrics.Checks.With(labels).Inc()
}

// CheckDuration records the overall duration of a full retention check.
func (t *retentionTracker) CheckDuration(dur time.Duration, success bool) {
	labels := t.Labels()

	if success {
		labels["status"] = "ok"
	} else {
		labels["status"] = "error"
	}

	t.metrics.CheckDuration.With(labels).Observe(dur.Seconds())
}
