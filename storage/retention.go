package storage

import (
	"context"
	"errors"
	"math"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/logger"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	bucketAPITimeout = 10 * time.Second
)

// A Deleter implementation is capable of deleting data from a storage engine.
type Deleter interface {
	DeleteBucketRange(orgID, bucketID platform.ID, min, max int64) error
}

// A BucketFinder is responsible for providing access to buckets via a filter.
type BucketFinder interface {
	FindBuckets(context.Context, platform.BucketFilter, ...platform.FindOptions) ([]*platform.Bucket, int, error)
}

// ErrServiceClosed is returned when the service is unavailable.
var ErrServiceClosed = errors.New("service is currently closed")

// The retentionEnforcer periodically removes data that is outside of the retention
// period of the bucket associated with the data.
type retentionEnforcer struct {
	// Engine provides access to data stored on the engine
	Engine Deleter

	// BucketService provides an API for retrieving buckets associated with
	// organisations.
	BucketService BucketFinder

	logger *zap.Logger

	metrics *retentionMetrics
}

// newRetentionEnforcer returns a new enforcer that ensures expired data is
// deleted every interval period. Setting interval to 0 is equivalent to
// disabling the service.
func newRetentionEnforcer(engine Deleter, bucketService BucketFinder) *retentionEnforcer {
	s := &retentionEnforcer{
		Engine:        engine,
		BucketService: bucketService,
		logger:        zap.NewNop(),
	}
	s.metrics = newRetentionMetrics(nil)
	return s
}

// WithLogger sets the logger l on the service. It must be called before Open.
func (s *retentionEnforcer) WithLogger(l *zap.Logger) {
	if s == nil {
		return // Not initialised
	}
	s.logger = l.With(zap.String("component", "retention_enforcer"))
}

// run periodically expires (deletes) all data that's fallen outside of the
// retention period for the associated bucket.
func (s *retentionEnforcer) run() {
	log, logEnd := logger.NewOperation(s.logger, "Data retention check", "data_retention_check")
	defer logEnd()

	buckets, err := s.getBucketInformation()
	if err != nil {
		log.Error("Unable to determine bucket information", zap.Error(err))
		return
	}

	now := time.Now().UTC()
	labels := s.metrics.Labels()
	labels["status"] = "ok"

	if err := s.expireData(buckets, now); err != nil {
		log.Error("Deletion not successful", zap.Error(err))
		labels["status"] = "error"
	}
	s.metrics.CheckDuration.With(labels).Observe(time.Since(now).Seconds())
	s.metrics.Checks.With(labels).Inc()
}

// expireData runs a delete operation on the storage engine.
//
// Any series data that (1) belongs to a bucket in the provided list and
// (2) falls outside the bucket's indicated retention period will be deleted.
func (s *retentionEnforcer) expireData(buckets []*platform.Bucket, now time.Time) error {
	logger, logEnd := logger.NewOperation(s.logger, "Data deletion", "data_deletion")
	defer logEnd()

	for _, b := range buckets {
		if b.RetentionPeriod == 0 {
			continue
		}

		max := now.Add(-b.RetentionPeriod).UnixNano()
		err := s.Engine.DeleteBucketRange(b.OrganizationID, b.ID, math.MinInt64, max)
		if err != nil {
			// TODO(jeff): metrics?
			logger.Info("unable to delete bucket range",
				zap.String("bucket id", b.ID.String()),
				zap.String("org id", b.OrganizationID.String()),
				zap.Error(err))
		}
	}

	return nil
}

// getBucketInformation returns a slice of buckets to run retention on.
func (s *retentionEnforcer) getBucketInformation() ([]*platform.Bucket, error) {
	ctx, cancel := context.WithTimeout(context.Background(), bucketAPITimeout)
	defer cancel()

	buckets, _, err := s.BucketService.FindBuckets(ctx, platform.BucketFilter{})
	return buckets, err
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (s *retentionEnforcer) PrometheusCollectors() []prometheus.Collector {
	return s.metrics.PrometheusCollectors()
}
