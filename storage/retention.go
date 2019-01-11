package storage

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	bucketAPITimeout = 10 * time.Second
	engineAPITimeout = time.Minute
)

// A Deleter implementation is capable of deleting data from a storage engine.
type Deleter interface {
	CreateSeriesCursor(context.Context, SeriesCursorRequest, influxql.Expr) (SeriesCursor, error)
	DeleteSeriesRangeWithPredicate(tsdb.SeriesIterator, func([]byte, models.Tags) (int64, int64, bool)) error
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

	rpByBucketID, err := s.getRetentionPeriodPerBucket()
	if err != nil {
		log.Error("Unable to determine bucket:RP mapping", zap.Error(err))
		return
	}

	now := time.Now().UTC()
	labels := s.metrics.Labels()
	labels["status"] = "ok"

	if err := s.expireData(rpByBucketID, now); err != nil {
		log.Error("Deletion not successful", zap.Error(err))
		labels["status"] = "error"
	}
	s.metrics.CheckDuration.With(labels).Observe(time.Since(now).Seconds())
	s.metrics.Checks.With(labels).Inc()
}

// expireData runs a delete operation on the storage engine.
//
// Any series data that (1) belongs to a bucket in the provided map and
// (2) falls outside the bucket's indicated retention period will be deleted.
func (s *retentionEnforcer) expireData(rpByBucketID map[platform.ID]time.Duration, now time.Time) error {
	_, logEnd := logger.NewOperation(s.logger, "Data deletion", "data_deletion")
	defer logEnd()

	ctx, cancel := context.WithTimeout(context.Background(), engineAPITimeout)
	defer cancel()
	cur, err := s.Engine.CreateSeriesCursor(ctx, SeriesCursorRequest{}, nil)
	if err != nil {
		return err
	}
	defer cur.Close()

	var mu sync.Mutex
	badMSketch := make(map[string]struct{})          // Badly formatted measurements.
	missingBSketch := make(map[platform.ID]struct{}) // Missing buckets.

	var seriesDeleted uint64 // Number of series where a delete is attempted.
	var seriesSkipped uint64 // Number of series that were skipped from delete.

	fn := func(name []byte, tags models.Tags) (int64, int64, bool) {
		if len(name) != platform.IDLength {
			mu.Lock()
			badMSketch[string(name)] = struct{}{}
			mu.Unlock()
			atomic.AddUint64(&seriesSkipped, 1)
			return 0, 0, false

		}

		var n [16]byte
		copy(n[:], name)
		_, bucketID := tsdb.DecodeName(n)

		retentionPeriod, ok := rpByBucketID[bucketID]
		if !ok {
			mu.Lock()
			missingBSketch[bucketID] = struct{}{}
			mu.Unlock()
			atomic.AddUint64(&seriesSkipped, 1)
			return 0, 0, false
		}
		if retentionPeriod == 0 {
			return 0, 0, false
		}

		atomic.AddUint64(&seriesDeleted, 1)
		to := now.Add(-retentionPeriod).UnixNano()
		return math.MinInt64, to, true
	}

	defer func() {
		if s.metrics == nil {
			return
		}
		labels := s.metrics.Labels()
		labels["status"] = "bad_measurement"
		s.metrics.Unprocessable.With(labels).Add(float64(len(badMSketch)))

		labels["status"] = "missing_bucket"
		s.metrics.Unprocessable.With(labels).Add(float64(len(missingBSketch)))

		labels["status"] = "ok"
		s.metrics.Series.With(labels).Add(float64(atomic.LoadUint64(&seriesDeleted)))

		labels["status"] = "skipped"
		s.metrics.Series.With(labels).Add(float64(atomic.LoadUint64(&seriesSkipped)))
	}()

	return s.Engine.DeleteSeriesRangeWithPredicate(newSeriesIteratorAdapter(cur), fn)
}

// getRetentionPeriodPerBucket returns a map of (bucket ID -> retention period)
// for all buckets.
func (s *retentionEnforcer) getRetentionPeriodPerBucket() (map[platform.ID]time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), bucketAPITimeout)
	defer cancel()
	buckets, _, err := s.BucketService.FindBuckets(ctx, platform.BucketFilter{})
	if err != nil {
		return nil, err
	}
	rpByBucketID := make(map[platform.ID]time.Duration, len(buckets))
	for _, bucket := range buckets {
		rpByBucketID[bucket.ID] = bucket.RetentionPeriod
	}
	return rpByBucketID, nil
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (s *retentionEnforcer) PrometheusCollectors() []prometheus.Collector {
	return s.metrics.PrometheusCollectors()
}

type seriesIteratorAdapter struct {
	itr  SeriesCursor
	ea   seriesElemAdapter
	elem tsdb.SeriesElem
}

func newSeriesIteratorAdapter(itr SeriesCursor) *seriesIteratorAdapter {
	si := &seriesIteratorAdapter{itr: itr}
	si.elem = &si.ea
	return si
}

// Next returns the next tsdb.SeriesElem.
//
// The returned tsdb.SeriesElem is valid for use until Next is called again.
func (s *seriesIteratorAdapter) Next() (tsdb.SeriesElem, error) {
	if s.itr == nil {
		return nil, nil
	}

	row, err := s.itr.Next()
	if err != nil {
		return nil, err
	}

	if row == nil {
		return nil, nil
	}

	s.ea.name = row.Name
	s.ea.tags = row.Tags
	return s.elem, nil
}

func (s *seriesIteratorAdapter) Close() error {
	if s.itr != nil {
		err := s.itr.Close()
		s.itr = nil
		return err
	}
	return nil
}

type seriesElemAdapter struct {
	name []byte
	tags models.Tags
}

func (e *seriesElemAdapter) Name() []byte        { return e.name }
func (e *seriesElemAdapter) Tags() models.Tags   { return e.tags }
func (e *seriesElemAdapter) Deleted() bool       { return false }
func (e *seriesElemAdapter) Expr() influxql.Expr { return nil }
