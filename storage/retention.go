package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxql"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/kit/prom"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/tsdb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	serviceName      = "retention"
	bucketAPITimeout = 10 * time.Second
	engineAPITimeout = time.Minute
)

type RetentionService interface {
	prom.PrometheusCollector

	Open() error
	Close() error
	WithLogger(*zap.Logger)
}

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

// The retentionService periodically removes data that is outside of the retention
// period of the bucket associated with the data.
type retentionService struct {
	// Engine provides access to data stored on the engine
	Engine Deleter

	// BucketService provides an API for retrieving buckets associated with
	// organisations.
	BucketService BucketFinder

	logger   *zap.Logger
	interval time.Duration // Interval that retention service deletes data.

	retentionMetrics    *retentionMetrics
	defaultMetricLabels prometheus.Labels // N.B this must not be mutated after Open is called.

	mu       sync.RWMutex
	_closing chan struct{}

	wg sync.WaitGroup
}

// newRetentionService returns a new Service that performs deletes
// every interval period. Setting interval to 0 is equivalent to disabling the
// service.
func newRetentionService(engine Deleter, bucketService BucketFinder, interval int64) *retentionService {
	s := &retentionService{
		Engine:        engine,
		BucketService: bucketService,
		logger:        zap.NewNop(),
		interval:      time.Duration(interval) * time.Second,
	}
	return s
}

// metricLabels returns a new copy of the default metric labels.
func (s *retentionService) metricLabels() prometheus.Labels {
	labels := make(map[string]string, len(s.defaultMetricLabels))
	for k, v := range s.defaultMetricLabels {
		labels[k] = v
	}
	return labels
}

// WithLogger sets the logger l on the service. It must be called before Open.
func (s *retentionService) WithLogger(l *zap.Logger) {
	s.logger = l.With(zap.String("service", serviceName))
}

// Open opens the service, which begins the process of removing expired data.
// Re-opening the service once it's open is a no-op.
func (s *retentionService) Open() error {
	if s.closing() != nil {
		return nil // Already open.
	}

	s.logger.Info("Service opening", zap.Duration("check_interval", s.interval))
	if s.interval < 0 {
		return fmt.Errorf("invalid interval %v", s.interval)
	}

	s.mu.Lock()
	s._closing = make(chan struct{})
	s.mu.Unlock()

	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.run() }()
	s.logger.Info("Service finished opening")

	return nil
}

// run periodically expires (deletes) all data that's fallen outside of the
// retention period for the associated bucket.
func (s *retentionService) run() {
	if s.interval == 0 {
		s.logger.Info("Service disabled")
		return
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	closingCh := s.closing()
	for {
		select {
		case <-ticker.C:
			log, logEnd := logger.NewOperation(s.logger, "Data retention check", "data_retention_check")
			defer logEnd()

			rpByBucketID, err := s.getRetentionPeriodPerBucket()
			if err != nil {
				log.Error("Unable to determine bucket:RP mapping", zap.Error(err))
				return
			}

			now := time.Now().UTC()
			labels := s.metricLabels()
			labels["status"] = "ok"

			if err := s.expireData(rpByBucketID, now); err != nil {
				log.Error("Deletion not successful", zap.Error(err))
				labels["status"] = "error"
			}

			if s.retentionMetrics == nil {
				continue
			}
			s.retentionMetrics.CheckDuration.With(labels).Observe(time.Since(now).Seconds())
			s.retentionMetrics.Checks.With(labels).Inc()
		case <-closingCh:
			return
		}
	}
}

// expireData runs a delete operation on the storage engine.
//
// Any series data that (1) belongs to a bucket in the provided map and
// (2) falls outside the bucket's indicated retention period will be deleted.
func (s *retentionService) expireData(rpByBucketID map[string]time.Duration, now time.Time) error {
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
	badMSketch := make(map[string]struct{})     // Badly formatted measurements.
	missingBSketch := make(map[string]struct{}) // Missing buckets.

	var seriesDeleted uint64 // Number of series where a delete is attempted.
	var seriesSkipped uint64 // Number of series that were skipped from delete.

	fn := func(name []byte, tags models.Tags) (int64, int64, bool) {
		_, bucketID, err := platform.ReadMeasurement(name)
		if err != nil {
			mu.Lock()
			badMSketch[string(name)] = struct{}{}
			mu.Unlock()
			atomic.AddUint64(&seriesSkipped, 1)
			return 0, 0, false
		}

		retentionPeriod, ok := rpByBucketID[string(bucketID)]
		if !ok {
			mu.Lock()
			missingBSketch[string(bucketID)] = struct{}{}
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
		if s.retentionMetrics == nil {
			return
		}
		labels := s.metricLabels()

		labels["status"] = "bad_measurement"
		s.retentionMetrics.Unprocessable.With(labels).Add(float64(len(badMSketch)))

		labels["status"] = "missing_bucket"
		s.retentionMetrics.Unprocessable.With(labels).Add(float64(len(missingBSketch)))

		labels["status"] = "ok"
		s.retentionMetrics.Series.With(labels).Add(float64(atomic.LoadUint64(&seriesDeleted)))

		labels["status"] = "skipped"
		s.retentionMetrics.Series.With(labels).Add(float64(atomic.LoadUint64(&seriesSkipped)))
	}()

	return s.Engine.DeleteSeriesRangeWithPredicate(newSeriesIteratorAdapter(cur), fn)
}

// getRetentionPeriodPerBucket returns a map of (bucket ID -> retention period)
// for all buckets.
func (s *retentionService) getRetentionPeriodPerBucket() (map[string]time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), bucketAPITimeout)
	defer cancel()
	buckets, _, err := s.BucketService.FindBuckets(ctx, platform.BucketFilter{})
	if err != nil {
		return nil, err
	}
	rpByBucketID := make(map[string]time.Duration, len(buckets))
	for _, bucket := range buckets {
		rpByBucketID[string(bucket.ID)] = bucket.RetentionPeriod
	}
	return rpByBucketID, nil
}

// closing returns a channel to signal that the service is closing.
func (s *retentionService) closing() chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s._closing
}

// Close closes the service.
//
// If a delete of data is in-progress, then it will be allowed to complete before
// Close returns. Re-closing the service once it's closed is a no-op.
func (s *retentionService) Close() error {
	if s.closing() == nil {
		return nil // Already closed.
	}

	now := time.Now()
	s.logger.Info("Service closing")
	close(s.closing())
	s.wg.Wait()

	s.logger.Info("Service closed", zap.Duration("took", time.Since(now)))
	s.mu.Lock()
	s._closing = nil
	s.mu.Unlock()
	return nil
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (s *retentionService) PrometheusCollectors() []prometheus.Collector {
	if s.retentionMetrics != nil {
		return s.retentionMetrics.PrometheusCollectors()
	}
	return nil
}

// A BucketService is an platform.BucketService that the RetentionService can open,
// close and log.
type BucketService interface {
	platform.BucketService
	Open() error
	Close() error
	WithLogger(l *zap.Logger)
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
