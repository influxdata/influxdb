package prometheus

import (
	"context"
	"fmt"
	"time"

	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/prometheus/client_golang/prometheus"
)

// AuthorizationService manages authorizations.
type AuthorizationService struct {
	requestCount         *prometheus.CounterVec
	requestDuration      *prometheus.HistogramVec
	AuthorizationService platform.AuthorizationService
}

// NewAuthorizationService creates an instance of AuthorizationService.
func NewAuthorizationService() *AuthorizationService {
	// TODO: what to make these values
	namespace := "auth"
	subsystem := "prometheus"
	s := &AuthorizationService{
		requestCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "requests_total",
			Help:      "Number of http requests received",
		}, []string{"method", "error"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "request_duration_seconds",
			Help:      "Time taken to respond to HTTP request",
			// TODO(desa): determine what spacing these buckets should have.
			Buckets: prometheus.ExponentialBuckets(0.001, 1.5, 25),
		}, []string{"method", "error"}),
	}

	return s
}

// FindAuthorizationByID returns an authorization given a id, records function call latency, and counts function calls.
func (s *AuthorizationService) FindAuthorizationByID(ctx context.Context, id platform2.ID) (a *platform.Authorization, err error) {
	defer func(start time.Time) {
		labels := prometheus.Labels{
			"method": "FindAuthorizationByID",
			"error":  fmt.Sprint(err != nil),
		}
		s.requestCount.With(labels).Add(1)
		s.requestDuration.With(labels).Observe(time.Since(start).Seconds())
	}(time.Now())
	return s.AuthorizationService.FindAuthorizationByID(ctx, id)
}

// FindAuthorizationByToken returns an authorization given a token, records function call latency, and counts function calls.
func (s *AuthorizationService) FindAuthorizationByToken(ctx context.Context, t string) (a *platform.Authorization, err error) {
	defer func(start time.Time) {
		labels := prometheus.Labels{
			"method": "FindAuthorizationByToken",
			"error":  fmt.Sprint(err != nil),
		}
		s.requestCount.With(labels).Add(1)
		s.requestDuration.With(labels).Observe(time.Since(start).Seconds())
	}(time.Now())
	return s.AuthorizationService.FindAuthorizationByToken(ctx, t)
}

// FindAuthorizations returns authorizations given a filter, records function call latency, and counts function calls.
func (s *AuthorizationService) FindAuthorizations(ctx context.Context, filter platform.AuthorizationFilter, opt ...platform.FindOptions) (as []*platform.Authorization, i int, err error) {
	defer func(start time.Time) {
		labels := prometheus.Labels{
			"method": "FindAuthorizations",
			"error":  fmt.Sprint(err != nil),
		}
		s.requestCount.With(labels).Add(1)
		s.requestDuration.With(labels).Observe(time.Since(start).Seconds())
	}(time.Now())

	return s.AuthorizationService.FindAuthorizations(ctx, filter, opt...)
}

// CreateAuthorization creates an authorization, records function call latency, and counts function calls.
func (s *AuthorizationService) CreateAuthorization(ctx context.Context, a *platform.Authorization) (err error) {
	defer func(start time.Time) {
		labels := prometheus.Labels{
			"method": "CreateAuthorization",
			"error":  fmt.Sprint(err != nil),
		}
		s.requestCount.With(labels).Add(1)
		s.requestDuration.With(labels).Observe(time.Since(start).Seconds())
	}(time.Now())

	return s.AuthorizationService.CreateAuthorization(ctx, a)
}

// DeleteAuthorization deletes an authorization, records function call latency, and counts function calls.
func (s *AuthorizationService) DeleteAuthorization(ctx context.Context, id platform2.ID) (err error) {
	defer func(start time.Time) {
		labels := prometheus.Labels{
			"method": "DeleteAuthorization",
			"error":  fmt.Sprint(err != nil),
		}
		s.requestCount.With(labels).Add(1)
		s.requestDuration.With(labels).Observe(time.Since(start).Seconds())
	}(time.Now())

	return s.AuthorizationService.DeleteAuthorization(ctx, id)
}

// UpdateAuthorization updates the status and description.
func (s *AuthorizationService) UpdateAuthorization(ctx context.Context, id platform2.ID, upd *platform.AuthorizationUpdate) (a *platform.Authorization, err error) {
	defer func(start time.Time) {
		labels := prometheus.Labels{
			"method": "setAuthorizationStatus",
			"error":  fmt.Sprint(err != nil),
		}
		s.requestCount.With(labels).Add(1)
		s.requestDuration.With(labels).Observe(time.Since(start).Seconds())
	}(time.Now())

	return s.AuthorizationService.UpdateAuthorization(ctx, id, upd)
}

// PrometheusCollectors returns all authorization service prometheus collectors.
func (s *AuthorizationService) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		s.requestCount,
		s.requestDuration,
	}
}
