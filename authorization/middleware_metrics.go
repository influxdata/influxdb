package authorization

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/prometheus/client_golang/prometheus"
)

type AuthMetrics struct {
	// RED metrics
	rec *metric.REDClient

	authService influxdb.AuthorizationService
}

var _ influxdb.AuthorizationService = (*AuthMetrics)(nil)

func NewAuthMetrics(reg prometheus.Registerer, s influxdb.AuthorizationService, opts ...MetricsOption) *AuthMetrics {
	o := applyOpts(opts...)
	return &AuthMetrics{
		rec:         metric.New(reg, o.applySuffix("token")),
		authService: s,
	}
}

func (m *AuthMetrics) CreateAuthorization(ctx context.Context, a *influxdb.Authorization) error {
	rec := m.rec.Record("create_authorization")
	err := m.authService.CreateAuthorization(ctx, a)
	return rec(err)
}

func (m *AuthMetrics) FindAuthorizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Authorization, error) {
	rec := m.rec.Record("find_authorization_by_id")
	a, err := m.authService.FindAuthorizationByID(ctx, id)
	return a, rec(err)
}
func (m *AuthMetrics) FindAuthorizationByToken(ctx context.Context, t string) (*influxdb.Authorization, error) {
	rec := m.rec.Record("find_authorization_by_token")
	a, err := m.authService.FindAuthorizationByToken(ctx, t)
	return a, rec(err)
}
func (m *AuthMetrics) FindAuthorizations(ctx context.Context, filter influxdb.AuthorizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
	rec := m.rec.Record("find_authorization_by_token")
	a, n, err := m.authService.FindAuthorizations(ctx, filter, opt...)
	return a, n, rec(err)
}

func (m *AuthMetrics) UpdateAuthorization(ctx context.Context, id influxdb.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
	rec := m.rec.Record("update_authorization")
	a, err := m.authService.UpdateAuthorization(ctx, id, upd)
	return a, rec(err)
}

func (m *AuthMetrics) DeleteAuthorization(ctx context.Context, id influxdb.ID) error {
	rec := m.rec.Record("delete_authorization")
	err := m.authService.DeleteAuthorization(ctx, id)
	return rec(err)
}

// Metrics options
type metricOpts struct {
	serviceSuffix string
}

func defaultOpts() *metricOpts {
	return &metricOpts{}
}

func (o *metricOpts) applySuffix(prefix string) string {
	if o.serviceSuffix != "" {
		return fmt.Sprintf("%s_%s", prefix, o.serviceSuffix)
	}
	return prefix
}

// MetricsOption is an option used by a metric middleware.
type MetricsOption func(*metricOpts)

// WithSuffix returns a metric option that applies a suffix to the service name of the metric.
func WithSuffix(suffix string) MetricsOption {
	return func(opts *metricOpts) {
		opts.serviceSuffix = suffix
	}
}

func applyOpts(opts ...MetricsOption) *metricOpts {
	o := defaultOpts()
	for _, opt := range opts {
		opt(o)
	}
	return o
}
