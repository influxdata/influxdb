package authorization

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

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

func NewAuthMetrics(reg prometheus.Registerer, s influxdb.AuthorizationService, opts ...metric.ClientOptFn) *AuthMetrics {
	o := metric.ApplyMetricOpts(opts...)
	return &AuthMetrics{
		rec:         metric.New(reg, o.ApplySuffix("token")),
		authService: s,
	}
}

func (m *AuthMetrics) CreateAuthorization(ctx context.Context, a *influxdb.Authorization) error {
	rec := m.rec.Record("create_authorization")
	err := m.authService.CreateAuthorization(ctx, a)
	return rec(err)
}

func (m *AuthMetrics) FindAuthorizationByID(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
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

func (m *AuthMetrics) UpdateAuthorization(ctx context.Context, id platform.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
	rec := m.rec.Record("update_authorization")
	a, err := m.authService.UpdateAuthorization(ctx, id, upd)
	return a, rec(err)
}

func (m *AuthMetrics) DeleteAuthorization(ctx context.Context, id platform.ID) error {
	rec := m.rec.Record("delete_authorization")
	err := m.authService.DeleteAuthorization(ctx, id)
	return rec(err)
}
