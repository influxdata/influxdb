package onboarding

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/prometheus/client_golang/prometheus"
)

// NewMetricCollectingService returns a metrics service middleware for the User Service.
func NewMetricCollectingService(reg prometheus.Registerer, s influxdb.OnboardingService, opts ...metric.ClientOptFn) *metricsService {
	o := metric.ApplyMetricOpts(opts...)
	return &metricsService{
		rec:        metric.New(reg, o.ApplySuffix("onboard")),
		underlying: s,
	}
}

type metricsService struct {
	rec        *metric.REDClient
	underlying influxdb.OnboardingService
}

var _ influxdb.OnboardingService = (*metricsService)(nil)

func (m *metricsService) IsOnboarding(ctx context.Context) (bool, error) {
	rec := m.rec.Record("is_onboarding")
	available, err := m.underlying.IsOnboarding(ctx)
	return available, rec(err)
}

func (m *metricsService) OnboardInitialUser(ctx context.Context, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	rec := m.rec.Record("onboard_initial_user")
	res, err := m.underlying.OnboardInitialUser(ctx, req)
	return res, rec(err)
}
