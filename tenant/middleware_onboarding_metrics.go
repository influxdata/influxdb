package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/prometheus/client_golang/prometheus"
)

var _ influxdb.OnboardingService = (*OnboardingMetrics)(nil)

type OnboardingMetrics struct {
	// RED metrics
	rec *metric.REDClient

	onboardingService influxdb.OnboardingService
}

// NewOnboardingMetrics returns a metrics service middleware for the User Service.
func NewOnboardingMetrics(reg prometheus.Registerer, s influxdb.OnboardingService, opts ...metric.ClientOptFn) *OnboardingMetrics {
	o := metric.ApplyMetricOpts(opts...)
	return &OnboardingMetrics{
		rec:               metric.New(reg, o.ApplySuffix("onboard")),
		onboardingService: s,
	}
}

func (m *OnboardingMetrics) IsOnboarding(ctx context.Context) (bool, error) {
	rec := m.rec.Record("is_onboarding")
	available, err := m.onboardingService.IsOnboarding(ctx)
	return available, rec(err)
}

func (m *OnboardingMetrics) OnboardInitialUser(ctx context.Context, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	rec := m.rec.Record("onboard_initial_user")
	res, err := m.onboardingService.OnboardInitialUser(ctx, req)
	return res, rec(err)
}
