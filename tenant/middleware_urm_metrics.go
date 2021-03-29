package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/prometheus/client_golang/prometheus"
)

type UrmMetrics struct {
	// RED metrics
	rec *metric.REDClient

	urmService influxdb.UserResourceMappingService
}

var _ influxdb.UserResourceMappingService = (*UrmMetrics)(nil)

// NewUrmMetrics returns a metrics service middleware for the User Resource Mapping Service.
func NewUrmMetrics(reg prometheus.Registerer, s influxdb.UserResourceMappingService, opts ...metric.ClientOptFn) *UrmMetrics {
	o := metric.ApplyMetricOpts(opts...)
	return &UrmMetrics{
		rec:        metric.New(reg, o.ApplySuffix("urm")),
		urmService: s,
	}
}

func (m *UrmMetrics) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	rec := m.rec.Record("find_urms")
	urms, n, err := m.urmService.FindUserResourceMappings(ctx, filter, opt...)
	return urms, n, rec(err)
}

func (m *UrmMetrics) CreateUserResourceMapping(ctx context.Context, urm *influxdb.UserResourceMapping) error {
	rec := m.rec.Record("create_urm")
	err := m.urmService.CreateUserResourceMapping(ctx, urm)
	return rec(err)
}

func (m *UrmMetrics) DeleteUserResourceMapping(ctx context.Context, resourceID, userID platform.ID) error {
	rec := m.rec.Record("delete_urm")
	err := m.urmService.DeleteUserResourceMapping(ctx, resourceID, userID)
	return rec(err)
}
