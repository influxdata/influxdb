package remotes

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/prometheus/client_golang/prometheus"
)

func NewMetricCollectingService(reg prometheus.Registerer, underlying influxdb.RemoteConnectionService, opts ...metric.ClientOptFn) *metricsService {
	o := metric.ApplyMetricOpts(opts...)
	return &metricsService{
		rec:        metric.New(reg, o.ApplySuffix("remote")),
		underlying: underlying,
	}
}

type metricsService struct {
	// RED metrics
	rec        *metric.REDClient
	underlying influxdb.RemoteConnectionService
}

var _ influxdb.RemoteConnectionService = (*metricsService)(nil)

func (m metricsService) ListRemoteConnections(ctx context.Context, filter influxdb.RemoteConnectionListFilter) (*influxdb.RemoteConnections, error) {
	rec := m.rec.Record("find_remotes")
	rcs, err := m.underlying.ListRemoteConnections(ctx, filter)
	return rcs, rec(err)
}

func (m metricsService) CreateRemoteConnection(ctx context.Context, request influxdb.CreateRemoteConnectionRequest) (*influxdb.RemoteConnection, error) {
	rec := m.rec.Record("create_remote")
	rc, err := m.underlying.CreateRemoteConnection(ctx, request)
	return rc, rec(err)
}

func (m metricsService) ValidateNewRemoteConnection(ctx context.Context, request influxdb.CreateRemoteConnectionRequest) error {
	rec := m.rec.Record("validate_create_remote")
	return rec(m.underlying.ValidateNewRemoteConnection(ctx, request))
}

func (m metricsService) GetRemoteConnection(ctx context.Context, id platform.ID) (*influxdb.RemoteConnection, error) {
	rec := m.rec.Record("find_remote_by_id")
	rc, err := m.underlying.GetRemoteConnection(ctx, id)
	return rc, rec(err)
}

func (m metricsService) UpdateRemoteConnection(ctx context.Context, id platform.ID, request influxdb.UpdateRemoteConnectionRequest) (*influxdb.RemoteConnection, error) {
	rec := m.rec.Record("update_remote")
	rc, err := m.underlying.UpdateRemoteConnection(ctx, id, request)
	return rc, rec(err)
}

func (m metricsService) ValidateUpdatedRemoteConnection(ctx context.Context, id platform.ID, request influxdb.UpdateRemoteConnectionRequest) error {
	rec := m.rec.Record("validate_update_remote")
	return rec(m.underlying.ValidateUpdatedRemoteConnection(ctx, id, request))
}

func (m metricsService) DeleteRemoteConnection(ctx context.Context, id platform.ID) error {
	rec := m.rec.Record("delete_remote")
	return rec(m.underlying.DeleteRemoteConnection(ctx, id))
}

func (m metricsService) ValidateRemoteConnection(ctx context.Context, id platform.ID) error {
	rec := m.rec.Record("validate_remote")
	return rec(m.underlying.ValidateRemoteConnection(ctx, id))
}
