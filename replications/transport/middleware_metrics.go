package transport

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/prometheus/client_golang/prometheus"
)

func newMetricCollectingService(reg prometheus.Registerer, underlying ReplicationService, opts ...metric.ClientOptFn) *metricsService {
	o := metric.ApplyMetricOpts(opts...)
	return &metricsService{
		rec:        metric.New(reg, o.ApplySuffix("replication")),
		underlying: underlying,
	}
}

type metricsService struct {
	rec        *metric.REDClient
	underlying ReplicationService
}

var _ ReplicationService = (*metricsService)(nil)

func (m metricsService) ListReplications(ctx context.Context, filter influxdb.ReplicationListFilter) (*influxdb.Replications, error) {
	rec := m.rec.Record("find_replications")
	rcs, err := m.underlying.ListReplications(ctx, filter)
	return rcs, rec(err)
}

func (m metricsService) CreateReplication(ctx context.Context, request influxdb.CreateReplicationRequest) (*influxdb.Replication, error) {
	rec := m.rec.Record("create_replication")
	r, err := m.underlying.CreateReplication(ctx, request)
	return r, rec(err)
}

func (m metricsService) ValidateNewReplication(ctx context.Context, request influxdb.CreateReplicationRequest) error {
	rec := m.rec.Record("validate_create_replication")
	return rec(m.underlying.ValidateNewReplication(ctx, request))
}

func (m metricsService) GetReplication(ctx context.Context, id platform.ID) (*influxdb.Replication, error) {
	rec := m.rec.Record("find_replication_by_id")
	r, err := m.underlying.GetReplication(ctx, id)
	return r, rec(err)
}

func (m metricsService) UpdateReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) (*influxdb.Replication, error) {
	rec := m.rec.Record("update_replication")
	r, err := m.underlying.UpdateReplication(ctx, id, request)
	return r, rec(err)
}

func (m metricsService) ValidateUpdatedReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) error {
	rec := m.rec.Record("validate_update_replication")
	return rec(m.underlying.ValidateUpdatedReplication(ctx, id, request))
}

func (m metricsService) DeleteReplication(ctx context.Context, id platform.ID) error {
	rec := m.rec.Record("delete_replication")
	return rec(m.underlying.DeleteReplication(ctx, id))
}

func (m metricsService) ValidateReplication(ctx context.Context, id platform.ID) error {
	rec := m.rec.Record("validate_replication")
	return rec(m.underlying.ValidateReplication(ctx, id))
}
