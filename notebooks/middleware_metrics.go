package notebooks

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/prometheus/client_golang/prometheus"
)

func NewMetricCollectingService(reg prometheus.Registerer, underlying influxdb.NotebookService, opts ...metric.ClientOptFn) *metricsService {
	o := metric.ApplyMetricOpts(opts...)
	return &metricsService{
		rec:        metric.New(reg, o.ApplySuffix("notebook")),
		underlying: underlying,
	}
}

type metricsService struct {
	// RED metrics
	rec        *metric.REDClient
	underlying influxdb.NotebookService
}

var _ influxdb.NotebookService = (*metricsService)(nil)

func (m metricsService) GetNotebook(ctx context.Context, id platform.ID) (*influxdb.Notebook, error) {
	rec := m.rec.Record("find_notebook_by_id")
	nb, err := m.underlying.GetNotebook(ctx, id)
	return nb, rec(err)
}

func (m metricsService) CreateNotebook(ctx context.Context, create *influxdb.NotebookReqBody) (*influxdb.Notebook, error) {
	rec := m.rec.Record("create_notebook")
	nb, err := m.underlying.CreateNotebook(ctx, create)
	return nb, rec(err)
}

func (m metricsService) UpdateNotebook(ctx context.Context, id platform.ID, update *influxdb.NotebookReqBody) (*influxdb.Notebook, error) {
	rec := m.rec.Record("update_notebook")
	nb, err := m.underlying.UpdateNotebook(ctx, id, update)
	return nb, rec(err)
}

func (m metricsService) DeleteNotebook(ctx context.Context, id platform.ID) (err error) {
	rec := m.rec.Record("delete_notebook")
	return rec(m.underlying.DeleteNotebook(ctx, id))
}

func (m metricsService) ListNotebooks(ctx context.Context, filter influxdb.NotebookListFilter) ([]*influxdb.Notebook, error) {
	rec := m.rec.Record("find_notebooks")
	nbs, err := m.underlying.ListNotebooks(ctx, filter)
	return nbs, rec(err)
}
