package annotations

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/prometheus/client_golang/prometheus"
)

func NewMetricCollectingService(reg prometheus.Registerer, underlying influxdb.AnnotationService, opts ...metric.ClientOptFn) *metricsService {
	o := metric.ApplyMetricOpts(opts...)
	return &metricsService{
		rec:        metric.New(reg, o.ApplySuffix("annotation")),
		underlying: underlying,
	}
}

type metricsService struct {
	// RED metrics
	rec        *metric.REDClient
	underlying influxdb.AnnotationService
}

var _ influxdb.AnnotationService = (*metricsService)(nil)

func (m metricsService) CreateAnnotations(ctx context.Context, orgID platform.ID, create []influxdb.AnnotationCreate) ([]influxdb.AnnotationEvent, error) {
	rec := m.rec.Record("create_annotation")
	ans, err := m.underlying.CreateAnnotations(ctx, orgID, create)
	return ans, rec(err)
}

func (m metricsService) ListAnnotations(ctx context.Context, orgID platform.ID, filter influxdb.AnnotationListFilter) ([]influxdb.StoredAnnotation, error) {
	rec := m.rec.Record("find_annotations")
	ans, err := m.underlying.ListAnnotations(ctx, orgID, filter)
	return ans, rec(err)
}

func (m metricsService) GetAnnotation(ctx context.Context, id platform.ID) (*influxdb.StoredAnnotation, error) {
	rec := m.rec.Record("find_annotation_by_id")
	an, err := m.underlying.GetAnnotation(ctx, id)
	return an, rec(err)
}

func (m metricsService) DeleteAnnotations(ctx context.Context, orgID platform.ID, delete influxdb.AnnotationDeleteFilter) error {
	rec := m.rec.Record("delete_annotations")
	return rec(m.underlying.DeleteAnnotations(ctx, orgID, delete))
}

func (m metricsService) DeleteAnnotation(ctx context.Context, id platform.ID) error {
	rec := m.rec.Record("delete_annotation")
	return rec(m.underlying.DeleteAnnotation(ctx, id))
}

func (m metricsService) UpdateAnnotation(ctx context.Context, id platform.ID, update influxdb.AnnotationCreate) (*influxdb.AnnotationEvent, error) {
	rec := m.rec.Record("update_annotation")
	an, err := m.underlying.UpdateAnnotation(ctx, id, update)
	return an, rec(err)
}

func (m metricsService) ListStreams(ctx context.Context, orgID platform.ID, filter influxdb.StreamListFilter) ([]influxdb.StoredStream, error) {
	rec := m.rec.Record("find_streams")
	stms, err := m.underlying.ListStreams(ctx, orgID, filter)
	return stms, rec(err)
}

func (m metricsService) CreateOrUpdateStream(ctx context.Context, orgID platform.ID, stream influxdb.Stream) (*influxdb.ReadStream, error) {
	rec := m.rec.Record("create_or_update_stream")
	stm, err := m.underlying.CreateOrUpdateStream(ctx, orgID, stream)
	return stm, rec(err)
}

func (m metricsService) GetStream(ctx context.Context, id platform.ID) (*influxdb.StoredStream, error) {
	rec := m.rec.Record("find_stream_by_id")
	stm, err := m.underlying.GetStream(ctx, id)
	return stm, rec(err)
}

func (m metricsService) UpdateStream(ctx context.Context, id platform.ID, stream influxdb.Stream) (*influxdb.ReadStream, error) {
	rec := m.rec.Record("update_stream")
	stm, err := m.underlying.UpdateStream(ctx, id, stream)
	return stm, rec(err)
}

func (m metricsService) DeleteStreams(ctx context.Context, orgID platform.ID, delete influxdb.BasicStream) error {
	rec := m.rec.Record("delete_streams")
	return rec(m.underlying.DeleteStreams(ctx, orgID, delete))
}

func (m metricsService) DeleteStreamByID(ctx context.Context, id platform.ID) error {
	rec := m.rec.Record("delete_stream")
	return rec(m.underlying.DeleteStreamByID(ctx, id))
}
