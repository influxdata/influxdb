package label

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/prometheus/client_golang/prometheus"
)

type LabelMetrics struct {
	// RED metrics
	rec *metric.REDClient

	labelService influxdb.LabelService
}

func NewLabelMetrics(reg prometheus.Registerer, s influxdb.LabelService, opts ...metric.ClientOptFn) *LabelMetrics {
	o := metric.ApplyMetricOpts(opts...)
	return &LabelMetrics{
		rec:          metric.New(reg, o.ApplySuffix("org")),
		labelService: s,
	}
}

var _ influxdb.LabelService = (*LabelMetrics)(nil)

func (m *LabelMetrics) CreateLabel(ctx context.Context, l *influxdb.Label) (err error) {
	rec := m.rec.Record("create_label")
	err = m.labelService.CreateLabel(ctx, l)
	return rec(err)
}

func (m *LabelMetrics) FindLabelByID(ctx context.Context, id platform.ID) (label *influxdb.Label, err error) {
	rec := m.rec.Record("find_label_by_id")
	l, err := m.labelService.FindLabelByID(ctx, id)
	return l, rec(err)
}

func (m *LabelMetrics) FindLabels(ctx context.Context, filter influxdb.LabelFilter, opt ...influxdb.FindOptions) (ls []*influxdb.Label, err error) {
	rec := m.rec.Record("find_labels")
	l, err := m.labelService.FindLabels(ctx, filter, opt...)
	return l, rec(err)
}

func (m *LabelMetrics) FindResourceLabels(ctx context.Context, filter influxdb.LabelMappingFilter) (ls []*influxdb.Label, err error) {
	rec := m.rec.Record("find_labels_for_resource")
	l, err := m.labelService.FindResourceLabels(ctx, filter)
	return l, rec(err)
}

func (m *LabelMetrics) UpdateLabel(ctx context.Context, id platform.ID, upd influxdb.LabelUpdate) (lbl *influxdb.Label, err error) {
	rec := m.rec.Record("update_label")
	l, err := m.labelService.UpdateLabel(ctx, id, upd)
	return l, rec(err)
}

func (m *LabelMetrics) DeleteLabel(ctx context.Context, id platform.ID) (err error) {
	rec := m.rec.Record("delete_label")
	err = m.labelService.DeleteLabel(ctx, id)
	return rec(err)
}

func (m *LabelMetrics) CreateLabelMapping(ctx context.Context, lm *influxdb.LabelMapping) (err error) {
	rec := m.rec.Record("create_label_mapping")
	err = m.labelService.CreateLabelMapping(ctx, lm)
	return rec(err)
}

func (m *LabelMetrics) DeleteLabelMapping(ctx context.Context, lm *influxdb.LabelMapping) (err error) {
	rec := m.rec.Record("delete_label_mapping")
	err = m.labelService.DeleteLabelMapping(ctx, lm)
	return rec(err)
}
