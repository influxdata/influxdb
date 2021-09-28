package telemetry

import (
	"github.com/golang/protobuf/proto" //lint:ignore SA1019 this deprecated package will be removed by https://github.com/influxdata/influxdb/pull/22571
	dto "github.com/prometheus/client_model/go"
)

func NewCounter(name string, v float64, ls ...*dto.LabelPair) *dto.MetricFamily {
	m := &dto.Metric{
		Label: ls,
		Counter: &dto.Counter{
			Value: &v,
		},
	}
	return &dto.MetricFamily{
		Name:   proto.String(name),
		Type:   dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{m},
	}
}
