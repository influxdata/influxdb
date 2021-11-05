package prometheus_test

import (
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
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
