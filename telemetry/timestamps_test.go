package telemetry

import (
	"reflect"
	"testing"
	"time"

	pr "github.com/influxdata/influxdb/v2/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

func goodMetricWithTime() *dto.MetricFamily {
	return &dto.MetricFamily{
		Name: proto.String("good"),
		Type: dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{
			{
				Label: []*dto.LabelPair{pr.L("n1", "v1")},
				Counter: &dto.Counter{
					Value: proto.Float64(1.0),
				},
				TimestampMs: proto.Int64(1),
			},
		},
	}
}

func TestAddTimestamps(t *testing.T) {
	type args struct {
		mfs []*dto.MetricFamily
		now func() time.Time
	}
	tests := []struct {
		name string
		args args
	}{
		{
			args: args{
				mfs: []*dto.MetricFamily{goodMetric()},
				now: func() time.Time { return time.Unix(0, int64(time.Millisecond)) },
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := AddTimestamps{
				now: tt.args.now,
			}
			got := ts.Transform(tt.args.mfs)
			want := []*dto.MetricFamily{goodMetricWithTime()}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("AddTimestamps.Transform() = %v, want %v", got, want)
			}
		})
	}
}
