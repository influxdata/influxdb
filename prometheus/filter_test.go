package prometheus

import (
	"fmt"
	"reflect"
	"testing"

	proto "github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestFilter_Gather(t *testing.T) {
	type fields struct {
		Gatherer prometheus.Gatherer
		Matcher  Matcher
	}
	tests := []struct {
		name    string
		fields  fields
		want    []*dto.MetricFamily
		wantErr bool
	}{
		{
			name: "no metrics returns nil",
			fields: fields{
				Gatherer: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
					return nil, nil
				}),
				Matcher: NewMatcher().
					Family("http_api_requests_total",
						L("handler", "platform"),
						L("method", "GET"),
						L("path", "/api/v2"),
						L("status", "2XX"),
					),
			},
		},
		{
			name: "gather error returns error",
			fields: fields{
				Gatherer: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
					return nil, fmt.Errorf("e1")
				}),
			},
			wantErr: true,
		},
		{
			name: "no matches returns no metric families",
			fields: fields{
				Gatherer: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
					mf := &dto.MetricFamily{
						Name: proto.String("n1"),
						Help: proto.String("h1"),
					}
					return []*dto.MetricFamily{mf}, nil
				}),
				Matcher: NewMatcher().
					Family("http_api_requests_total",
						L("handler", "platform"),
						L("method", "GET"),
						L("path", "/api/v2"),
						L("status", "2XX"),
					),
			},
			want: []*dto.MetricFamily{},
		},
		{
			name: "matching family without metric matches nothing",
			fields: fields{
				Gatherer: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
					mf := &dto.MetricFamily{
						Name: proto.String("go_memstats_frees_total"),
					}
					return []*dto.MetricFamily{mf}, nil
				}),
				Matcher: NewMatcher().
					Family("go_memstats_frees_total"),
			},
			want: []*dto.MetricFamily{},
		},
		{
			name: "matching family with no labels matches",
			fields: fields{
				Gatherer: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
					return []*dto.MetricFamily{NewCounter("go_memstats_frees_total", 1.0)}, nil
				}),
				Matcher: NewMatcher().
					Family("go_memstats_frees_total"),
			},
			want: []*dto.MetricFamily{NewCounter("go_memstats_frees_total", 1.0)},
		},
		{
			name: "matching with labels a family with labels matches",
			fields: fields{
				Gatherer: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
					return []*dto.MetricFamily{NewCounter("go_memstats_frees_total", 1.0, L("n1", "v1"))}, nil
				}),
				Matcher: NewMatcher().
					Family("go_memstats_frees_total", L("n1", "v1")),
			},
			want: []*dto.MetricFamily{NewCounter("go_memstats_frees_total", 1.0, L("n1", "v1"))},
		},
		{
			name: "matching a family that has no labels with labels matches",
			fields: fields{
				Gatherer: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
					return []*dto.MetricFamily{NewCounter("go_memstats_frees_total", 1.0, L("n1", "v1"))}, nil
				}),
				Matcher: NewMatcher().
					Family("go_memstats_frees_total"),
			},
			want: []*dto.MetricFamily{NewCounter("go_memstats_frees_total", 1.0, L("n1", "v1"))},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Filter{
				Gatherer: tt.fields.Gatherer,
				Matcher:  tt.fields.Matcher,
			}
			got, err := f.Gather()
			if (err != nil) != tt.wantErr {
				t.Errorf("Filter.Gather() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Filter.Gather() = %v, want %v", got, tt.want)
			}
		})
	}
}

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
