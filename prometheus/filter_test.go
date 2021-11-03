package prometheus_test

import (
	"fmt"
	"reflect"
	"testing"

	pr "github.com/influxdata/influxdb/v2/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

func TestFilter_Gather(t *testing.T) {
	type fields struct {
		Gatherer prometheus.Gatherer
		Matcher  pr.Matcher
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
				Matcher: pr.NewMatcher().
					Family("http_api_requests_total",
						pr.L("handler", "platform"),
						pr.L("method", "GET"),
						pr.L("path", "/api/v2"),
						pr.L("status", "2XX"),
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
				Matcher: pr.NewMatcher().
					Family("http_api_requests_total",
						pr.L("handler", "platform"),
						pr.L("method", "GET"),
						pr.L("path", "/api/v2"),
						pr.L("status", "2XX"),
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
				Matcher: pr.NewMatcher().
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
				Matcher: pr.NewMatcher().
					Family("go_memstats_frees_total"),
			},
			want: []*dto.MetricFamily{NewCounter("go_memstats_frees_total", 1.0)},
		},
		{
			name: "matching with labels a family with labels matches",
			fields: fields{
				Gatherer: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
					return []*dto.MetricFamily{NewCounter("go_memstats_frees_total", 1.0, pr.L("n1", "v1"))}, nil
				}),
				Matcher: pr.NewMatcher().
					Family("go_memstats_frees_total", pr.L("n1", "v1")),
			},
			want: []*dto.MetricFamily{NewCounter("go_memstats_frees_total", 1.0, pr.L("n1", "v1"))},
		},
		{
			name: "matching a family that has no labels with labels matches",
			fields: fields{
				Gatherer: prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
					return []*dto.MetricFamily{NewCounter("go_memstats_frees_total", 1.0, pr.L("n1", "v1"))}, nil
				}),
				Matcher: pr.NewMatcher().
					Family("go_memstats_frees_total"),
			},
			want: []*dto.MetricFamily{NewCounter("go_memstats_frees_total", 1.0, pr.L("n1", "v1"))},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &pr.Filter{
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
