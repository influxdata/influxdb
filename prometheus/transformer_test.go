package prometheus_test

import (
	"reflect"
	"testing"

	pr "github.com/influxdata/influxdb/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestAddLabels_Transform(t *testing.T) {
	type fields struct {
		Labels map[string]string
	}
	type args struct {
		mfs []*dto.MetricFamily
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []*dto.MetricFamily
	}{
		{
			name: "add label from metric replaces label",
			fields: fields{
				Labels: map[string]string{
					"handler": "influxdb",
				},
			},
			args: args{
				mfs: []*dto.MetricFamily{
					NewCounter("http_api_requests_total", 10,
						pr.L("handler", "platform"),
						pr.L("method", "GET"),
						pr.L("path", "/api/v2"),
						pr.L("status", "2XX"),
					),
				},
			},
			want: []*dto.MetricFamily{
				NewCounter("http_api_requests_total", 10,
					pr.L("handler", "influxdb"),
					pr.L("method", "GET"),
					pr.L("path", "/api/v2"),
					pr.L("status", "2XX"),
				),
			},
		},
		{
			name: "add label from metric replaces label",
			fields: fields{
				Labels: map[string]string{
					"org": "myorg",
				},
			},
			args: args{
				mfs: []*dto.MetricFamily{
					NewCounter("http_api_requests_total", 10,
						pr.L("handler", "platform"),
						pr.L("method", "GET"),
						pr.L("path", "/api/v2"),
						pr.L("status", "2XX"),
					),
				},
			},
			want: []*dto.MetricFamily{
				NewCounter("http_api_requests_total", 10,
					pr.L("handler", "platform"),
					pr.L("method", "GET"),
					pr.L("org", "myorg"),
					pr.L("path", "/api/v2"),
					pr.L("status", "2XX"),
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &pr.AddLabels{
				Labels: tt.fields.Labels,
			}
			if got := a.Transform(tt.args.mfs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AddLabels.Transform() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRemoveLabels_Transform(t *testing.T) {
	type fields struct {
		Labels map[string]struct{}
	}
	type args struct {
		mfs []*dto.MetricFamily
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []*dto.MetricFamily
	}{
		{
			name: "remove label from metric",
			fields: fields{
				Labels: map[string]struct{}{
					"handler": struct{}{},
				},
			},
			args: args{
				mfs: []*dto.MetricFamily{
					NewCounter("http_api_requests_total", 10,
						pr.L("handler", "platform"),
						pr.L("method", "GET"),
						pr.L("path", "/api/v2"),
						pr.L("status", "2XX"),
					),
				},
			},
			want: []*dto.MetricFamily{
				NewCounter("http_api_requests_total", 10,
					pr.L("method", "GET"),
					pr.L("path", "/api/v2"),
					pr.L("status", "2XX"),
				),
			},
		},
		{
			name: "no match removes no labels",
			fields: fields{
				Labels: map[string]struct{}{
					"handler": struct{}{},
				},
			},
			args: args{
				mfs: []*dto.MetricFamily{
					NewCounter("http_api_requests_total", 10,
						pr.L("method", "GET"),
						pr.L("path", "/api/v2"),
						pr.L("status", "2XX"),
					),
				},
			},
			want: []*dto.MetricFamily{
				NewCounter("http_api_requests_total", 10,
					pr.L("method", "GET"),
					pr.L("path", "/api/v2"),
					pr.L("status", "2XX"),
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &pr.RemoveLabels{
				Labels: tt.fields.Labels,
			}
			if got := r.Transform(tt.args.mfs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RemoveLabels.Transform() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRenameFamilies_Transform(t *testing.T) {
	type fields struct {
		FromTo map[string]string
	}
	type args struct {
		mfs []*dto.MetricFamily
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []*dto.MetricFamily
	}{
		{
			name: "rename metric family in sort order",
			fields: fields{
				FromTo: map[string]string{
					"http_api_requests_total": "api_requests_total",
				},
			},
			args: args{
				mfs: []*dto.MetricFamily{
					NewCounter("handler", 10,
						pr.L("handler", "platform"),
					),
					NewCounter("http_api_requests_total", 10,
						pr.L("handler", "platform"),
						pr.L("method", "GET"),
						pr.L("path", "/api/v2"),
						pr.L("status", "2XX"),
					),
				},
			},
			want: []*dto.MetricFamily{
				NewCounter("api_requests_total", 10,
					pr.L("handler", "platform"),
					pr.L("method", "GET"),
					pr.L("path", "/api/v2"),
					pr.L("status", "2XX"),
				),
				NewCounter("handler", 10,
					pr.L("handler", "platform"),
				),
			},
		},
		{
			name: "ignored if not found",
			fields: fields{
				FromTo: map[string]string{
					"http_api_requests_total": "api_requests_total",
				},
			},
			args: args{
				mfs: []*dto.MetricFamily{
					NewCounter("handler", 10,
						pr.L("handler", "platform"),
					),
				},
			},
			want: []*dto.MetricFamily{
				NewCounter("handler", 10,
					pr.L("handler", "platform"),
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &pr.RenameFamilies{
				FromTo: tt.fields.FromTo,
			}
			if got := r.Transform(tt.args.mfs); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RenameFamilies.Transform() = %v, want %v", got, tt.want)
			}
		})
	}
}
