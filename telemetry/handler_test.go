package telemetry

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	pr "github.com/influxdata/influxdb/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap/zaptest"
)

func TestPushGateway_Handler(t *testing.T) {
	type fields struct {
		Store *mockStore
		now   func() time.Time
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		contentType string
		wantStatus  int
		want        []byte
	}{

		{
			name: "unknown content-type is a bad request",
			fields: fields{
				Store: &mockStore{},
			},
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("POST", "/", nil),
			},
			wantStatus: http.StatusBadRequest,
		},

		{
			name: "bad metric with timestamp is a bad request",
			fields: fields{
				Store: &mockStore{},
				now:   func() time.Time { return time.Unix(0, 0) },
			},
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("POST", "/",
					mustEncode(t,
						[]*dto.MetricFamily{badMetric()},
					),
				),
			},
			contentType: string(expfmt.FmtProtoDelim),
			wantStatus:  http.StatusBadRequest,
		},
		{
			name: "store error is an internal server error",
			fields: fields{
				Store: &mockStore{
					err: fmt.Errorf("e1"),
				},
				now: func() time.Time { return time.Unix(0, 0) },
			},
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("POST", "/",
					mustEncode(t,
						[]*dto.MetricFamily{NewCounter("mf1", 1.0, pr.L("n1", "v1"))},
					),
				),
			},
			contentType: string(expfmt.FmtProtoDelim),
			wantStatus:  http.StatusInternalServerError,
			want:        []byte(`[{"name":"mf1","type":0,"metric":[{"label":[{"name":"n1","value":"v1"}],"counter":{"value":1},"timestamp_ms":0}]}]`),
		},
		{
			name: "metric store in store",
			fields: fields{
				Store: &mockStore{},
				now:   func() time.Time { return time.Unix(0, 0) },
			},
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("POST", "/",
					mustEncode(t,
						[]*dto.MetricFamily{NewCounter("mf1", 1.0, pr.L("n1", "v1"))},
					),
				),
			},
			contentType: string(expfmt.FmtProtoDelim),
			wantStatus:  http.StatusAccepted,
			want:        []byte(`[{"name":"mf1","type":0,"metric":[{"label":[{"name":"n1","value":"v1"}],"counter":{"value":1},"timestamp_ms":0}]}]`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPushGateway(
				zaptest.NewLogger(t),
				tt.fields.Store,
				&AddTimestamps{
					now: tt.fields.now,
				},
			)
			p.Encoder = &pr.JSON{}
			tt.args.r.Header.Set("Content-Type", tt.contentType)
			p.Handler(tt.args.w, tt.args.r)

			if tt.args.w.Code != http.StatusAccepted {
				t.Logf("Body: %s", tt.args.w.Body.String())
			}
			if got, want := tt.args.w.Code, tt.wantStatus; got != want {
				t.Errorf("PushGateway.Handler() StatusCode = %v, want %v", got, want)
			}

			if got, want := tt.fields.Store.data, tt.want; string(got) != string(want) {
				t.Errorf("PushGateway.Handler() Data = %s, want %s", got, want)
			}
		})
	}
}

func Test_decodePostMetricsRequest(t *testing.T) {
	type args struct {
		req      *http.Request
		maxBytes int64
	}
	tests := []struct {
		name        string
		args        args
		contentType string
		want        []*dto.MetricFamily
		wantErr     bool
	}{
		{
			name: "bad body returns no metrics",
			args: args{
				req:      httptest.NewRequest("POST", "/", bytes.NewBuffer([]byte{0x10})),
				maxBytes: 10,
			},
			contentType: string(expfmt.FmtProtoDelim),
			want:        []*dto.MetricFamily{},
		},
		{
			name: "no body returns no metrics",
			args: args{
				req:      httptest.NewRequest("POST", "/", nil),
				maxBytes: 10,
			},
			contentType: string(expfmt.FmtProtoDelim),
			want:        []*dto.MetricFamily{},
		},
		{
			name: "metrics are returned from POST",
			args: args{
				req: httptest.NewRequest("POST", "/",
					mustEncode(t,
						[]*dto.MetricFamily{NewCounter("mf1", 1.0, pr.L("n1", "v1"))},
					),
				),
				maxBytes: 31,
			},
			contentType: string(expfmt.FmtProtoDelim),
			want:        []*dto.MetricFamily{NewCounter("mf1", 1.0, pr.L("n1", "v1"))},
		},
		{
			name: "max bytes limits on record boundary returns a single record",
			args: args{
				req: httptest.NewRequest("POST", "/",
					mustEncode(t,
						[]*dto.MetricFamily{
							NewCounter("mf1", 1.0, pr.L("n1", "v1")),
							NewCounter("mf2", 1.0, pr.L("n2", "v2")),
						},
					),
				),
				maxBytes: 31,
			},
			contentType: string(expfmt.FmtProtoDelim),
			want:        []*dto.MetricFamily{NewCounter("mf1", 1.0, pr.L("n1", "v1"))},
		},
		{
			name: "exceeding max bytes returns an error",
			args: args{
				req: httptest.NewRequest("POST", "/",
					mustEncode(t,
						[]*dto.MetricFamily{
							NewCounter("mf1", 1.0, pr.L("n1", "v1")),
							NewCounter("mf2", 1.0, pr.L("n2", "v2")),
						},
					),
				),
				maxBytes: 33,
			},
			contentType: string(expfmt.FmtProtoDelim),
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.args.req.Header.Set("Content-Type", tt.contentType)
			got, err := decodePostMetricsRequest(tt.args.req.Body, expfmt.Format(tt.contentType), tt.args.maxBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodePostMetricsRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodePostMetricsRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func badMetric() *dto.MetricFamily {
	return &dto.MetricFamily{
		Name: proto.String("bad"),
		Type: dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{
			&dto.Metric{
				Label: []*dto.LabelPair{pr.L("n1", "v1")},
				Counter: &dto.Counter{
					Value: proto.Float64(1.0),
				},
				TimestampMs: proto.Int64(1),
			},
		},
	}
}

func goodMetric() *dto.MetricFamily {
	return &dto.MetricFamily{
		Name: proto.String("good"),
		Type: dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{
			&dto.Metric{
				Label: []*dto.LabelPair{pr.L("n1", "v1")},
				Counter: &dto.Counter{
					Value: proto.Float64(1.0),
				},
			},
		},
	}
}

func Test_valid(t *testing.T) {
	type args struct {
		mfs []*dto.MetricFamily
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "metric with timestamp is invalid",
			args: args{
				mfs: []*dto.MetricFamily{badMetric()},
			},
			wantErr: true,
		},
		{
			name: "metric without timestamp is valid",
			args: args{
				mfs: []*dto.MetricFamily{goodMetric()},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := valid(tt.args.mfs); (err != nil) != tt.wantErr {
				t.Errorf("valid() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

type mockStore struct {
	data []byte
	err  error
}

func (m *mockStore) WriteMessage(ctx context.Context, data []byte) error {
	m.data = data
	return m.err

}

func mustEncode(t *testing.T, mfs []*dto.MetricFamily) io.Reader {
	b, err := pr.EncodeExpfmt(mfs)
	if err != nil {
		t.Fatalf("unable to encode %v", err)
	}
	return bytes.NewBuffer(b)
}
