package http

import (
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestHandler_ServeHTTP(t *testing.T) {
	type fields struct {
		name    string
		handler http.Handler
		log     *zap.Logger
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "should record metrics when http handling",
			fields: fields{
				name:    "test",
				handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
				log:     zaptest.NewLogger(t),
			},
			args: args{
				r: httptest.NewRequest(http.MethodGet, "/", nil),
				w: httptest.NewRecorder(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prom.NewRegistry(zaptest.NewLogger(t))
			h := NewHandlerFromRegistry(
				tt.fields.name,
				reg,
				WithLog(tt.fields.log),
				WithAPIHandler(tt.fields.handler),
			)

			tt.args.r.Header.Set("User-Agent", "ua1")
			h.ServeHTTP(tt.args.w, tt.args.r)

			mfs, err := reg.Gather()
			if err != nil {
				t.Fatal(err)
			}

			c := promtest.MustFindMetric(t, mfs, "http_api_requests_total", map[string]string{
				"handler":       "test",
				"method":        "GET",
				"path":          "/",
				"status":        "2XX",
				"user_agent":    "ua1",
				"response_code": "200",
			})
			if got := c.GetCounter().GetValue(); got != 1 {
				t.Fatalf("expected counter to be 1, got %v", got)
			}

			g := promtest.MustFindMetric(t, mfs, "http_api_request_duration_seconds", map[string]string{
				"handler":       "test",
				"method":        "GET",
				"path":          "/",
				"status":        "2XX",
				"user_agent":    "ua1",
				"response_code": "200",
			})
			if got := g.GetHistogram().GetSampleCount(); got != 1 {
				t.Fatalf("expected histogram sample count to be 1, got %v", got)
			}
		})

	}
}
