package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestHandler_ServeHTTP(t *testing.T) {
	type fields struct {
		name          string
		handler       http.Handler
		handlerHidden bool
		log           *zap.Logger
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "should record metrics when http handling",
			fields: fields{
				name:    "test",
				handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
				log:     zaptest.NewLogger(t),
			},
		},
		{
			name: "should record metrics even when not exposed over HTTP",
			fields: fields{
				name:          "test",
				handler:       http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
				handlerHidden: true,
				log:           zaptest.NewLogger(t),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prom.NewRegistry(zaptest.NewLogger(t))
			h := NewRootHandler(
				tt.fields.name,
				WithLog(tt.fields.log),
				WithAPIHandler(tt.fields.handler),
				WithMetrics(reg, !tt.fields.handlerHidden),
			)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("User-Agent", "ua1")
			h.ServeHTTP(httptest.NewRecorder(), req)

			mfs, err := reg.Gather()
			require.NoError(t, err)

			c := promtest.MustFindMetric(t, mfs, "http_api_requests_total", map[string]string{
				"handler":       "test",
				"method":        "GET",
				"path":          "/",
				"status":        "2XX",
				"user_agent":    "ua1",
				"response_code": "200",
			})
			require.Equal(t, 1, int(c.GetCounter().GetValue()))

			g := promtest.MustFindMetric(t, mfs, "http_api_request_duration_seconds", map[string]string{
				"handler":       "test",
				"method":        "GET",
				"path":          "/",
				"status":        "2XX",
				"user_agent":    "ua1",
				"response_code": "200",
			})
			require.Equal(t, 1, int(g.GetHistogram().GetSampleCount()))

			req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
			recorder := httptest.NewRecorder()
			h.ServeHTTP(recorder, req)

			if tt.fields.handlerHidden {
				require.Equal(t, http.StatusForbidden, recorder.Code)
			} else {
				require.Equal(t, http.StatusOK, recorder.Code)
			}
		})
	}
}
