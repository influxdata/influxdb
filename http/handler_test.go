package http

import (
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func TestHandler_ServeHTTP(t *testing.T) {
	type fields struct {
		name           string
		MetricsHandler http.Handler
		ReadyHandler   http.Handler
		HealthHandler  http.Handler
		DebugHandler   http.Handler
		Handler        http.Handler
		requests       *prometheus.CounterVec
		requestDur     *prometheus.HistogramVec
		Logger         *zap.Logger
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
			name: "howdy",
			fields: fields{
				name:    "doody",
				Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
				Logger:  zap.NewNop(),
			},
			args: args{
				r: httptest.NewRequest(http.MethodGet, "/", nil),
				w: httptest.NewRecorder(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handler{
				name:    tt.fields.name,
				Handler: tt.fields.Handler,
				Logger:  tt.fields.Logger,
			}
			h.initMetrics()
			h.ServeHTTP(tt.args.w, tt.args.r)
		})
	}
}
