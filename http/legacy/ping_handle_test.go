package legacy

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPingHandler(t *testing.T) {
	tests := []struct {
		name string
		w    *httptest.ResponseRecorder
		r    *http.Request
	}{
		{
			name: "GET request",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest(http.MethodGet, "/ping", nil),
		},
		{
			name: "HEAD request",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest(http.MethodHead, "/ping", nil),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			NewPingHandler().pingHandler(tt.w, tt.r)
			res := tt.w.Result()
			build := res.Header.Get("X-Influxdb-Build")
			version := res.Header.Get("X-Influxdb-Version")

			if res.StatusCode != http.StatusNoContent {
				t.Errorf("%q. PingHandler() = %v, want %v", tt.name, res.StatusCode, http.StatusNoContent)
			}
			if build != "" {
				t.Errorf("%q. PingHandler() = %v, want empty string", tt.name, build)
			}
			if version != "" {
				t.Errorf("%q. PingHandler() = %v, want empty string", tt.name, version)
			}
		})
	}
}
