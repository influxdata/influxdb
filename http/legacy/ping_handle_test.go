package legacy

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPingHandler(t *testing.T) {
	type wants struct {
		statusCode int
		version    string
		build      string
	}
	tests := []struct {
		name  string
		w     *httptest.ResponseRecorder
		r     *http.Request
		wants wants
	}{
		{
			name: "GET request",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest(http.MethodGet, "/ping", nil),
			wants: wants{
				statusCode: http.StatusNoContent,
				version:    "2.0.0",
				build:      "oss",
			},
		},
		{
			name: "HEAD request",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest(http.MethodHead, "/ping", nil),
			wants: wants{
				statusCode: http.StatusNoContent,
				version:    "2.0.0",
				build:      "oss",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			NewPingHandler("2.0.0").pingHandler(tt.w, tt.r)
			res := tt.w.Result()
			build := res.Header.Get("X-Influxdb-Build")
			version := res.Header.Get("X-Influxdb-Version")

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. PingHandler() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if build != tt.wants.build {
				t.Errorf("%q. PingHandler() = %v, want %v", tt.name, build, tt.wants.build)
			}
			if version != tt.wants.version {
				t.Errorf("%q. PingHandler() = %v, want %v", tt.name, version, tt.wants.version)
			}
		})
	}
}
