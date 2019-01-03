package http

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthHandler(t *testing.T) {
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}
	tests := []struct {
		name  string
		w     *httptest.ResponseRecorder
		r     *http.Request
		wants wants
	}{
		{
			name: "health endpoint returns pass",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest(http.MethodGet, "/health", nil),
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body:        `{"name":"influxdb", "message":"ready for queries and writes", "status":"pass", "checks":[]}`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			HealthHandler(tt.w, tt.r)
			res := tt.w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. HealthHandler() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. HealthHandler() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. HealthHandler() = ***%s***", tt.name, diff)
			}
		})
	}
}
