package http

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthHandler(t *testing.T) {
	type wants struct {
		statusCode  int
		contentType string
		status      string
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
				status:      "pass",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			HealthHandler(tt.w, tt.r)
			res := tt.w.Result()
			contentType := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. HealthHandler() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && contentType != tt.wants.contentType {
				t.Errorf("%q. HealthHandler() = %v, want %v", tt.name, contentType, tt.wants.contentType)
			}
			var content map[string]interface{}
			if err := json.Unmarshal(body, &content); err != nil {
				t.Errorf("%q, HealthHandler(). error unmarshalling json %v", tt.name, err)
				return
			}
			if _, found := content["name"]; !found {
				t.Errorf("%q. HealthHandler() no name reported", tt.name)
			}
			if content["status"] != tt.wants.status {
				t.Errorf("%q. HealthHandler() status= %v, want %v", tt.name, content["status"], tt.wants.status)
			}
			if _, found := content["message"]; !found {
				t.Errorf("%q. HealthHandler() no message reported", tt.name)
			}
			if _, found := content["checks"]; !found {
				t.Errorf("%q. HealthHandler() no checks reported", tt.name)
			}
			if _, found := content["version"]; !found {
				t.Errorf("%q. HealthHandler() no version reported", tt.name)
			}
			if _, found := content["commit"]; !found {
				t.Errorf("%q. HealthHandler() no commit reported", tt.name)
			}
		})
	}
}
