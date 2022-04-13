package http

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadyHandler(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	ReadyHandler().ServeHTTP(w, r)
	res := w.Result()
	contentType := res.Header.Get("Content-Type")
	body, _ := io.ReadAll(res.Body)

	if res.StatusCode != 200 {
		t.Errorf("TestReadyHandler. ReadyHandler() StatusCode = %v, want 200", res.StatusCode)
	}
	if !strings.HasPrefix(contentType, "application/json") {
		t.Errorf("TestReadyHandler. ReadyHandler() Content-Type = %v, want application/json", contentType)
	}
	var content map[string]interface{}
	if err := json.Unmarshal(body, &content); err != nil {
		t.Errorf("TestReadyHandler. ReadyHandler() error unmarshaling json body %v", err)
		return
	}
	if val := content["status"]; val != "ready" {
		t.Errorf("TestReadyHandler. ReadyHandler() .status = %v, want 'ready'", val)
	}
	if val := content["started"]; val == nil {
		t.Errorf("TestReadyHandler. ReadyHandler() .started is not returned")
	}
	if val := content["up"]; val == nil {
		t.Errorf("TestReadyHandler. ReadyHandler() .up is not returned")
	}
}
