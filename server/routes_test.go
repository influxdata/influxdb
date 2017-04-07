package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/chronograf/log"
)

func TestAllRoutes(t *testing.T) {
	logger := log.New(log.DebugLevel)
	handler := AllRoutes([]AuthRoute{}, logger)
	req := httptest.NewRequest("GET", "http://docbrowns-inventions.com", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	if err != nil {
		t.Error("TestAllRoutes not able to retrieve body")
	}
	var routes getRoutesResponse
	if err := json.Unmarshal(body, &routes); err != nil {
		t.Error("TestAllRoutes not able to unmarshal JSON response")
	}
}
