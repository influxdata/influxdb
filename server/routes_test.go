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
	handler := &AllRoutes{
		Logger: logger,
	}
	req := httptest.NewRequest("GET", "http://docbrowns-inventions.com", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

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
	want := `{"layouts":"/chronograf/v1/layouts","mappings":"/chronograf/v1/mappings","sources":"/chronograf/v1/sources","me":"/chronograf/v1/me","dashboards":"/chronograf/v1/dashboards","auth":[],"external":{}}
`
	if want != string(body) {
		t.Errorf("TestAllRoutes\nwanted\n*%s*\ngot\n*%s*", want, string(body))
	}

}

func TestAllRoutesWithAuth(t *testing.T) {
	logger := log.New(log.DebugLevel)
	handler := &AllRoutes{
		AuthRoutes: []AuthRoute{
			{
				Name:     "github",
				Label:    "GitHub",
				Login:    "/oauth/github/login",
				Logout:   "/oauth/github/logout",
				Callback: "/oauth/github/callback",
			},
		},
		LogoutLink: "/oauth/logout",
		Logger:     logger,
	}
	req := httptest.NewRequest("GET", "http://docbrowns-inventions.com", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	if err != nil {
		t.Error("TestAllRoutesWithAuth not able to retrieve body")
	}
	var routes getRoutesResponse
	if err := json.Unmarshal(body, &routes); err != nil {
		t.Error("TestAllRoutesWithAuth not able to unmarshal JSON response")
	}
	want := `{"layouts":"/chronograf/v1/layouts","mappings":"/chronograf/v1/mappings","sources":"/chronograf/v1/sources","me":"/chronograf/v1/me","dashboards":"/chronograf/v1/dashboards","auth":[{"name":"github","label":"GitHub","login":"/oauth/github/login","logout":"/oauth/github/logout","callback":"/oauth/github/callback"}],"logout":"/oauth/logout","external":{}}
`
	if want != string(body) {
		t.Errorf("TestAllRoutesWithAuth\nwanted\n*%s*\ngot\n*%s*", want, string(body))
	}
}
