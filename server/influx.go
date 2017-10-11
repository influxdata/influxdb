package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/influx"
)

// ValidInfluxRequest checks if queries specify a command.
func ValidInfluxRequest(p chronograf.Query) error {
	if p.Command == "" {
		return fmt.Errorf("query field required")
	}
	return nil
}

type postInfluxResponse struct {
	Results interface{} `json:"results"` // results from influx
}

// Influx proxies requests to influxdb.
func (s *Service) Influx(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	var req chronograf.Query
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}
	if err = ValidInfluxRequest(req); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	ctx := r.Context()
	src, err := s.SourcesStore.Get(ctx, id)
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}

	ts, err := s.TimeSeries(src)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	if err = ts.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	response, err := ts.Query(ctx, req)
	if err != nil {
		if err == chronograf.ErrUpstreamTimeout {
			msg := "Timeout waiting for Influx response"
			Error(w, http.StatusRequestTimeout, msg, s.Logger)
			return
		}
		// TODO: Here I want to return the error code from influx.
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	res := postInfluxResponse{
		Results: response,
	}
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

func (s *Service) Write(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	src, err := s.SourcesStore.Get(ctx, id)
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}

	u, err := url.Parse(src.URL)
	if err != nil {
		msg := fmt.Sprintf("Error parsing source url: %v", err)
		Error(w, http.StatusUnprocessableEntity, msg, s.Logger)
		return
	}
	u.Path = "/write"
	u.RawQuery = r.URL.RawQuery

	director := func(req *http.Request) {
		// Set the Host header of the original source URL
		req.Host = u.Host
		req.URL = u
		// Because we are acting as a proxy, influxdb needs to have the
		// basic auth or bearer token information set as a header directly
		auth := influx.DefaultAuthorization(&src)
		auth.Set(req)
	}
	proxy := &httputil.ReverseProxy{
		Director: director,
	}
	proxy.ServeHTTP(w, r)
}
