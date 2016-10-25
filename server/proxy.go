package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/influxdata/chronograf"
)

type InfluxProxy struct {
	Srcs         chronograf.SourcesStore
	ServersStore chronograf.ServersStore
	TimeSeries   chronograf.TimeSeries
	Logger       chronograf.Logger
}

func ValidProxyRequest(p chronograf.Query) error {
	if p.Command == "" {
		return fmt.Errorf("query field required")
	}
	return nil
}

type postProxyResponse struct {
	Results interface{} `json:"results"` // results from influx
}

func (h *InfluxProxy) Proxy(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	var req chronograf.Query
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w)
		return
	}
	if err = ValidProxyRequest(req); err != nil {
		invalidData(w, err)
		return
	}

	ctx := r.Context()
	src, err := h.Srcs.Get(ctx, id)
	if err != nil {
		notFound(w, id)
		return
	}

	if err = h.TimeSeries.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d", id)
		Error(w, http.StatusBadRequest, msg)
		return
	}

	response, err := h.TimeSeries.Query(ctx, req)
	if err != nil {
		if err == chronograf.ErrUpstreamTimeout {
			msg := "Timeout waiting for Influx response"
			Error(w, http.StatusRequestTimeout, msg)
			return
		}
		// TODO: Here I want to return the error code from influx.
		Error(w, http.StatusBadRequest, err.Error())
		return
	}

	res := postProxyResponse{
		Results: response,
	}
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

func (h *InfluxProxy) KapacitorProxy(w http.ResponseWriter, r *http.Request) {
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		Error(w, http.StatusUnprocessableEntity, "path query parameter required")
		return
	}

	ctx := r.Context()
	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id)
		return
	}

	u, err := url.Parse(srv.URL)
	if err != nil {
		msg := fmt.Sprintf("Error parsing kapacitor url: %v", err)
		Error(w, http.StatusUnprocessableEntity, msg)
		return
	}

	if srv.Username != "" && srv.Password != "" {
		u.User = url.UserPassword(srv.Username, srv.Password)
	}
	u.Path = path

	director := func(req *http.Request) {
		req.URL = u
	}
	proxy := &httputil.ReverseProxy{
		Director: director,
	}
	proxy.ServeHTTP(w, r)
}

func (h *InfluxProxy) KapacitorProxyPost(w http.ResponseWriter, r *http.Request) {
	h.KapacitorProxy(w, r)
}

func (h *InfluxProxy) KapacitorProxyPatch(w http.ResponseWriter, r *http.Request) {
	h.KapacitorProxy(w, r)
}

func (h *InfluxProxy) KapacitorProxyGet(w http.ResponseWriter, r *http.Request) {
	h.KapacitorProxy(w, r)
}

func (h *InfluxProxy) KapacitorProxyDelete(w http.ResponseWriter, r *http.Request) {
	h.KapacitorProxy(w, r)
}
