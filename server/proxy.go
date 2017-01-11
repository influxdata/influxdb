package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/influxdata/chronograf"
)

// ValidProxyRequest checks if queries specify a command.
func ValidProxyRequest(p chronograf.Query) error {
	if p.Command == "" {
		return fmt.Errorf("query field required")
	}
	return nil
}

type postProxyResponse struct {
	Results interface{} `json:"results"` // results from influx
}

// Proxy proxies requests to infludb.
func (h *Service) Proxy(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	var req chronograf.Query
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	if err = ValidProxyRequest(req); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	ctx := r.Context()
	src, err := h.SourcesStore.Get(ctx, id)
	if err != nil {
		notFound(w, id, h.Logger)
		return
	}

	if err = h.TimeSeries.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d", id)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	response, err := h.TimeSeries.Query(ctx, req)
	if err != nil {
		if err == chronograf.ErrUpstreamTimeout {
			msg := "Timeout waiting for Influx response"
			Error(w, http.StatusRequestTimeout, msg, h.Logger)
			return
		}
		// TODO: Here I want to return the error code from influx.
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	res := postProxyResponse{
		Results: response,
	}
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// KapacitorProxy proxies requests to kapacitor using the path query parameter.
func (h *Service) KapacitorProxy(w http.ResponseWriter, r *http.Request) {
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		Error(w, http.StatusUnprocessableEntity, "path query parameter required", h.Logger)
		return
	}

	ctx := r.Context()
	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, h.Logger)
		return
	}

	u, err := url.Parse(srv.URL)
	if err != nil {
		msg := fmt.Sprintf("Error parsing kapacitor url: %v", err)
		Error(w, http.StatusUnprocessableEntity, msg, h.Logger)
		return
	}

	u.Path = path

	director := func(req *http.Request) {
		req.URL = u
		// Because we are acting as a proxy, kapacitor needs to have the basic auth information set as
		// a header directly
		if srv.Username != "" && srv.Password != "" {
			auth := "Basic " + srv.Username + ":" + srv.Password
			header := base64.StdEncoding.EncodeToString([]byte(auth))
			req.Header.Set("Authorization", header)
		}
	}
	proxy := &httputil.ReverseProxy{
		Director: director,
	}
	proxy.ServeHTTP(w, r)
}

// KapacitorProxyPost proxies POST to kapacitor
func (h *Service) KapacitorProxyPost(w http.ResponseWriter, r *http.Request) {
	h.KapacitorProxy(w, r)
}

// KapacitorProxyPatch proxies PATCH to kapacitor
func (h *Service) KapacitorProxyPatch(w http.ResponseWriter, r *http.Request) {
	h.KapacitorProxy(w, r)
}

// KapacitorProxyGet proxies GET to kapacitor
func (h *Service) KapacitorProxyGet(w http.ResponseWriter, r *http.Request) {
	h.KapacitorProxy(w, r)
}

// KapacitorProxyDelete proxies DELETE to kapacitor
func (h *Service) KapacitorProxyDelete(w http.ResponseWriter, r *http.Request) {
	h.KapacitorProxy(w, r)
}
