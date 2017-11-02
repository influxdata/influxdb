package server

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// KapacitorProxy proxies requests to kapacitor using the path query parameter.
func (s *Service) KapacitorProxy(w http.ResponseWriter, r *http.Request) {
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		Error(w, http.StatusUnprocessableEntity, "path query parameter required", s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.Store.Servers(ctx).Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, s.Logger)
		return
	}

	u, err := url.Parse(srv.URL)
	if err != nil {
		msg := fmt.Sprintf("Error parsing kapacitor url: %v", err)
		Error(w, http.StatusUnprocessableEntity, msg, s.Logger)
		return
	}

	u.Path = path

	director := func(req *http.Request) {
		// Set the Host header of the original Kapacitor URL
		req.Host = u.Host

		req.URL = u
		// Because we are acting as a proxy, kapacitor needs to have the basic auth information set as
		// a header directly
		if srv.Username != "" && srv.Password != "" {
			req.SetBasicAuth(srv.Username, srv.Password)
		}
	}
	proxy := &httputil.ReverseProxy{
		Director: director,
	}
	proxy.ServeHTTP(w, r)
}

// KapacitorProxyPost proxies POST to kapacitor
func (s *Service) KapacitorProxyPost(w http.ResponseWriter, r *http.Request) {
	s.KapacitorProxy(w, r)
}

// KapacitorProxyPatch proxies PATCH to kapacitor
func (s *Service) KapacitorProxyPatch(w http.ResponseWriter, r *http.Request) {
	s.KapacitorProxy(w, r)
}

// KapacitorProxyGet proxies GET to kapacitor
func (s *Service) KapacitorProxyGet(w http.ResponseWriter, r *http.Request) {
	s.KapacitorProxy(w, r)
}

// KapacitorProxyDelete proxies DELETE to kapacitor
func (s *Service) KapacitorProxyDelete(w http.ResponseWriter, r *http.Request) {
	s.KapacitorProxy(w, r)
}
