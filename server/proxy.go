package server

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
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

	// To preserve any HTTP query arguments to the kapacitor path,
	// we concat and parse them into u.
	uri := singleJoiningSlash(srv.URL, path)
	u, err := url.Parse(uri)
	if err != nil {
		msg := fmt.Sprintf("Error parsing kapacitor url: %v", err)
		Error(w, http.StatusUnprocessableEntity, msg, s.Logger)
		return
	}

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

	// Without a FlushInterval the HTTP Chunked response for kapacitor logs is
	// buffered and flushed every 30 seconds.
	proxy := &httputil.ReverseProxy{
		Director:      director,
		FlushInterval: time.Second,
	}

	// The connection to kapacitor is using a self-signed certificate.
	// This modifies uses the same values as http.DefaultTransport but specifies
	// InsecureSkipVerify
	if srv.InsecureSkipVerify {
		proxy.Transport = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		}
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

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	if aslash && bslash {
		return a + b[1:]
	}
	if !aslash && !bslash {
		return a + "/" + b
	}
	return a + b
}
