package http

import (
	"net/http"
)

var _ http.Handler = &proxyHandler{}

// withFeatureProxy wraps an HTTP handler in a proxyHandler
func withFeatureProxy(proxy FeatureProxyHandler, h http.Handler) *proxyHandler {
	if proxy == nil {
		proxy = &NoopProxyHandler{}
	}
	return &proxyHandler{
		proxy:   proxy,
		handler: h,
	}
}

// proxyHandler is a wrapper around an http.Handler that conditionally forwards
// a request to another HTTP backend using a proxy. If the proxy doesn't decide
// to forward the request, we fall-back to our normal http.Handler behavior.
type proxyHandler struct {
	proxy   FeatureProxyHandler
	handler http.Handler
}

// ServeHTTP implements http.Handler interface. It first
func (h *proxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.proxy.Do(w, r) {
		return
	}
	h.handler.ServeHTTP(w, r)
}

// FeatureProxyHandler is an HTTP proxy that conditionally forwards requests to
// another backend.
type FeatureProxyHandler interface {
	Do(w http.ResponseWriter, r *http.Request) bool
}

// NoopProxyHandler is a no-op FeatureProxyHandler. It should be used if
// no feature-flag driven proxying is necessary.
type NoopProxyHandler struct{}

// Do implements FeatureProxyHandler.
func (h *NoopProxyHandler) Do(http.ResponseWriter, *http.Request) bool {
	return false
}
