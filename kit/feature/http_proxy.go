package feature

import (
	"context"
	"net/http"
	"net/http/httputil"
	"net/url"

	"go.uber.org/zap"
)

// HTTPProxy is an HTTP proxy that's guided by a feature flag. If the feature flag
// presented to it is enabled, it will perform the proxying behavior. Otherwise
// it will be a no-op.
type HTTPProxy struct {
	proxy   *httputil.ReverseProxy
	logger  *zap.Logger
	enabler ProxyEnabler
}

// NewHTTPProxy returns a new Proxy.
func NewHTTPProxy(dest *url.URL, logger *zap.Logger, enabler ProxyEnabler) *HTTPProxy {
	return &HTTPProxy{
		proxy:   newReverseProxy(dest, enabler.Key()),
		logger:  logger,
		enabler: enabler,
	}
}

// Do performs the proxying. It returns whether or not the request was proxied.
func (p *HTTPProxy) Do(w http.ResponseWriter, r *http.Request) bool {
	if p.enabler.Enabled(r.Context()) {
		p.proxy.ServeHTTP(w, r)
		return true
	}
	return false
}

const (
	// headerProxyFlag is the HTTP header for enriching the request and response
	// with the feature flag key that precipitated the proxying behavior.
	headerProxyFlag = "X-Platform-Proxy-Flag"
)

// newReverseProxy creates a new single-host reverse proxy.
func newReverseProxy(dest *url.URL, enablerKey string) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(dest)

	defaultDirector := proxy.Director
	proxy.Director = func(r *http.Request) {
		defaultDirector(r)
		r.Header.Set(headerProxyFlag, enablerKey)
	}
	proxy.ModifyResponse = func(r *http.Response) error {
		r.Header.Set(headerProxyFlag, enablerKey)
		return nil
	}
	return proxy
}

// ProxyEnabler is a boolean feature flag.
type ProxyEnabler interface {
	Key() string
	Enabled(ctx context.Context, fs ...Flagger) bool
}
