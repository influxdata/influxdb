package http

import (
	nethttp "net/http"
	"strings"

	idpctx "github.com/influxdata/platform/context"
)

// PlatformHandler is a collection of all the service handlers.
type PlatformHandler struct {
	BucketHandler        *BucketHandler
	UserHandler          *UserHandler
	OrgHandler           *OrgHandler
	AuthorizationHandler *AuthorizationHandler
}

func setCORSResponseHeaders(w nethttp.ResponseWriter, r *nethttp.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization")
	}
}

// ServeHTTP delegates a request to the appropriate subhandler.
func (h *PlatformHandler) ServeHTTP(w nethttp.ResponseWriter, r *nethttp.Request) {

	setCORSResponseHeaders(w, r)
	if r.Method == "OPTIONS" {
		return
	}

	ctx := r.Context()
	ctx = idpctx.TokenFromAuthorizationHeader(ctx, r)
	r = r.WithContext(ctx)

	if strings.HasPrefix(r.URL.Path, "/v1/buckets") {
		h.BucketHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/v1/users") {
		h.UserHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/v1/orgs") {
		h.OrgHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/v1/authorizations") {
		h.AuthorizationHandler.ServeHTTP(w, r)
		return
	}

	nethttp.NotFound(w, r)
}
