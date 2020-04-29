package http

import (
	"net/http"
	"strings"

	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
)

// PlatformHandler is a collection of all the service handlers.
type PlatformHandler struct {
	AssetHandler *AssetHandler
	DocsHandler  http.HandlerFunc
	APIHandler   http.Handler
}

// NewPlatformHandler returns a platform handler that serves the API and associated assets.
func NewPlatformHandler(b *APIBackend, opts ...APIHandlerOptFn) *PlatformHandler {
	h := NewAuthenticationHandler(b.Logger, b.HTTPErrorHandler)
	h.Handler = NewAPIHandler(b, opts...)
	h.AuthorizationService = b.AuthorizationService
	h.SessionService = b.SessionService
	h.SessionRenewDisabled = b.SessionRenewDisabled
	h.UserService = b.UserService

	h.RegisterNoAuthRoute("GET", "/api/v2")
	h.RegisterNoAuthRoute("POST", "/api/v2/signin")
	h.RegisterNoAuthRoute("POST", "/api/v2/signout")
	h.RegisterNoAuthRoute("POST", "/api/v2/setup")
	h.RegisterNoAuthRoute("GET", "/api/v2/setup")
	h.RegisterNoAuthRoute("GET", "/api/v2/swagger.json")

	assetHandler := NewAssetHandler()
	assetHandler.Path = b.AssetsPath

	wrappedHandler := kithttp.SetCORS(h)
	wrappedHandler = kithttp.SkipOptions(wrappedHandler)

	return &PlatformHandler{
		AssetHandler: assetHandler,
		DocsHandler:  Redoc("/api/v2/swagger.json"),
		APIHandler:   wrappedHandler,
	}
}

// ServeHTTP delegates a request to the appropriate subhandler.
func (h *PlatformHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/docs") {
		h.DocsHandler.ServeHTTP(w, r)
		return
	}

	// Serve the chronograf assets for any basepath that does not start with addressable parts
	// of the platform API.
	if !strings.HasPrefix(r.URL.Path, "/v1") &&
		!strings.HasPrefix(r.URL.Path, "/api/v2") &&
		!strings.HasPrefix(r.URL.Path, "/chronograf/") {
		h.AssetHandler.ServeHTTP(w, r)
		return
	}

	h.APIHandler.ServeHTTP(w, r)
}
