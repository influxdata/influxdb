package http

import (
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// PlatformHandler is a collection of all the service handlers.
type PlatformHandler struct {
	AssetHandler *AssetHandler
	APIHandler   http.Handler
}

func setCORSResponseHeaders(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization")
	}
}

// NewPlatformHandler returns a platform handler that serves the API and associated assets.
func NewPlatformHandler(b *APIBackend) *PlatformHandler {
	h := NewAuthenticationHandler()
	h.Handler = NewAPIHandler(b)
	h.AuthorizationService = b.AuthorizationService
	h.SessionService = b.SessionService

	h.RegisterNoAuthRoute("GET", "/api/v2")
	h.RegisterNoAuthRoute("POST", "/api/v2/signin")
	h.RegisterNoAuthRoute("POST", "/api/v2/signout")
	h.RegisterNoAuthRoute("POST", "/api/v2/setup")
	h.RegisterNoAuthRoute("GET", "/api/v2/setup")

	return &PlatformHandler{
		AssetHandler: NewAssetHandler(),
		APIHandler:   h,
	}
}

// ServeHTTP delegates a request to the appropriate subhandler.
func (h *PlatformHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setCORSResponseHeaders(w, r)
	if r.Method == "OPTIONS" {
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

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (h *PlatformHandler) PrometheusCollectors() []prometheus.Collector {
	// TODO: collect and return relevant metrics.
	return nil
}
