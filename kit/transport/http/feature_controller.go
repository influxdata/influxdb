package http

import (
	"context"
	"net/http"

	"github.com/influxdata/influxdb/v2/kit/feature"
)

// Enabler allows the switching between two HTTP Handlers
type Enabler interface {
	Enabled(ctx context.Context, flagger ...feature.Flagger) bool
}

// FeatureHandler is used to switch requests between an exisiting and a feature flagged
// HTTP Handler on a per-request basis
type FeatureHandler struct {
	enabler    Enabler
	flagger    feature.Flagger
	oldHandler http.Handler
	newHandler http.Handler
	prefix     string
}

func NewFeatureHandler(e Enabler, f feature.Flagger, old, new http.Handler, prefix string) *FeatureHandler {
	return &FeatureHandler{e, f, old, new, prefix}
}

func (h *FeatureHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.enabler.Enabled(r.Context(), h.flagger) {
		h.newHandler.ServeHTTP(w, r)
		return
	}
	h.oldHandler.ServeHTTP(w, r)
}

func (h *FeatureHandler) Prefix() string {
	return h.prefix
}
