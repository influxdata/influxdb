package http

import (
	"context"
	"encoding/json"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	MetricsPath = "/metrics"
	HealthzPath = "/healthz"
	DebugPath   = "/debug"
)

// Handler provides basic handling of metrics, healthz and debug endpoints.
// All other requests are passed down to the sub handler.
type Handler struct {
	name string
	// HealthzHandler handles healthz requests
	HealthzHandler http.Handler
	// MetricsHandler handles metrics requests
	MetricsHandler http.Handler
	// DebugHandler handles debug requests
	DebugHandler http.Handler
	// Handler handles all other requests
	Handler http.Handler
}

// NewHandler creates a new handler with the given name.
// The name is used to tag the metrics produced by this handler.
func NewHandler(name string) *Handler {
	return &Handler{
		name:           name,
		MetricsHandler: promhttp.Handler(),
		DebugHandler:   http.DefaultServeMux,
	}
}

// ServeHTTP delegates a request to the appropriate subhandler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO: better way to do this?
	statusW := newStatusResponseWriter(w)
	w = statusW

	// TODO: This could be problematic eventually. But for now it should be fine.
	defer func() {
		httpRequests.With(prometheus.Labels{
			"handler": h.name,
			"method":  r.Method,
			"path":    r.URL.Path,
			"status":  statusW.statusCodeClass(),
		}).Inc()
	}()
	defer func(start time.Time) {
		httpRequestDuration.With(prometheus.Labels{
			"handler": h.name,
			"method":  r.Method,
			"path":    r.URL.Path,
			"status":  statusW.statusCodeClass(),
		}).Observe(time.Since(start).Seconds())
	}(time.Now())

	switch {
	case r.URL.Path == MetricsPath:
		h.MetricsHandler.ServeHTTP(w, r)
	case r.URL.Path == HealthzPath:
		h.HealthzHandler.ServeHTTP(w, r)
	case strings.HasPrefix(r.URL.Path, DebugPath):
		h.DebugHandler.ServeHTTP(w, r)
	default:
		h.Handler.ServeHTTP(w, r)
	}
}

func encodeResponse(ctx context.Context, w http.ResponseWriter, code int, res interface{}) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)

	return json.NewEncoder(w).Encode(res)
}

func init() {
	prometheus.MustRegister(httpCollectors...)
}

// namespace is the leading part of all published metrics for the http handler service.
const namespace = "http"

const handlerSubsystem = "api"

// http metrics track request latency and count by path.
var (
	httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: handlerSubsystem,
		Name:      "requests_total",
		Help:      "Number of http requests received",
	}, []string{"handler", "method", "path", "status"})

	httpRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: handlerSubsystem,
		Name:      "request_duration_seconds",
		Help:      "Time taken to respond to HTTP request",
		// TODO(desa): determine what spacing these buckets should have.
		Buckets: prometheus.ExponentialBuckets(0.001, 1.5, 25),
	}, []string{"handler", "method", "path", "status"})

	httpCollectors = []prometheus.Collector{httpRequests, httpRequestDuration}
)
