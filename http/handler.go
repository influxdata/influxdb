package http

import (
	"context"
	"encoding/json"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/influxdata/platform/kit/prom"
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

	requests   *prometheus.CounterVec
	requestDur *prometheus.HistogramVec
}

// NewHandler creates a new handler with the given name.
// The name is used to tag the metrics produced by this handler.
//
// The MetricsHandler is set to the default prometheus handler.
// It is the caller's responsibility to call prometheus.MustRegister(h.PrometheusCollectors()...).
// In most cases, you want to use NewHandlerFromRegistry instead.
func NewHandler(name string) *Handler {
	h := &Handler{
		name:           name,
		MetricsHandler: promhttp.Handler(),
		DebugHandler:   http.DefaultServeMux,
	}
	h.initMetrics()
	return h
}

// NewHandlerFromRegistry creates a new handler with the given name,
// and sets the /metrics endpoint to use the metrics from the given registry,
// after self-registering h's metrics.
func NewHandlerFromRegistry(name string, reg *prom.Registry) *Handler {
	h := &Handler{
		name:           name,
		MetricsHandler: reg.HTTPHandler(),
		DebugHandler:   http.DefaultServeMux,
	}
	h.initMetrics()
	reg.MustRegister(h.PrometheusCollectors()...)
	return h
}

// ServeHTTP delegates a request to the appropriate subhandler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO: better way to do this?
	statusW := newStatusResponseWriter(w)
	w = statusW

	// TODO: This could be problematic eventually. But for now it should be fine.
	defer func() {
		h.requests.With(prometheus.Labels{
			"handler": h.name,
			"method":  r.Method,
			"path":    r.URL.Path,
			"status":  statusW.statusCodeClass(),
		}).Inc()
	}()
	defer func(start time.Time) {
		h.requestDur.With(prometheus.Labels{
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

// PrometheusCollectors satisifies prom.PrometheusCollector.
func (h *Handler) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		h.requests,
		h.requestDur,
	}
}

func (h *Handler) initMetrics() {
	const namespace = "http"
	const handlerSubsystem = "api"

	h.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: handlerSubsystem,
		Name:      "requests_total",
		Help:      "Number of http requests received",
	}, []string{"handler", "method", "path", "status"})

	h.requestDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: handlerSubsystem,
		Name:      "request_duration_seconds",
		Help:      "Time taken to respond to HTTP request",
		// TODO(desa): determine what spacing these buckets should have.
		Buckets: prometheus.ExponentialBuckets(0.001, 1.5, 25),
	}, []string{"handler", "method", "path", "status"})
}
