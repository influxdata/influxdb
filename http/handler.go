package http

import (
	"context"
	"encoding/json"
	"net/http"
	_ "net/http/pprof" // used for debug pprof at the default path.

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2/kit/prom"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// MetricsPath exposes the prometheus metrics over /metrics.
	MetricsPath = "/metrics"
	// ReadyPath exposes the readiness of the service over /ready.
	ReadyPath = "/ready"
	// HealthPath exposes the health of the service over /health.
	HealthPath = "/health"
	// DebugPath exposes /debug/pprof for go debugging.
	DebugPath = "/debug"
)

// Handler provides basic handling of metrics, health and debug endpoints.
// All other requests are passed down to the sub handler.
type Handler struct {
	name string
	r    chi.Router

	requests   *prometheus.CounterVec
	requestDur *prometheus.HistogramVec

	// log logs all HTTP requests as they are served
	log *zap.Logger
}

type (
	handlerOpts struct {
		log            *zap.Logger
		apiHandler     http.Handler
		debugHandler   http.Handler
		healthHandler  http.Handler
		metricsHandler http.Handler
		readyHandler   http.Handler
	}

	HandlerOptFn func(opts *handlerOpts)
)

func WithLog(l *zap.Logger) HandlerOptFn {
	return func(opts *handlerOpts) {
		opts.log = l
	}
}

func WithAPIHandler(h http.Handler) HandlerOptFn {
	return func(opts *handlerOpts) {
		opts.apiHandler = h
	}
}

func WithDebugHandler(h http.Handler) HandlerOptFn {
	return func(opts *handlerOpts) {
		opts.debugHandler = h
	}
}

func WithHealthHandler(h http.Handler) HandlerOptFn {
	return func(opts *handlerOpts) {
		opts.healthHandler = h
	}
}

func WithMetricsHandler(h http.Handler) HandlerOptFn {
	return func(opts *handlerOpts) {
		opts.metricsHandler = h
	}
}

func WithReadyHandler(h http.Handler) HandlerOptFn {
	return func(opts *handlerOpts) {
		opts.readyHandler = h
	}
}

// NewHandlerFromRegistry creates a new handler with the given name,
// and sets the /metrics endpoint to use the metrics from the given registry,
// after self-registering h's metrics.
func NewHandlerFromRegistry(name string, reg *prom.Registry, opts ...HandlerOptFn) *Handler {
	opt := handlerOpts{
		log:            zap.NewNop(),
		debugHandler:   http.DefaultServeMux,
		healthHandler:  http.HandlerFunc(HealthHandler),
		metricsHandler: reg.HTTPHandler(),
		readyHandler:   ReadyHandler(),
	}
	for _, o := range opts {
		o(&opt)
	}

	h := &Handler{
		name: name,
		log:  opt.log,
	}
	h.initMetrics()

	r := chi.NewRouter()
	r.Use(
		kithttp.Trace(name),
		kithttp.Metrics(name, h.requests, h.requestDur),
	)
	{
		r.Mount(MetricsPath, opt.metricsHandler)
		r.Mount(ReadyPath, opt.readyHandler)
		r.Mount(HealthPath, opt.healthHandler)
		r.Mount(DebugPath, opt.debugHandler)
		r.Mount("/", opt.apiHandler)
	}
	h.r = r

	reg.MustRegister(h.PrometheusCollectors()...)
	return h
}

// ServeHTTP delegates a request to the appropriate subhandler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.r.ServeHTTP(w, r)
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

	labelNames := []string{"handler", "method", "path", "status", "user_agent"}
	h.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: handlerSubsystem,
		Name:      "requests_total",
		Help:      "Number of http requests received",
	}, labelNames)

	h.requestDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: handlerSubsystem,
		Name:      "request_duration_seconds",
		Help:      "Time taken to respond to HTTP request",
	}, labelNames)
}

func encodeResponse(ctx context.Context, w http.ResponseWriter, code int, res interface{}) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)

	return json.NewEncoder(w).Encode(res)
}

func logEncodingError(log *zap.Logger, r *http.Request, err error) {
	// If we encounter an error while encoding the response to an http request
	// the best thing we can do is log that error, as we may have already written
	// the headers for the http request in question.
	log.Info("Error encoding response",
		zap.String("path", r.URL.Path),
		zap.String("method", r.Method),
		zap.Error(err))
}
