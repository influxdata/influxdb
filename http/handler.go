package http

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/prom"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/pprof"
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
		log           *zap.Logger
		apiHandler    http.Handler
		healthHandler http.Handler
		readyHandler  http.Handler
		pprofEnabled  bool

		// NOTE: Track the registry even if metricsExposed = false
		// so we can report HTTP metrics via telemetry.
		metricsRegistry *prom.Registry
		metricsExposed  bool
	}

	HandlerOptFn func(opts *handlerOpts)
)

func (o *handlerOpts) metricsHTTPHandler() http.Handler {
	if o.metricsRegistry != nil && o.metricsExposed {
		return o.metricsRegistry.HTTPHandler()
	}
	handlerFunc := func(rw http.ResponseWriter, r *http.Request) {
		kithttp.WriteErrorResponse(r.Context(), rw, influxdb.EForbidden, "metrics disabled")
	}
	return http.HandlerFunc(handlerFunc)
}

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

func WithPprofEnabled(enabled bool) HandlerOptFn {
	return func(opts *handlerOpts) {
		opts.pprofEnabled = enabled
	}
}

func WithMetrics(reg *prom.Registry, exposed bool) HandlerOptFn {
	return func(opts *handlerOpts) {
		opts.metricsRegistry = reg
		opts.metricsExposed = exposed
	}
}

type AddHeader struct {
	WriteHeader func(header http.Header)
}

// Middleware is a middleware that mutates the header of all responses
func (h *AddHeader) Middleware(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		h.WriteHeader(w.Header())
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// NewRootHandler creates a new handler with the given name and registers any root-level
// (non-API) routes enabled by the caller.
func NewRootHandler(name string, opts ...HandlerOptFn) *Handler {
	opt := handlerOpts{
		log:             zap.NewNop(),
		healthHandler:   http.HandlerFunc(HealthHandler),
		readyHandler:    ReadyHandler(),
		pprofEnabled:    false,
		metricsRegistry: nil,
		metricsExposed:  false,
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
	buildHeader := &AddHeader{
		WriteHeader: func(header http.Header) {
			header.Add("X-Influxdb-Build", "OSS")
			header.Add("X-Influxdb-Version", influxdb.GetBuildInfo().Version)
		},
	}
	r.Use(buildHeader.Middleware)
	// only gather metrics for system handlers
	r.Group(func(r chi.Router) {
		r.Use(
			kithttp.Metrics(name, h.requests, h.requestDur),
		)
		r.Mount(MetricsPath, opt.metricsHTTPHandler())
		r.Mount(ReadyPath, opt.readyHandler)
		r.Mount(HealthPath, opt.healthHandler)
		r.Mount(DebugPath, pprof.NewHTTPHandler(opt.pprofEnabled))
	})

	// gather metrics and traces for everything else
	r.Group(func(r chi.Router) {
		r.Use(
			kithttp.Trace(name),
			kithttp.Metrics(name, h.requests, h.requestDur),
		)
		r.Mount("/", opt.apiHandler)
	})

	h.r = r

	if opt.metricsRegistry != nil {
		opt.metricsRegistry.MustRegister(h.PrometheusCollectors()...)
	}
	return h
}

// ServeHTTP delegates a request to the appropriate subhandler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.r.ServeHTTP(w, r)
}

// PrometheusCollectors satisfies prom.PrometheusCollector.
func (h *Handler) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		h.requests,
		h.requestDur,
	}
}

func (h *Handler) initMetrics() {
	const namespace = "http"
	const handlerSubsystem = "api"

	labelNames := []string{"handler", "method", "path", "status", "user_agent", "response_code"}
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
	log.Error("Error encoding response",
		zap.String("path", r.URL.Path),
		zap.String("method", r.Method),
		zap.Error(err))
}
