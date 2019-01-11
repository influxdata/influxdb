// Package prom provides a wrapper around a prometheus metrics registry
// so that all services are unified in how they expose prometheus metrics.
package prom

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// PrometheusCollector is the interface for a type to expose prometheus metrics.
// This interface is provided as a convention, so that you can optionally check
// if a type implements it and then pass its collectors to (*Registry).MustRegister.
type PrometheusCollector interface {
	// PrometheusCollectors returns a slice of prometheus collectors
	// containing metrics for the underlying instance.
	PrometheusCollectors() []prometheus.Collector
}

// Registry embeds a prometheus registry and adds a couple convenience methods.
type Registry struct {
	*prometheus.Registry

	logger *zap.Logger
}

// NewRegistry returns a new registry.
func NewRegistry() *Registry {
	return &Registry{
		Registry: prometheus.NewRegistry(),
		logger:   zap.NewNop(),
	}
}

// WithLogger sets the logger for the Registry.
// The logger will print any errors that occur while serving metrics over HTTP.
func (r *Registry) WithLogger(l *zap.Logger) {
	r.logger = l.With(zap.String("service", "prom_registry"))
}

// HTTPHandler returns an http.Handler for the registry,
// so that the /metrics HTTP handler is uniformly configured across all apps in the platform.
func (r *Registry) HTTPHandler() http.Handler {
	opts := promhttp.HandlerOpts{
		ErrorLog: promLogger{r: r},
		// TODO(mr): decide if we want to set MaxRequestsInFlight or Timeout.
	}
	return promhttp.HandlerFor(r.Registry, opts)
}

// promLogger satisfies the promhttp.Logger interface with the registry.
// Because normal usage is that WithLogger is called after HTTPHandler,
// we refer to the Registry rather than its logger.
type promLogger struct {
	r *Registry
}

var _ promhttp.Logger = (*promLogger)(nil)

// Println implements promhttp.Logger.
func (pl promLogger) Println(v ...interface{}) {
	pl.r.logger.Sugar().Info(v...)
}
