package prom_test

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"

	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// RandomHandler implements an HTTP endpoint that prints a random float,
// and it tracks prometheus metrics about the numbers it returns.
type RandomHandler struct {
	// Cumulative sum of values served.
	valueCounter prometheus.Counter

	// Total times page served.
	serveCounter prometheus.Counter
}

var (
	_ http.Handler             = (*RandomHandler)(nil)
	_ prom.PrometheusCollector = (*RandomHandler)(nil)
)

func NewRandomHandler() *RandomHandler {
	return &RandomHandler{
		valueCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "value_counter",
			Help: "Cumulative sum of values served.",
		}),
		serveCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "serve_counter",
			Help: "Counter of times page has been served.",
		}),
	}
}

// ServeHTTP serves a random float value and updates rh's internal metrics.
func (rh *RandomHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Increment serveCounter every time we serve a page.
	rh.serveCounter.Inc()

	n := rand.Float64()
	// Track the cumulative values served.
	rh.valueCounter.Add(n)

	fmt.Fprintf(w, "%v", n)
}

// PrometheusCollectors implements prom.PrometheusCollector.
func (rh *RandomHandler) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{rh.valueCounter, rh.serveCounter}
}

func Example() {
	// A collection of endpoints and http.Handlers.
	handlers := map[string]http.Handler{
		"/random": NewRandomHandler(),
		"/time": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, time.Now().String())
		}),
	}

	// Use a local registry, not the global registry in the prometheus package.
	reg := prom.NewRegistry(zap.NewNop())

	// Build the mux out of handlers from above.
	mux := http.NewServeMux()
	for path, h := range handlers {
		mux.Handle(path, h)

		// Only register those handlers which implement prom.PrometheusCollector.
		if pc, ok := h.(prom.PrometheusCollector); ok {
			reg.MustRegister(pc.PrometheusCollectors()...)
		}
	}

	// Add metrics to registry.
	mux.Handle("/metrics", reg.HTTPHandler())

	http.ListenAndServe("localhost:8080", mux)
}
