package metric

import (
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// REDClient is a metrics client for collection RED metrics.
type REDClient struct {
	metrics []metricCollector
}

// New creates a new REDClient.
func New(reg prometheus.Registerer, service string, opts ...ClientOptFn) *REDClient {
	opt := metricOpts{
		namespace: "service",
		service:   service,
		counterMetrics: map[string]newVecOpts{
			"call_total": {
				help:       "Number of calls",
				labelNames: []string{"method"},
				counterFn: func(vec *prometheus.CounterVec, o fnOpts) {
					vec.With(prometheus.Labels{"method": o.method}).Inc()
				},
			},
			"error_total": {
				help:       "Number of errors encountered",
				labelNames: []string{"method", "code"},
				counterFn: func(vec *prometheus.CounterVec, o fnOpts) {
					if o.err != nil {
						vec.With(prometheus.Labels{
							"method": o.method,
							"code":   influxdb.ErrorCode(o.err),
						}).Inc()
					}
				},
			},
		},
		histogramMetrics: map[string]newVecOpts{
			"duration": {
				help:       "Duration of calls",
				labelNames: []string{"method"},
				histogramFn: func(vec *prometheus.HistogramVec, o fnOpts) {
					vec.
						With(prometheus.Labels{"method": o.method}).
						Observe(time.Since(o.start).Seconds())
				},
			},
		},
	}
	for _, o := range opts {
		o(&opt)
	}

	client := new(REDClient)
	for metricName, vecOpts := range opt.counterMetrics {
		client.metrics = append(client.metrics, &counter{
			fn: vecOpts.counterFn,
			CounterVec: prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: opt.namespace,
				Subsystem: opt.serviceName(),
				Name:      metricName,
				Help:      vecOpts.help,
			}, vecOpts.labelNames),
		})
	}

	for metricName, vecOpts := range opt.histogramMetrics {
		client.metrics = append(client.metrics, &histogram{
			fn: vecOpts.histogramFn,
			HistogramVec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Namespace: opt.namespace,
				Subsystem: opt.serviceName(),
				Name:      metricName,
				Help:      vecOpts.help,
			}, vecOpts.labelNames),
		})
	}

	reg.MustRegister(client.collectors()...)

	return client
}

// Record returns a record fn that is called on any given return err. If an error is encountered
// it will register the err metric. The err is never altered.
func (c *REDClient) Record(method string) func(error) error {
	start := time.Now()
	return func(err error) error {
		for _, metric := range c.metrics {
			metric.collect(fnOpts{
				method: method,
				start:  start,
				err:    err,
			})
		}

		return err
	}
}

func (c *REDClient) collectors() []prometheus.Collector {
	var collectors []prometheus.Collector
	for _, metric := range c.metrics {
		collectors = append(collectors, metric)
	}
	return collectors
}

type metricCollector interface {
	prometheus.Collector

	collect(o fnOpts)
}

type counter struct {
	*prometheus.CounterVec

	fn counterFn
}

func (c *counter) collect(o fnOpts) {
	c.fn(c.CounterVec, o)
}

type histogram struct {
	*prometheus.HistogramVec

	fn histogramFn
}

func (h *histogram) collect(o fnOpts) {
	h.fn(h.HistogramVec, o)
}
