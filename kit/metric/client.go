package metric

import (
	"time"

	"github.com/influxdata/influxdb/kit/platform/errors"
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
		counterMetrics: map[string]VecOpts{
			"call_total": {
				Help:       "Number of calls",
				LabelNames: []string{"method"},
				CounterFn: func(vec *prometheus.CounterVec, o CollectFnOpts) {
					vec.With(prometheus.Labels{"method": o.Method}).Inc()
				},
			},
			"error_total": {
				Help:       "Number of errors encountered",
				LabelNames: []string{"method", "code"},
				CounterFn: func(vec *prometheus.CounterVec, o CollectFnOpts) {
					if o.Err != nil {
						vec.With(prometheus.Labels{
							"method": o.Method,
							"code":   errors.ErrorCode(o.Err),
						}).Inc()
					}
				},
			},
		},
		histogramMetrics: map[string]VecOpts{
			"duration": {
				Help:       "Duration of calls",
				LabelNames: []string{"method"},
				HistogramFn: func(vec *prometheus.HistogramVec, o CollectFnOpts) {
					vec.
						With(prometheus.Labels{"method": o.Method}).
						Observe(time.Since(o.Start).Seconds())
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
			fn: vecOpts.CounterFn,
			CounterVec: prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: opt.namespace,
				Subsystem: opt.serviceName(),
				Name:      metricName,
				Help:      vecOpts.Help,
			}, vecOpts.LabelNames),
		})
	}

	for metricName, vecOpts := range opt.histogramMetrics {
		client.metrics = append(client.metrics, &histogram{
			fn: vecOpts.HistogramFn,
			HistogramVec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Namespace: opt.namespace,
				Subsystem: opt.serviceName(),
				Name:      metricName,
				Help:      vecOpts.Help,
			}, vecOpts.LabelNames),
		})
	}

	reg.MustRegister(client.collectors()...)

	return client
}

type RecordFn func(err error, opts ...func(opts *CollectFnOpts)) error

// RecordAdditional provides an extension to the base method, err data provided
// to the metrics.
func RecordAdditional(props map[string]interface{}) func(opts *CollectFnOpts) {
	return func(opts *CollectFnOpts) {
		opts.AdditionalProps = props
	}
}

// Record returns a record fn that is called on any given return err. If an error is encountered
// it will register the err metric. The err is never altered.
func (c *REDClient) Record(method string) RecordFn {
	start := time.Now()
	return func(err error, opts ...func(opts *CollectFnOpts)) error {
		opt := CollectFnOpts{
			Method: method,
			Start:  start,
			Err:    err,
		}
		for _, o := range opts {
			o(&opt)
		}

		for _, metric := range c.metrics {
			metric.collect(opt)
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

	collect(o CollectFnOpts)
}

type counter struct {
	*prometheus.CounterVec

	fn CounterFn
}

func (c *counter) collect(o CollectFnOpts) {
	c.fn(c.CounterVec, o)
}

type histogram struct {
	*prometheus.HistogramVec

	fn HistogramFn
}

func (h *histogram) collect(o CollectFnOpts) {
	h.fn(h.HistogramVec, o)
}
