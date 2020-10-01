package control

import (
	"github.com/prometheus/client_golang/prometheus"
)

// controllerMetrics holds metrics related to the query controller.
type ControllerMetrics struct {
	Requests          *prometheus.CounterVec
	NotImplemented    *prometheus.CounterVec
	RequestsLatency   *prometheus.HistogramVec
	ExecutingDuration *prometheus.HistogramVec
}

const (
	LabelSuccess        = "success"
	LabelGenericError   = "generic_err"
	LabelParseErr       = "parse_err"
	LabelInterruptedErr = "interrupt_err"
	LabelRuntimeError   = "runtime_error"
	LabelNotImplError   = "not_implemented"
	LabelNotExecuted    = "not_executed"
)

func NewControllerMetrics(labels []string) *ControllerMetrics {
	const (
		namespace = "influxql"
		subsystem = "service"
	)

	return &ControllerMetrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "requests_total",
			Help:      "Count of the query requests",
		}, append(labels, "result")),

		NotImplemented: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "not_implemented_total",
			Help:      "Count of the query requests executing unimplemented operations",
		}, []string{"operation"}),

		RequestsLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "requests_latency_seconds",
			Help:      "Histogram of times spent for end-to-end latency (from issuing query request, to receiving the first byte of the response)",
			Buckets:   prometheus.ExponentialBuckets(1e-3, 5, 7),
		}, append(labels, "result")),

		ExecutingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "executing_duration_seconds",
			Help:      "Histogram of times spent executing queries",
			Buckets:   prometheus.ExponentialBuckets(1e-3, 5, 7),
		}, append(labels, "result")),
	}
}

func (cm *ControllerMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		cm.Requests,
		cm.NotImplemented,
		cm.ExecutingDuration,
	}
}
