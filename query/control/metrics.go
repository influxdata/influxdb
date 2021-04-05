package control

import "github.com/prometheus/client_golang/prometheus"

// controllerMetrics holds metrics related to the query controller.
type controllerMetrics struct {
	requests  *prometheus.CounterVec
	functions *prometheus.CounterVec

	all          *prometheus.GaugeVec
	compiling    *prometheus.GaugeVec
	queueing     *prometheus.GaugeVec
	executing    *prometheus.GaugeVec
	memoryUnused *prometheus.GaugeVec

	allDur       *prometheus.HistogramVec
	compilingDur *prometheus.HistogramVec
	queueingDur  *prometheus.HistogramVec
	executingDur *prometheus.HistogramVec
}

type requestsLabel string

const (
	labelSuccess      = requestsLabel("success")
	labelCompileError = requestsLabel("compile_error")
	labelRuntimeError = requestsLabel("runtime_error")
	labelQueueError   = requestsLabel("queue_error")
)

func newControllerMetrics(labels []string) *controllerMetrics {
	return &controllerMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "qc_requests_total",
			Help: "Count of the query requests",
		}, append(labels, "result")),

		functions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "qc_functions_total",
			Help: "Count of functions in queries",
		}, append(labels, "function")),

		all: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "qc_all_active",
			Help: "Number of queries in all states",
		}, labels),

		compiling: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "qc_compiling_active",
			Help: "Number of queries actively compiling",
		}, append(labels, "compiler_type")),

		queueing: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "qc_queueing_active",
			Help: "Number of queries actively queueing",
		}, labels),

		executing: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "qc_executing_active",
			Help: "Number of queries actively executing",
		}, labels),

		memoryUnused: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "qc_memory_unused_bytes",
			Help: "The free memory as seen by the internal memory manager",
		}, labels),

		allDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "qc_all_duration_seconds",
			Help:    "Histogram of total times spent in all query states",
			Buckets: prometheus.ExponentialBuckets(1e-3, 5, 7),
		}, labels),

		compilingDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "qc_compiling_duration_seconds",
			Help:    "Histogram of times spent compiling queries",
			Buckets: prometheus.ExponentialBuckets(1e-3, 5, 7),
		}, append(labels, "compiler_type")),

		queueingDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "qc_queueing_duration_seconds",
			Help:    "Histogram of times spent queueing queries",
			Buckets: prometheus.ExponentialBuckets(1e-3, 5, 7),
		}, labels),

		executingDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "qc_executing_duration_seconds",
			Help:    "Histogram of times spent executing queries",
			Buckets: prometheus.ExponentialBuckets(1e-3, 5, 7),
		}, labels),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (cm *controllerMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		cm.requests,
		cm.functions,

		cm.all,
		cm.compiling,
		cm.queueing,
		cm.executing,
		cm.memoryUnused,

		cm.allDur,
		cm.compilingDur,
		cm.queueingDur,
		cm.executingDur,
	}
}
