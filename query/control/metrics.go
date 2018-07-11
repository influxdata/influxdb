package control

import "github.com/prometheus/client_golang/prometheus"

// controllerMetrics holds metrics related to the query controller.
type controllerMetrics struct {
	compiling  *prometheus.GaugeVec
	queueing   *prometheus.GaugeVec
	requeueing *prometheus.GaugeVec
	planning   *prometheus.GaugeVec
	executing  *prometheus.GaugeVec

	compilingDur  *prometheus.HistogramVec
	queueingDur   *prometheus.HistogramVec
	requeueingDur *prometheus.HistogramVec
	planningDur   *prometheus.HistogramVec
	executingDur  *prometheus.HistogramVec
}

func newControllerMetrics() *controllerMetrics {
	const (
		namespace = "query"
		subsystem = "control"
	)

	labels := []string{"org"}

	return &controllerMetrics{
		compiling: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "compiling_active",
			Help:      "Number of queries actively compiling",
		}, labels),

		queueing: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queueing_active",
			Help:      "Number of queries actively queueing",
		}, labels),

		requeueing: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "requeueing_active",
			Help:      "Number of queries actively requeueing",
		}, labels),

		planning: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "planning_active",
			Help:      "Number of queries actively planning",
		}, labels),

		executing: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "executing_active",
			Help:      "Number of queries actively executing",
		}, labels),

		compilingDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "compiling_duration_seconds",
			Help:      "Histogram of times spent compiling queries",
			Buckets:   prometheus.ExponentialBuckets(1e-3, 5, 7),
		}, labels),

		queueingDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queueing_duration_seconds",
			Help:      "Histogram of times spent queueing queries",
			Buckets:   prometheus.ExponentialBuckets(1e-3, 5, 7),
		}, labels),

		requeueingDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "requeueing_duration_seconds",
			Help:      "Histogram of times spent requeueing queries",
			Buckets:   prometheus.ExponentialBuckets(1e-3, 5, 7),
		}, labels),

		planningDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "planning_duration_seconds",
			Help:      "Histogram of times spent planning queries",
			Buckets:   prometheus.ExponentialBuckets(1e-5, 5, 7),
		}, labels),

		executingDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "executing_duration_seconds",
			Help:      "Histogram of times spent executing queries",
			Buckets:   prometheus.ExponentialBuckets(1e-3, 5, 7),
		}, labels),
	}
}

// PrometheusCollectors satisifies the prom.PrometheusCollector interface.
func (cm *controllerMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		cm.compiling,
		cm.queueing,
		cm.requeueing,
		cm.planning,
		cm.executing,

		cm.compilingDur,
		cm.queueingDur,
		cm.requeueingDur,
		cm.planningDur,
		cm.executingDur,
	}
}
