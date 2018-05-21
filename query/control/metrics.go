package control

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace = "query"
	subsystem = "control"
)

var (
	labels = []string{"org"}
)

var (
	compilingGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "compiling_active",
		Help:      "Number of queries actively compiling",
	}, labels)

	queueingGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "queueing_active",
		Help:      "Number of queries actively queueing",
	}, labels)

	requeueingGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "requeueing_active",
		Help:      "Number of queries actively requeueing",
	}, labels)

	planningGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "planning_active",
		Help:      "Number of queries actively planning",
	}, labels)

	executingGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "executing_active",
		Help:      "Number of queries actively executing",
	}, labels)

	compilingHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "compiling_duration_seconds",
		Help:      "Histogram of times spent compiling queries",
		Buckets:   prometheus.ExponentialBuckets(1e-3, 5, 7),
	}, labels)

	queueingHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "queueing_duration_seconds",
		Help:      "Histogram of times spent queueing queries",
		Buckets:   prometheus.ExponentialBuckets(1e-3, 5, 7),
	}, labels)

	requeueingHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "requeueing_duration_seconds",
		Help:      "Histogram of times spent requeueing queries",
		Buckets:   prometheus.ExponentialBuckets(1e-3, 5, 7),
	}, labels)

	planningHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "planning_duration_seconds",
		Help:      "Histogram of times spent planning queries",
		Buckets:   prometheus.ExponentialBuckets(1e-5, 5, 7),
	}, labels)

	executingHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "executing_duration_seconds",
		Help:      "Histogram of times spent executing queries",
		Buckets:   prometheus.ExponentialBuckets(1e-3, 5, 7),
	}, labels)
)

func init() {
	prometheus.MustRegister(compilingGauge)
	prometheus.MustRegister(queueingGauge)
	prometheus.MustRegister(requeueingGauge)
	prometheus.MustRegister(planningGauge)
	prometheus.MustRegister(executingGauge)

	prometheus.MustRegister(compilingHist)
	prometheus.MustRegister(queueingHist)
	prometheus.MustRegister(requeueingHist)
	prometheus.MustRegister(planningHist)
	prometheus.MustRegister(executingHist)
}
