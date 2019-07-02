package executor

import (
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/prometheus/client_golang/prometheus"
)

type ExecutorMetrics struct {
	totalRunsComplete *prometheus.CounterVec
	totalRunsActive   prometheus.Gauge
	queueDelta        prometheus.Summary
}

func NewExecutorMetrics() *ExecutorMetrics {
	const namespace = "task"
	const subsystem = "executor"

	return &ExecutorMetrics{
		totalRunsComplete: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_runs_complete",
			Help:      "Total number of runs completed across all tasks, split out by success or failure.",
		}, []string{"status"}),

		totalRunsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_runs_active",
			Help:      "Total number of runs across all tasks that have started but not yet completed.",
		}),

		queueDelta: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  namespace,
			Subsystem:  subsystem,
			Name:       "run_queue_delta",
			Help:       "The duration in seconds between a run being due to start and actually starting.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (em *ExecutorMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		em.totalRunsComplete,
		em.totalRunsActive,
		em.queueDelta,
	}
}

// StartRun adjusts the metrics to indicate a run is in progress for the given task ID.
// We are also storing the delta time between when a run is due to start and actually starting.
func (em *ExecutorMetrics) StartRun(taskID influxdb.ID, queueDelta time.Duration) {
	em.totalRunsActive.Inc()
	em.queueDelta.Observe(queueDelta.Seconds())
}

// FinishRun adjusts the metrics to indicate a run is no longer in progress for the given task ID.
func (em *ExecutorMetrics) FinishRun(taskID influxdb.ID, status backend.RunStatus) {
	em.totalRunsActive.Dec()
	em.totalRunsComplete.WithLabelValues(status.String()).Inc()
}
