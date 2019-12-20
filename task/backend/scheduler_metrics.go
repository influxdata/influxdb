package backend

import (
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// schedulerMetrics is a collection of metrics relating to task scheduling.
// All of its methods which accept task IDs, take them as strings,
// under the assumption that it is at least somewhat likely the caller already has a stringified version of the ID.
type schedulerMetrics struct {
	totalRunsComplete *prometheus.CounterVec
	totalRunsActive   prometheus.Gauge

	runsComplete *prometheus.CounterVec
	runsActive   *prometheus.GaugeVec

	errorsCounter *prometheus.CounterVec

	claimsComplete *prometheus.CounterVec
	claimsActive   prometheus.Gauge

	queueDelta     *prometheus.SummaryVec
	executionDelta *prometheus.SummaryVec
}

func newSchedulerMetrics() *schedulerMetrics {
	const namespace = "task"
	const subsystem = "scheduler"

	return &schedulerMetrics{
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

		runsComplete: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "runs_complete",
			Help:      "Number of runs completed, split out by task ID and success or failure.",
		}, []string{"task_id", "status"}),
		runsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "runs_active",
			Help:      "Total number of runs that have started but not yet completed, split out by task ID.",
		}, []string{"task_id"}),

		errorsCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "errors_counter",
			Help:      "The number of errors thrown by scheduler with the type of error (ex. Invalid, Internal, etc.",
		}, []string{"error_type"}),

		claimsComplete: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "claims_complete",
			Help:      "Total number of claims completed, split out by success or failure.",
		}, []string{"status"}),
		claimsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "claims_active",
			Help:      "Total number of claims currently held.",
		}),
		queueDelta: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  namespace,
			Subsystem:  subsystem,
			Name:       "run_queue_delta",
			Help:       "The duration in seconds between a run being due to start and actually starting.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"taskID"}),
		executionDelta: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  namespace,
			Subsystem:  subsystem,
			Name:       "run_execution_delta",
			Help:       "The duration in seconds between a run starting and complete execution.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"taskID"}),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (sm *schedulerMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		sm.totalRunsComplete,
		sm.totalRunsActive,
		sm.runsComplete,
		sm.runsActive,
		sm.claimsComplete,
		sm.claimsActive,
		sm.queueDelta,
		sm.executionDelta,
		sm.errorsCounter,
	}
}

// StartRun adjusts the metrics to indicate a run is in progress for the given task ID.
// We are also storing the delta time between when a run is due to start and actually starting.
func (sm *schedulerMetrics) StartRun(tid string, queueDelta time.Duration) {
	sm.totalRunsActive.Inc()
	sm.queueDelta.WithLabelValues("all").Observe(queueDelta.Seconds())
	sm.queueDelta.WithLabelValues(tid).Observe(queueDelta.Seconds())
	sm.runsActive.WithLabelValues(tid).Inc()
}

// FinishRun adjusts the metrics to indicate a run is no longer in progress for the given task ID.
func (sm *schedulerMetrics) FinishRun(tid string, succeeded bool, executionDelta time.Duration) {
	status := statusString(succeeded)

	sm.totalRunsActive.Dec()
	sm.totalRunsComplete.WithLabelValues(status).Inc()

	sm.runsActive.WithLabelValues(tid).Dec()
	sm.runsComplete.WithLabelValues(tid, status).Inc()

	sm.executionDelta.WithLabelValues("all").Observe(executionDelta.Seconds())
	sm.executionDelta.WithLabelValues(tid).Observe(executionDelta.Seconds())
}

// LogError increments the count of errors.
func (sm *schedulerMetrics) LogError(err error) {
	switch e := err.(type) {
	case *influxdb.Error:
		sm.errorsCounter.WithLabelValues(e.Code).Inc()
	default:
		sm.errorsCounter.WithLabelValues("unknown").Inc()
	}
}

// ClaimTask adjusts the metrics to indicate the result of an attempted claim.
func (sm *schedulerMetrics) ClaimTask(succeeded bool) {
	status := statusString(succeeded)

	sm.claimsComplete.WithLabelValues(status).Inc()
	if succeeded {
		sm.claimsActive.Inc()
	}
}

// ReleaseTask adjusts the metrics to indicate a task is no longer claimed.
// We are not (currently) tracking failed releases, so only call this on a successful release.
func (sm *schedulerMetrics) ReleaseTask(tid string) {
	sm.claimsActive.Dec()
	sm.runsActive.DeleteLabelValues(tid)
	sm.runsComplete.DeleteLabelValues(tid, statusString(true))
	sm.runsComplete.DeleteLabelValues(tid, statusString(false))
}

func statusString(succeeded bool) string {
	if succeeded {
		return "success"
	}
	return "failure"
}
