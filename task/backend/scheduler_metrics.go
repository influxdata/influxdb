package backend

import "github.com/prometheus/client_golang/prometheus"

// schedulerMetrics is a collection of metrics relating to task scheduling.
// All of its methods which accept task IDs, take them as strings,
// under the assumption that it is at least somewhat likely the caller already has a stringified version of the ID.
type schedulerMetrics struct {
	totalRunsComplete *prometheus.CounterVec
	totalRunsActive   prometheus.Gauge

	runsComplete *prometheus.CounterVec
	runsActive   *prometheus.GaugeVec

	claimsComplete *prometheus.CounterVec
	claimsActive   prometheus.Gauge
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
	}
}

// StartRun adjusts the metrics to indicate a run is in progress for the given task ID.
func (sm *schedulerMetrics) StartRun(tid string) {
	sm.totalRunsActive.Inc()
	sm.runsActive.WithLabelValues(tid).Inc()
}

// FinishRun adjusts the metrics to indicate a run is no longer in progress for the given task ID.
func (sm *schedulerMetrics) FinishRun(tid string, succeeded bool) {
	status := statusString(succeeded)

	sm.totalRunsActive.Dec()
	sm.totalRunsComplete.WithLabelValues(status).Inc()

	sm.runsActive.WithLabelValues(tid).Dec()
	sm.runsComplete.WithLabelValues(tid, status).Inc()
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
