package scheduler

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type SchedulerMetrics struct {
	totalExecuteCalls   prometheus.Counter
	totalExecuteFailure prometheus.Counter
	scheduleCalls       prometheus.Counter
	scheduleFails       prometheus.Counter
	releaseCalls        prometheus.Counter

	executingTasks *executingTasks
	scheduleDelay  prometheus.Summary
	executeDelta   prometheus.Summary
}

type executingTasks struct {
	desc *prometheus.Desc
	ts   *TreeScheduler
}

func NewSchedulerMetrics(te *TreeScheduler) *SchedulerMetrics {
	const namespace = "task"
	const subsystem = "scheduler"

	return &SchedulerMetrics{
		totalExecuteCalls: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_execution_calls",
			Help:      "Total number of executions across all tasks.",
		}),
		scheduleCalls: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_schedule_calls",
			Help:      "Total number of schedule requests.",
		}),
		scheduleFails: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_schedule_fails",
			Help:      "Total number of schedule requests that fail to schedule.",
		}),

		totalExecuteFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_execute_failure",
			Help:      "Total number of times an execution has failed.",
		}),

		releaseCalls: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_release_calls",
			Help:      "Total number of release requests.",
		}),
		executingTasks: newExecutingTasks(te),
		scheduleDelay: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  namespace,
			Subsystem:  subsystem,
			Name:       "schedule_delay",
			Help:       "The duration between when a Item should be scheduled and when it is told to execute.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),

		executeDelta: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  namespace,
			Subsystem:  subsystem,
			Name:       "execute_delta",
			Help:       "The duration in seconds between a run starting and finishing.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (em *SchedulerMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		em.totalExecuteCalls,
		em.totalExecuteFailure,
		em.scheduleCalls,
		em.scheduleFails,
		em.releaseCalls,
		em.executingTasks,
		em.scheduleDelay,
		em.executeDelta,
	}
}

func (em *SchedulerMetrics) schedule(taskID ID) {
	em.scheduleCalls.Inc()
}

func (em *SchedulerMetrics) scheduleFail(taskID ID) {
	em.scheduleFails.Inc()
}

func (em *SchedulerMetrics) release(taskID ID) {
	em.releaseCalls.Inc()
}

func (em *SchedulerMetrics) reportScheduleDelay(d time.Duration) {
	em.scheduleDelay.Observe(d.Seconds())
}

func (em *SchedulerMetrics) reportExecution(err error, d time.Duration) {
	em.totalExecuteCalls.Inc()
	em.executeDelta.Observe(d.Seconds())
	if err != nil {
		em.totalExecuteFailure.Inc()
	}
}

func newExecutingTasks(ts *TreeScheduler) *executingTasks {
	return &executingTasks{
		desc: prometheus.NewDesc(
			"task_scheduler_current_execution",
			"Number of tasks currently being executed",
			nil,
			prometheus.Labels{},
		),
		ts: ts,
	}
}

// Describe returns all descriptions associated with the run collector.
func (r *executingTasks) Describe(ch chan<- *prometheus.Desc) {
	ch <- r.desc
}

// Collect returns the current state of all metrics of the run collector.
func (r *executingTasks) Collect(ch chan<- prometheus.Metric) {
	// TODO(docmerlin): fix this metric
	ch <- prometheus.MustNewConstMetric(r.desc, prometheus.GaugeValue, float64(len(r.ts.workchans)))
}
