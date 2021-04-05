package executor

import (
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/prometheus/client_golang/prometheus"
)

type ExecutorMetrics struct {
	totalRunsComplete    *prometheus.CounterVec
	activeRuns           prometheus.Collector
	queueDelta           *prometheus.SummaryVec
	runDuration          *prometheus.SummaryVec
	errorsCounter        *prometheus.CounterVec
	manualRunsCounter    *prometheus.CounterVec
	resumeRunsCounter    *prometheus.CounterVec
	unrecoverableCounter *prometheus.CounterVec
	runLatency           *prometheus.HistogramVec
}

type runCollector struct {
	totalRunsActive   *prometheus.Desc
	workersBusy       *prometheus.Desc
	promiseQueueUsage *prometheus.Desc
	ex                *Executor
}

func NewExecutorMetrics(ex *Executor) *ExecutorMetrics {
	const namespace = "task"
	const subsystem = "executor"

	return &ExecutorMetrics{
		totalRunsComplete: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_runs_complete",
			Help:      "Total number of runs completed across all tasks, split out by success or failure.",
		}, []string{"task_type", "status"}),

		activeRuns: NewRunCollector(ex),

		queueDelta: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  namespace,
			Subsystem:  subsystem,
			Name:       "run_queue_delta",
			Help:       "The duration in seconds between a run being due to start and actually starting.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"task_type", "taskID"}),

		runDuration: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  namespace,
			Subsystem:  subsystem,
			Name:       "run_duration",
			Help:       "The duration in seconds between a run starting and finishing.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"task_type", "taskID"}),

		errorsCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "errors_counter",
			Help:      "The number of errors thrown by the executor with the type of error (ex. Invalid, Internal, etc.)",
		}, []string{"task_type", "errorType"}),

		unrecoverableCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "unrecoverable_counter",
			Help:      "The number of errors by taskID that must be manually resolved or have the task deactivated.",
		}, []string{"taskID", "errorType"}),

		manualRunsCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "manual_runs_counter",
			Help:      "Total number of manual runs scheduled to run by task ID",
		}, []string{"taskID"}),

		resumeRunsCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "resume_runs_counter",
			Help:      "Total number of runs resumed by task ID",
		}, []string{"taskID"}),

		runLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "run_latency_seconds",
			Help:      "Records the latency between the time the run was due to run and the time the task started execution, by task type",
		}, []string{"task_type"}),
	}
}

// NewRunCollector returns a collector which exports influxdb process metrics.
func NewRunCollector(ex *Executor) prometheus.Collector {
	return &runCollector{
		workersBusy: prometheus.NewDesc(
			"task_executor_workers_busy",
			"Percent of total available workers that are currently busy",
			nil,
			prometheus.Labels{},
		),
		totalRunsActive: prometheus.NewDesc(
			"task_executor_total_runs_active",
			"Total number of workers currently running tasks",
			nil,
			prometheus.Labels{},
		),
		promiseQueueUsage: prometheus.NewDesc(
			"task_executor_promise_queue_usage",
			"Percent of the promise queue that is currently full",
			nil,
			prometheus.Labels{},
		),
		ex: ex,
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (em *ExecutorMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		em.totalRunsComplete,
		em.activeRuns,
		em.queueDelta,
		em.errorsCounter,
		em.runDuration,
		em.manualRunsCounter,
		em.resumeRunsCounter,
		em.unrecoverableCounter,
		em.runLatency,
	}
}

// StartRun store the delta time between when a run is due to start and actually starting.
func (em *ExecutorMetrics) StartRun(task *taskmodel.Task, queueDelta time.Duration, runLatency time.Duration) {
	em.queueDelta.WithLabelValues(task.Type, "all").Observe(queueDelta.Seconds())
	em.queueDelta.WithLabelValues("", task.ID.String()).Observe(queueDelta.Seconds())

	// schedule interval duration = (time task was scheduled to run) - (time it actually ran)
	em.runLatency.WithLabelValues(task.Type).Observe(runLatency.Seconds())
}

// FinishRun adjusts the metrics to indicate a run is no longer in progress for the given task ID.
func (em *ExecutorMetrics) FinishRun(task *taskmodel.Task, status taskmodel.RunStatus, runDuration time.Duration) {
	em.totalRunsComplete.WithLabelValues(task.Type, status.String()).Inc()

	em.runDuration.WithLabelValues(task.Type, "all").Observe(runDuration.Seconds())
	em.runDuration.WithLabelValues("", task.ID.String()).Observe(runDuration.Seconds())
}

// LogError increments the count of errors by error code.
func (em *ExecutorMetrics) LogError(taskType string, err error) {
	switch e := err.(type) {
	case *errors.Error:
		em.errorsCounter.WithLabelValues(taskType, e.Code).Inc()
	default:
		em.errorsCounter.WithLabelValues(taskType, "unknown").Inc()
	}
}

// LogUnrecoverableError increments the count of unrecoverable errors, which require admin intervention to resolve or deactivate
// This count is separate from the errors count so that the errors metric can be used to identify only internal, rather than user errors
// and so that unrecoverable errors can be quickly identified for deactivation
func (em *ExecutorMetrics) LogUnrecoverableError(taskID platform.ID, err error) {
	switch e := err.(type) {
	case *errors.Error:
		em.unrecoverableCounter.WithLabelValues(taskID.String(), e.Code).Inc()
	default:
		em.unrecoverableCounter.WithLabelValues(taskID.String(), "unknown").Inc()
	}
}

// Describe returns all descriptions associated with the run collector.
func (r *runCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- r.workersBusy
	ch <- r.promiseQueueUsage
	ch <- r.totalRunsActive
}

// Collect returns the current state of all metrics of the run collector.
func (r *runCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(r.workersBusy, prometheus.GaugeValue, r.ex.WorkersBusy())

	ch <- prometheus.MustNewConstMetric(r.promiseQueueUsage, prometheus.GaugeValue, r.ex.PromiseQueueUsage())

	ch <- prometheus.MustNewConstMetric(r.totalRunsActive, prometheus.GaugeValue, float64(r.ex.RunsActive()))
}
