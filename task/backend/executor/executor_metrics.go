package executor

import (
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/prometheus/client_golang/prometheus"
)

type ExecutorMetrics struct {
	totalRunsComplete *prometheus.CounterVec
	activeRuns        prometheus.Collector
	queueDelta        prometheus.Summary
	runDuration       prometheus.Summary
	errorsCounter     prometheus.Counter
	manualRunsCounter *prometheus.CounterVec
	resumeRunsCounter *prometheus.CounterVec
}

type runCollector struct {
	totalRunsActive   *prometheus.Desc
	workersBusy       *prometheus.Desc
	promiseQueueUsage *prometheus.Desc
	te                *TaskExecutor
}

func NewExecutorMetrics(te *TaskExecutor) *ExecutorMetrics {
	const namespace = "task"
	const subsystem = "executor"

	return &ExecutorMetrics{
		totalRunsComplete: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_runs_complete",
			Help:      "Total number of runs completed across all tasks, split out by success or failure.",
		}, []string{"status"}),

		activeRuns: NewRunCollector(te),

		queueDelta: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  namespace,
			Subsystem:  subsystem,
			Name:       "run_queue_delta",
			Help:       "The duration in seconds between a run being due to start and actually starting.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),

		runDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  namespace,
			Subsystem:  subsystem,
			Name:       "run_duration",
			Help:       "The duration in seconds between a run starting and finishing.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}),

		errorsCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "errors_counter",
			Help:      "The number of errors thrown by the executor.",
		}),

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
	}
}

// NewRunCollector returns a collector which exports influxdb process metrics.
func NewRunCollector(te *TaskExecutor) prometheus.Collector {
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
		te: te,
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
	}
}

// StartRun store the delta time between when a run is due to start and actually starting.
func (em *ExecutorMetrics) StartRun(taskID influxdb.ID, queueDelta time.Duration) {
	em.queueDelta.Observe(queueDelta.Seconds())
}

// FinishRun adjusts the metrics to indicate a run is no longer in progress for the given task ID.
func (em *ExecutorMetrics) FinishRun(taskID influxdb.ID, status backend.RunStatus, runDuration time.Duration) {
	em.totalRunsComplete.WithLabelValues(status.String()).Inc()

	em.runDuration.Observe(runDuration.Seconds())
}

// LogError increments the count of errors.
func (em *ExecutorMetrics) LogError() {
	em.errorsCounter.Inc()
}

// Describe returns all descriptions associated with the run collector.
func (r *runCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- r.workersBusy
	ch <- r.promiseQueueUsage
	ch <- r.totalRunsActive
}

// Collect returns the current state of all metrics of the run collector.
func (r *runCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(r.workersBusy, prometheus.GaugeValue, r.te.WorkersBusy())

	ch <- prometheus.MustNewConstMetric(r.promiseQueueUsage, prometheus.GaugeValue, r.te.PromiseQueueUsage())

	ch <- prometheus.MustNewConstMetric(r.totalRunsActive, prometheus.GaugeValue, float64(r.te.RunsActive()))
}
