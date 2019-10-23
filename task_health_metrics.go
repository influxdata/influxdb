package influxdb

import (
	"github.com/prometheus/client_golang/prometheus"
)

// goal: find out if there are any tasks that are active but not actually running
// for each task ID, record the difference between
// the time it was last expected to be completed
// and the time it was actually last completed

type TaskHealthMetrics struct {
	activeTaskQueueDeltas *prometheus.SummaryVec
}

func NewTaskHealthMetrics(ths *TaskLatencyMetricsService) *TaskHealthMetrics {
	const namespace = "task"
	const subsystem = "health"

	return &TaskHealthMetrics{
		activeTaskQueueDeltas: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  namespace,
			Subsystem:  subsystem,
			Name:       "active_task_queue_deltas",
			Help:       "Difference between latest completed run and latest expected run of all active tasks by task type and ID.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"task_type", "taskID"}),
	}
}

func (thm *TaskHealthMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		thm.activeTaskQueueDeltas,
	}
}

func (thm *TaskHealthMetrics) CalculateQueueDeltas() {

}
