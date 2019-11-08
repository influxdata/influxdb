package influxdb

import (
	"context"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	namespace = "task_monitoring"
	subsystem = "summary"
)

// FindTasksService implements the FindTasks method of a TaskService for the TaskHealthService to use
type FindTasksService interface {
	FindTasks(ctx context.Context, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error)
}

// taskLatency is responsible for monitoring the latency of active tasks
type taskLatency struct {
	log          *zap.Logger
	fts          FindTasksService
	delayedTasks *prometheus.SummaryVec
}

// newTaskLatency takes a FindTasksService and creates a new taskLatency
func newTaskLatency(ctx context.Context, fts FindTasksService, log *zap.Logger) *taskLatency {
	return &taskLatency{
		log: log,
		fts: fts,
		delayedTasks: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  namespace,
			Subsystem:  subsystem,
			Name:       "active_task_schedule_latency",
			Help:       "Time since active tasks were last scheduled, in minutes",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		}, []string{"task_type", "taskID"}),
	}
}

// Update task latency metrics with active tasks with LastScheduled values in the past
func (tl *taskLatency) Update(ctx context.Context) {
	active := true
	filter := influxdb.TaskFilter{Active: &active}

	tasks, _, err := tl.fts.FindTasks(ctx, filter)
	if err != nil {
		tl.log.Error("Could not find tasks: ", zap.Error(err))
	}

	for _, task := range tasks {
		if task.LatestScheduled.Before(time.Now().UTC()) {
			tl.delayedTasks.WithLabelValues(task.Type, task.ID.String()).Observe(time.Since(task.LatestScheduled).Minutes())
		}
	}
}

func (tl *taskLatency) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		tl.delayedTasks,
	}
}
