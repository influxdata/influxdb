package backend

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"go.uber.org/zap"
)

var now = func() time.Time {
	return time.Now().UTC()
}

// TaskService is a type on which tasks can be listed
type TaskService interface {
	FindTasks(context.Context, taskmodel.TaskFilter) ([]*taskmodel.Task, int, error)
	UpdateTask(context.Context, platform.ID, taskmodel.TaskUpdate) (*taskmodel.Task, error)
}

// Coordinator is a type with a single method which
// is called when a task has been created
type Coordinator interface {
	TaskCreated(context.Context, *taskmodel.Task) error
}

// NotifyCoordinatorOfExisting lists all tasks by the provided task service and for
// each task it calls the provided coordinators task created method
func NotifyCoordinatorOfExisting(ctx context.Context, log *zap.Logger, ts TaskService, coord Coordinator) error {
	// If we missed a Create Action
	tasks, _, err := ts.FindTasks(ctx, taskmodel.TaskFilter{})
	if err != nil {
		return err
	}

	latestCompleted := now()
	for len(tasks) > 0 {
		for _, task := range tasks {
			if task.Status != string(taskmodel.TaskActive) {
				continue
			}

			task, err := ts.UpdateTask(context.Background(), task.ID, taskmodel.TaskUpdate{
				LatestCompleted: &latestCompleted,
				LatestScheduled: &latestCompleted,
			})
			if err != nil {
				log.Error("Failed to set latestCompleted", zap.Error(err))
				continue
			}

			coord.TaskCreated(ctx, task)
		}

		tasks, _, err = ts.FindTasks(ctx, taskmodel.TaskFilter{
			After: &tasks[len(tasks)-1].ID,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

type TaskResumer func(ctx context.Context, id platform.ID, runID platform.ID) error

// TaskNotifyCoordinatorOfExisting lists all tasks by the provided task service and for
// each task it calls the provided coordinators task created method
// TODO(docmerlin): this is temporary untill the executor queue is persistent
func TaskNotifyCoordinatorOfExisting(ctx context.Context, ts TaskService, tcs TaskControlService, coord Coordinator, exec TaskResumer, log *zap.Logger) error {
	// If we missed a Create Action
	tasks, _, err := ts.FindTasks(ctx, taskmodel.TaskFilter{})
	if err != nil {
		return err
	}

	latestCompleted := now()
	for len(tasks) > 0 {
		for _, task := range tasks {
			if task.Status != string(taskmodel.TaskActive) {
				continue
			}

			task, err := ts.UpdateTask(context.Background(), task.ID, taskmodel.TaskUpdate{
				LatestCompleted: &latestCompleted,
				LatestScheduled: &latestCompleted,
			})
			if err != nil {
				log.Error("Failed to set latestCompleted", zap.Error(err))
				continue
			}

			coord.TaskCreated(ctx, task)
			runs, err := tcs.CurrentlyRunning(ctx, task.ID)
			if err != nil {
				return err
			}
			for i := range runs {
				if err := exec(ctx, runs[i].TaskID, runs[i].ID); err != nil {
					return err
				}
			}
		}

		tasks, _, err = ts.FindTasks(ctx, taskmodel.TaskFilter{
			After: &tasks[len(tasks)-1].ID,
		})
		if err != nil {
			return err
		}
	}

	return nil
}
