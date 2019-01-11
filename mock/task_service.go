package mock

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

var _ platform.TaskService = &TaskService{}

type TaskService struct {
	FindTaskByIDFn func(context.Context, platform.ID) (*platform.Task, error)
	FindTasksFn    func(context.Context, platform.TaskFilter) ([]*platform.Task, int, error)
	CreateTaskFn   func(context.Context, *platform.Task) error
	UpdateTaskFn   func(context.Context, platform.ID, platform.TaskUpdate) (*platform.Task, error)
	DeleteTaskFn   func(context.Context, platform.ID) error
	FindLogsFn     func(context.Context, platform.LogFilter) ([]*platform.Log, int, error)
	FindRunsFn     func(context.Context, platform.RunFilter) ([]*platform.Run, int, error)
	FindRunByIDFn  func(context.Context, platform.ID, platform.ID) (*platform.Run, error)
	CancelRunFn    func(context.Context, platform.ID, platform.ID) error
	RetryRunFn     func(context.Context, platform.ID, platform.ID) (*platform.Run, error)
	ForceRunFn     func(context.Context, platform.ID, int64) (*platform.Run, error)
}

func (s *TaskService) FindTaskByID(ctx context.Context, id platform.ID) (*platform.Task, error) {
	return s.FindTaskByIDFn(ctx, id)
}

func (s *TaskService) FindTasks(ctx context.Context, filter platform.TaskFilter) ([]*platform.Task, int, error) {
	return s.FindTasksFn(ctx, filter)
}

func (s *TaskService) CreateTask(ctx context.Context, t *platform.Task) error {
	return s.CreateTaskFn(ctx, t)
}

func (s *TaskService) UpdateTask(ctx context.Context, id platform.ID, upd platform.TaskUpdate) (*platform.Task, error) {
	return s.UpdateTaskFn(ctx, id, upd)
}

func (s *TaskService) DeleteTask(ctx context.Context, id platform.ID) error {
	return s.DeleteTaskFn(ctx, id)
}

func (s *TaskService) FindLogs(ctx context.Context, filter platform.LogFilter) ([]*platform.Log, int, error) {
	return s.FindLogsFn(ctx, filter)
}

func (s *TaskService) FindRuns(ctx context.Context, filter platform.RunFilter) ([]*platform.Run, int, error) {
	return s.FindRunsFn(ctx, filter)
}

func (s *TaskService) FindRunByID(ctx context.Context, taskID, runID platform.ID) (*platform.Run, error) {
	return s.FindRunByIDFn(ctx, taskID, runID)
}

func (s *TaskService) CancelRun(ctx context.Context, taskID, runID platform.ID) error {
	return s.CancelRunFn(ctx, taskID, runID)
}

func (s *TaskService) RetryRun(ctx context.Context, taskID, runID platform.ID) (*platform.Run, error) {
	return s.RetryRunFn(ctx, taskID, runID)
}

func (s *TaskService) ForceRun(ctx context.Context, taskID platform.ID, scheduledFor int64) (*platform.Run, error) {
	return s.ForceRunFn(ctx, taskID, scheduledFor)
}
