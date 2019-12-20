package mock

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/task/backend"
)

var _ influxdb.TaskService = (*TaskService)(nil)
var _ backend.TaskControlService = (*TaskControlService)(nil)

type TaskService struct {
	FindTaskByIDFn func(context.Context, influxdb.ID) (*influxdb.Task, error)
	FindTasksFn    func(context.Context, influxdb.TaskFilter) ([]*influxdb.Task, int, error)
	CreateTaskFn   func(context.Context, influxdb.TaskCreate) (*influxdb.Task, error)
	UpdateTaskFn   func(context.Context, influxdb.ID, influxdb.TaskUpdate) (*influxdb.Task, error)
	DeleteTaskFn   func(context.Context, influxdb.ID) error
	FindLogsFn     func(context.Context, influxdb.LogFilter) ([]*influxdb.Log, int, error)
	FindRunsFn     func(context.Context, influxdb.RunFilter) ([]*influxdb.Run, int, error)
	FindRunByIDFn  func(context.Context, influxdb.ID, influxdb.ID) (*influxdb.Run, error)
	CancelRunFn    func(context.Context, influxdb.ID, influxdb.ID) error
	RetryRunFn     func(context.Context, influxdb.ID, influxdb.ID) (*influxdb.Run, error)
	ForceRunFn     func(context.Context, influxdb.ID, int64) (*influxdb.Run, error)
}

func (s *TaskService) FindTaskByID(ctx context.Context, id influxdb.ID) (*influxdb.Task, error) {
	return s.FindTaskByIDFn(ctx, id)
}

func (s *TaskService) FindTasks(ctx context.Context, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	return s.FindTasksFn(ctx, filter)
}

func (s *TaskService) CreateTask(ctx context.Context, t influxdb.TaskCreate) (*influxdb.Task, error) {
	return s.CreateTaskFn(ctx, t)
}

func (s *TaskService) UpdateTask(ctx context.Context, id influxdb.ID, upd influxdb.TaskUpdate) (*influxdb.Task, error) {
	return s.UpdateTaskFn(ctx, id, upd)
}

func (s *TaskService) DeleteTask(ctx context.Context, id influxdb.ID) error {
	return s.DeleteTaskFn(ctx, id)
}

func (s *TaskService) FindLogs(ctx context.Context, filter influxdb.LogFilter) ([]*influxdb.Log, int, error) {
	return s.FindLogsFn(ctx, filter)
}

func (s *TaskService) FindRuns(ctx context.Context, filter influxdb.RunFilter) ([]*influxdb.Run, int, error) {
	return s.FindRunsFn(ctx, filter)
}

func (s *TaskService) FindRunByID(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	return s.FindRunByIDFn(ctx, taskID, runID)
}

func (s *TaskService) CancelRun(ctx context.Context, taskID, runID influxdb.ID) error {
	return s.CancelRunFn(ctx, taskID, runID)
}

func (s *TaskService) RetryRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	return s.RetryRunFn(ctx, taskID, runID)
}

func (s *TaskService) ForceRun(ctx context.Context, taskID influxdb.ID, scheduledFor int64) (*influxdb.Run, error) {
	return s.ForceRunFn(ctx, taskID, scheduledFor)
}

type TaskControlService struct {
	CreateNextRunFn    func(ctx context.Context, taskID influxdb.ID, now int64) (backend.RunCreation, error)
	NextDueRunFn       func(ctx context.Context, taskID influxdb.ID) (int64, error)
	CreateRunFn        func(ctx context.Context, taskID influxdb.ID, scheduledFor time.Time, runAt time.Time) (*influxdb.Run, error)
	CurrentlyRunningFn func(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error)
	ManualRunsFn       func(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error)
	StartManualRunFn   func(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error)
	FinishRunFn        func(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error)
	UpdateRunStateFn   func(ctx context.Context, taskID, runID influxdb.ID, when time.Time, state backend.RunStatus) error
	AddRunLogFn        func(ctx context.Context, taskID, runID influxdb.ID, when time.Time, log string) error
}

func (tcs *TaskControlService) CreateNextRun(ctx context.Context, taskID influxdb.ID, now int64) (backend.RunCreation, error) {
	return tcs.CreateNextRunFn(ctx, taskID, now)
}
func (tcs *TaskControlService) NextDueRun(ctx context.Context, taskID influxdb.ID) (int64, error) {
	return tcs.NextDueRunFn(ctx, taskID)
}
func (tcs *TaskControlService) CreateRun(ctx context.Context, taskID influxdb.ID, scheduledFor time.Time, runAt time.Time) (*influxdb.Run, error) {
	return tcs.CreateRunFn(ctx, taskID, scheduledFor, runAt)
}
func (tcs *TaskControlService) CurrentlyRunning(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error) {
	return tcs.CurrentlyRunningFn(ctx, taskID)
}
func (tcs *TaskControlService) ManualRuns(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error) {
	return tcs.ManualRunsFn(ctx, taskID)
}
func (tcs *TaskControlService) StartManualRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	return tcs.StartManualRunFn(ctx, taskID, runID)
}
func (tcs *TaskControlService) FinishRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	return tcs.FinishRunFn(ctx, taskID, runID)
}
func (tcs *TaskControlService) UpdateRunState(ctx context.Context, taskID, runID influxdb.ID, when time.Time, state backend.RunStatus) error {
	return tcs.UpdateRunStateFn(ctx, taskID, runID, when, state)
}
func (tcs *TaskControlService) AddRunLog(ctx context.Context, taskID, runID influxdb.ID, when time.Time, log string) error {
	return tcs.AddRunLogFn(ctx, taskID, runID, when, log)
}
