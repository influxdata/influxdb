package mock

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/task/backend"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

var _ taskmodel.TaskService = (*TaskService)(nil)
var _ backend.TaskControlService = (*TaskControlService)(nil)

type TaskService struct {
	FindTaskByIDFn    func(context.Context, platform.ID) (*taskmodel.Task, error)
	FindTaskByIDCalls SafeCount
	FindTasksFn       func(context.Context, taskmodel.TaskFilter) ([]*taskmodel.Task, int, error)
	FindTasksCalls    SafeCount
	CreateTaskFn      func(context.Context, taskmodel.TaskCreate) (*taskmodel.Task, error)
	CreateTaskCalls   SafeCount
	UpdateTaskFn      func(context.Context, platform.ID, taskmodel.TaskUpdate) (*taskmodel.Task, error)
	UpdateTaskCalls   SafeCount
	DeleteTaskFn      func(context.Context, platform.ID) error
	DeleteTaskCalls   SafeCount
	FindLogsFn        func(context.Context, taskmodel.LogFilter) ([]*taskmodel.Log, int, error)
	FindLogsCalls     SafeCount
	FindRunsFn        func(context.Context, taskmodel.RunFilter) ([]*taskmodel.Run, int, error)
	FindRunsCalls     SafeCount
	FindRunByIDFn     func(context.Context, platform.ID, platform.ID) (*taskmodel.Run, error)
	FindRunByIDCalls  SafeCount
	CancelRunFn       func(context.Context, platform.ID, platform.ID) error
	CancelRunCalls    SafeCount
	RetryRunFn        func(context.Context, platform.ID, platform.ID) (*taskmodel.Run, error)
	RetryRunCalls     SafeCount
	ForceRunFn        func(context.Context, platform.ID, int64) (*taskmodel.Run, error)
	ForceRunCalls     SafeCount
}

func NewTaskService() *TaskService {
	return &TaskService{
		FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
			return nil, nil
		},
		FindTasksFn: func(ctx context.Context, f taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
			return nil, 0, nil
		},
		CreateTaskFn: func(ctx context.Context, taskCreate taskmodel.TaskCreate) (*taskmodel.Task, error) {
			return nil, nil
		},
		UpdateTaskFn: func(ctx context.Context, id platform.ID, update taskmodel.TaskUpdate) (*taskmodel.Task, error) {
			return nil, nil
		},
		DeleteTaskFn: func(ctx context.Context, id platform.ID) error {
			return nil
		},
		FindLogsFn: func(ctx context.Context, f taskmodel.LogFilter) ([]*taskmodel.Log, int, error) {
			return nil, 0, nil
		},
		FindRunsFn: func(ctx context.Context, f taskmodel.RunFilter) ([]*taskmodel.Run, int, error) {
			return nil, 0, nil
		},
		FindRunByIDFn: func(ctx context.Context, id platform.ID, id2 platform.ID) (*taskmodel.Run, error) {
			return nil, nil
		},
		CancelRunFn: func(ctx context.Context, id platform.ID, id2 platform.ID) error {
			return nil
		},
		RetryRunFn: func(ctx context.Context, id platform.ID, id2 platform.ID) (*taskmodel.Run, error) {
			return nil, nil
		},
		ForceRunFn: func(ctx context.Context, id platform.ID, i int64) (*taskmodel.Run, error) {
			return nil, nil
		},
	}
}

func (s *TaskService) FindTaskByID(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
	defer s.FindTaskByIDCalls.IncrFn()()
	return s.FindTaskByIDFn(ctx, id)
}

func (s *TaskService) FindTasks(ctx context.Context, filter taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
	defer s.FindTasksCalls.IncrFn()()
	return s.FindTasksFn(ctx, filter)
}

func (s *TaskService) CreateTask(ctx context.Context, t taskmodel.TaskCreate) (*taskmodel.Task, error) {
	defer s.CreateTaskCalls.IncrFn()()
	return s.CreateTaskFn(ctx, t)
}

func (s *TaskService) UpdateTask(ctx context.Context, id platform.ID, upd taskmodel.TaskUpdate) (*taskmodel.Task, error) {
	defer s.UpdateTaskCalls.IncrFn()()
	return s.UpdateTaskFn(ctx, id, upd)
}

func (s *TaskService) DeleteTask(ctx context.Context, id platform.ID) error {
	defer s.DeleteTaskCalls.IncrFn()()
	return s.DeleteTaskFn(ctx, id)
}

func (s *TaskService) FindLogs(ctx context.Context, filter taskmodel.LogFilter) ([]*taskmodel.Log, int, error) {
	defer s.FindLogsCalls.IncrFn()()
	return s.FindLogsFn(ctx, filter)
}

func (s *TaskService) FindRuns(ctx context.Context, filter taskmodel.RunFilter) ([]*taskmodel.Run, int, error) {
	defer s.FindRunsCalls.IncrFn()()
	return s.FindRunsFn(ctx, filter)
}

func (s *TaskService) FindRunByID(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	defer s.FindRunByIDCalls.IncrFn()()
	return s.FindRunByIDFn(ctx, taskID, runID)
}

func (s *TaskService) CancelRun(ctx context.Context, taskID, runID platform.ID) error {
	defer s.CancelRunCalls.IncrFn()()
	return s.CancelRunFn(ctx, taskID, runID)
}

func (s *TaskService) RetryRun(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	defer s.RetryRunCalls.IncrFn()()
	return s.RetryRunFn(ctx, taskID, runID)
}

func (s *TaskService) ForceRun(ctx context.Context, taskID platform.ID, scheduledFor int64) (*taskmodel.Run, error) {
	defer s.ForceRunCalls.IncrFn()()
	return s.ForceRunFn(ctx, taskID, scheduledFor)
}

type TaskControlService struct {
	CreateRunFn        func(ctx context.Context, taskID platform.ID, scheduledFor time.Time, runAt time.Time) (*taskmodel.Run, error)
	CurrentlyRunningFn func(ctx context.Context, taskID platform.ID) ([]*taskmodel.Run, error)
	ManualRunsFn       func(ctx context.Context, taskID platform.ID) ([]*taskmodel.Run, error)
	StartManualRunFn   func(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error)
	FinishRunFn        func(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error)
	UpdateRunStateFn   func(ctx context.Context, taskID, runID platform.ID, when time.Time, state taskmodel.RunStatus) error
	AddRunLogFn        func(ctx context.Context, taskID, runID platform.ID, when time.Time, log string) error
}

func (tcs *TaskControlService) CreateRun(ctx context.Context, taskID platform.ID, scheduledFor time.Time, runAt time.Time) (*taskmodel.Run, error) {
	return tcs.CreateRunFn(ctx, taskID, scheduledFor, runAt)
}
func (tcs *TaskControlService) CurrentlyRunning(ctx context.Context, taskID platform.ID) ([]*taskmodel.Run, error) {
	return tcs.CurrentlyRunningFn(ctx, taskID)
}
func (tcs *TaskControlService) ManualRuns(ctx context.Context, taskID platform.ID) ([]*taskmodel.Run, error) {
	return tcs.ManualRunsFn(ctx, taskID)
}
func (tcs *TaskControlService) StartManualRun(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	return tcs.StartManualRunFn(ctx, taskID, runID)
}
func (tcs *TaskControlService) FinishRun(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	return tcs.FinishRunFn(ctx, taskID, runID)
}
func (tcs *TaskControlService) UpdateRunState(ctx context.Context, taskID, runID platform.ID, when time.Time, state taskmodel.RunStatus) error {
	return tcs.UpdateRunStateFn(ctx, taskID, runID, when, state)
}
func (tcs *TaskControlService) AddRunLog(ctx context.Context, taskID, runID platform.ID, when time.Time, log string) error {
	return tcs.AddRunLogFn(ctx, taskID, runID, when, log)
}
