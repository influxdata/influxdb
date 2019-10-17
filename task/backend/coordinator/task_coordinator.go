package coordinator

import (
	"context"
	"errors"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/backend/executor"
	"github.com/influxdata/influxdb/task/backend/middleware"
	"github.com/influxdata/influxdb/task/backend/scheduler"
	"go.uber.org/zap"
)

var _ middleware.Coordinator = (*Coordinator)(nil)
var _ Executor = (*executor.TaskExecutor)(nil)

// DefaultLimit is the maximum number of tasks that a given taskd server can own
const DefaultLimit = 1000

// Executor is an abstraction of the task executor with only the functions needed by the coordinator
type Executor interface {
	ManualRun(ctx context.Context, id influxdb.ID, runID influxdb.ID) (executor.Promise, error)
	Cancel(ctx context.Context, runID influxdb.ID) error
}

// TaskCoordinator (temporary name) is the intermediary between the scheduling/executing system and the rest of the task system
type TaskCoordinator struct {
	logger *zap.Logger
	sch    scheduler.Scheduler
	ex     Executor

	limit int
}

type CoordinatorOption func(*TaskCoordinator)

// SchedulableTask is a wrapper around the Task struct, giving it methods to make it compatible with the Scheduler
type SchedulableTask struct {
	*influxdb.Task
}

func (t SchedulableTask) ID() scheduler.ID {
	return scheduler.ID(t.Task.ID)
}

// Schedule takes the time a Task is scheduled for and returns a Schedule object
func (t SchedulableTask) Schedule() scheduler.Schedule {
	// TODO (al): use new scheduler's NewSchedule method
	return scheduler.Schedule{}
}

// Offset returns a time.Duration for the Task's offset property
func (t SchedulableTask) Offset() time.Duration {
	offset, _ := t.OffsetDuration()
	return offset
}

// LastScheduled parses the task's LatestCompleted value as a Time object
func (t SchedulableTask) LastScheduled() time.Time {
	tm, _ := t.LatestCompletedTime()
	return tm
}

func WithLimitOpt(i int) CoordinatorOption {
	return func(c *TaskCoordinator) {
		c.limit = i
	}
}

// NewSchedulableTask transforms an influxdb task to a schedulable task type
func NewSchedulableTask(task *influxdb.Task) (SchedulableTask, error) {
	if offset, err := task.OffsetDuration(); offset != time.Duration(0) && err != nil {
		return SchedulableTask{}, errors.New("could not create schedulable task: offset duration could not be parsed")
	}

	if _, err := task.LatestCompletedTime(); err != nil {
		return SchedulableTask{}, errors.New("could not create schedulable task: latest completed time could not be parsed")
	}
	t := SchedulableTask{task}

	return t, nil
}

func NewCoordinator(logger *zap.Logger, scheduler scheduler.Scheduler, executor Executor, opts ...CoordinatorOption) *TaskCoordinator {
	c := &TaskCoordinator{
		logger: logger,
		sch:    scheduler,
		ex:     executor,
		limit:  DefaultLimit,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// TaskCreated asks the Scheduler to schedule the newly created task
func (c *TaskCoordinator) TaskCreated(ctx context.Context, task *influxdb.Task) error {
	t, err := NewSchedulableTask(task)

	if err != nil {
		return err
	}
	// func new schedulable task
	// catch errors from offset and last scheduled
	if err = c.sch.Schedule(t); err != nil {
		return err
	}

	return nil
}

// TaskUpdated releases the task if it is being disabled, and schedules it otherwise
func (c *TaskCoordinator) TaskUpdated(ctx context.Context, from, to *influxdb.Task) error {
	sid := scheduler.ID(to.ID)
	t, err := NewSchedulableTask(to)
	if err != nil {
		return err
	}

	// if disabling the task, release it before schedule update
	if to.Status != from.Status && to.Status == string(backend.TaskInactive) {
		if err := c.sch.Release(sid); err != nil && err != influxdb.ErrTaskNotClaimed {
			return err
		}
	} else {
		if err := c.sch.Schedule(t); err != nil {
			return err
		}
	}

	return nil
}

//TaskDeleted asks the Scheduler to release the deleted task
func (c *TaskCoordinator) TaskDeleted(ctx context.Context, id influxdb.ID) error {
	tid := scheduler.ID(id)
	if err := c.sch.Release(tid); err != nil && err != influxdb.ErrTaskNotClaimed {
		return err
	}

	return nil
}

// RunCancelled speaks directly to the executor to cancel a task run
// TODO(docmerlin): remove the middle variable and refactor the interface when we delete the old scheduler
func (c *TaskCoordinator) RunCancelled(ctx context.Context, _, runID influxdb.ID) error {
	err := c.ex.Cancel(ctx, runID)

	return err
}

// RunRetried speaks directly to the executor to re-try a task run immediately
func (c *TaskCoordinator) RunRetried(ctx context.Context, task *influxdb.Task, run *influxdb.Run) error {
	promise, err := c.ex.ManualRun(ctx, task.ID, run.ID)
	if err != nil {
		return influxdb.ErrRunExecutionError(err)
	}

	<-promise.Done()
	if err = promise.Error(); err != nil {
		return err
	}

	return nil
}

// RunForced speaks directly to the Executor to run a task immediately
func (c *TaskCoordinator) RunForced(ctx context.Context, task *influxdb.Task, run *influxdb.Run) error {
	promise, err := c.ex.ManualRun(ctx, task.ID, run.ID)
	if err != nil {
		return influxdb.ErrRunExecutionError(err)
	}

	<-promise.Done()
	if err = promise.Error(); err != nil {
		return err
	}

	return nil
}
