package coordinator

import (
	"context"
	"fmt"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend"
	"go.uber.org/zap"
)

type Coordinator struct {
	platform.TaskService

	logger *zap.Logger
	sch    backend.Scheduler

	limit         int
	claimExisting bool
}

type Option func(*Coordinator)

func WithLimit(i int) Option {
	return func(c *Coordinator) {
		c.limit = i
	}
}

// WithoutExistingTasks allows us to skip claiming tasks already in the system.
func WithoutExistingTasks() Option {
	return func(c *Coordinator) {
		c.claimExisting = false
	}
}

func New(logger *zap.Logger, scheduler backend.Scheduler, ts platform.TaskService, opts ...Option) *Coordinator {
	c := &Coordinator{
		logger:        logger,
		sch:           scheduler,
		TaskService:   ts,
		limit:         1000,
		claimExisting: true,
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.claimExisting {
		go c.claimExistingTasks()
	}

	return c
}

// claimExistingTasks is called on startup to claim all tasks in the store.
func (c *Coordinator) claimExistingTasks() {
	tasks, _, err := c.TaskService.FindTasks(context.Background(), platform.TaskFilter{})
	if err != nil {
		return
	}
	newLatestCompleted := time.Now().UTC().Format(time.RFC3339)
	for len(tasks) > 0 {
		for _, task := range tasks {

			task, err := c.TaskService.UpdateTask(context.Background(), task.ID, platform.TaskUpdate{LatestCompleted: &newLatestCompleted})
			if err != nil {
				c.logger.Error("failed to set latestCompleted", zap.Error(err))
			}

			if task.Status != string(backend.TaskActive) {
				// Don't claim inactive tasks at startup.
				continue
			}

			// I may need a context with an auth here
			if err := c.sch.ClaimTask(context.Background(), task); err != nil {
				c.logger.Error("failed claim task", zap.Error(err))
				continue
			}
		}
		tasks, _, err = c.TaskService.FindTasks(context.Background(), platform.TaskFilter{
			After: &tasks[len(tasks)-1].ID,
		})
		if err != nil {
			c.logger.Error("failed list additional tasks", zap.Error(err))
			return
		}
	}
}

func (c *Coordinator) CreateTask(ctx context.Context, t platform.TaskCreate) (*platform.Task, error) {
	task, err := c.TaskService.CreateTask(ctx, t)
	if err != nil {
		return task, err
	}

	if err := c.sch.ClaimTask(ctx, task); err != nil {
		delErr := c.TaskService.DeleteTask(ctx, task.ID)
		if delErr != nil {
			return task, fmt.Errorf("schedule task failed: %s\n\tcleanup also failed: %s", err, delErr)
		}
		return task, err
	}

	return task, nil
}

func (c *Coordinator) UpdateTask(ctx context.Context, id platform.ID, upd platform.TaskUpdate) (*platform.Task, error) {
	oldTask, err := c.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return nil, err
	}

	task, err := c.TaskService.UpdateTask(ctx, id, upd)
	if err != nil {
		return task, err
	}

	// If disabling the task, do so before modifying the script.
	if task.Status != oldTask.Status && task.Status == string(backend.TaskInactive) {
		if err := c.sch.ReleaseTask(id); err != nil && err != platform.ErrTaskNotClaimed {
			return task, err
		}
	}

	if err := c.sch.UpdateTask(ctx, task); err != nil && err != platform.ErrTaskNotClaimed {
		return task, err
	}

	// If enabling the task, claim it after modifying the script.
	if task.Status != oldTask.Status && task.Status == string(backend.TaskActive) {
		// don't catch up on all the missed task runs while disabled
		newLatestCompleted := c.sch.Now().UTC().Format(time.RFC3339)
		task, err := c.TaskService.UpdateTask(ctx, task.ID, platform.TaskUpdate{LatestCompleted: &newLatestCompleted})
		if err != nil {
			return task, err
		}

		if err := c.sch.ClaimTask(ctx, task); err != nil && err != platform.ErrTaskAlreadyClaimed {
			return task, err
		}
	}

	return task, nil
}

func (c *Coordinator) DeleteTask(ctx context.Context, id platform.ID) error {
	if err := c.sch.ReleaseTask(id); err != nil && err != platform.ErrTaskNotClaimed {
		return err
	}

	return c.TaskService.DeleteTask(ctx, id)
}

func (c *Coordinator) CancelRun(ctx context.Context, taskID, runID platform.ID) error {
	err := c.sch.CancelRun(ctx, taskID, runID)
	if err != nil {
		return err
	}

	// TODO(lh): Im not sure if we need to call the task service here directly or if the scheduler does that
	// for now we will do it and then if it causes errors we can opt to do it in the scheduler only
	return c.TaskService.CancelRun(ctx, taskID, runID)
}

func (c *Coordinator) RetryRun(ctx context.Context, taskID, runID platform.ID) (*platform.Run, error) {
	task, err := c.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	r, err := c.TaskService.RetryRun(ctx, taskID, runID)
	if err != nil {
		return r, err
	}

	return r, c.sch.UpdateTask(ctx, task)
}

func (c *Coordinator) ForceRun(ctx context.Context, taskID platform.ID, scheduledFor int64) (*platform.Run, error) {
	task, err := c.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	r, err := c.TaskService.ForceRun(ctx, taskID, scheduledFor)
	if err != nil {
		return r, err
	}

	return r, c.sch.UpdateTask(ctx, task)
}
