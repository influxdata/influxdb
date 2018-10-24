package coordinator

import (
	"context"
	"fmt"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/task/backend"
)

type Coordinator struct {
	backend.Store

	sch backend.Scheduler

	limit int
}

type Option func(*Coordinator)

func WithLimit(i int) Option {
	return func(c *Coordinator) {
		c.limit = i
	}
}

func New(scheduler backend.Scheduler, st backend.Store, opts ...Option) backend.Store {
	c := &Coordinator{
		sch:   scheduler,
		Store: st,
		limit: 1000,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *Coordinator) CreateTask(ctx context.Context, req backend.CreateTaskRequest) (platform.ID, error) {
	id, err := c.Store.CreateTask(ctx, req)
	if err != nil {
		return id, err
	}

	task, meta, err := c.Store.FindTaskByIDWithMeta(ctx, id)
	if err != nil {
		return id, err
	}

	if err := c.sch.ClaimTask(task, meta); err != nil {
		_, delErr := c.Store.DeleteTask(ctx, id)
		if delErr != nil {
			return id, fmt.Errorf("schedule task failed: %s\n\tcleanup also failed: %s", err, delErr)
		}
		return id, err
	}

	return id, nil
}

func (c *Coordinator) UpdateTask(ctx context.Context, req backend.UpdateTaskRequest) (backend.UpdateTaskResult, error) {
	res, err := c.Store.UpdateTask(ctx, req)
	if err != nil {
		return res, err
	}

	task, meta, err := c.Store.FindTaskByIDWithMeta(ctx, req.ID)
	if err != nil {
		return res, err
	}

	// If disabling the task, do so before modifying the script.
	if req.Status == backend.TaskInactive && res.OldStatus != backend.TaskInactive {
		if err := c.sch.ReleaseTask(req.ID); err != nil && err != backend.ErrTaskNotClaimed {
			return res, err
		}
	}

	if err := c.sch.UpdateTask(task, meta); err != nil && err != backend.ErrTaskNotClaimed {
		return res, err
	}

	// If enabling the task, claim it after modifying the script.
	if req.Status == backend.TaskActive {
		if err := c.sch.ClaimTask(task, meta); err != nil && err != backend.ErrTaskAlreadyClaimed {
			return res, err
		}
	}

	return res, nil
}

func (c *Coordinator) DeleteTask(ctx context.Context, id platform.ID) (deleted bool, err error) {
	if err := c.sch.ReleaseTask(id); err != nil && err != backend.ErrTaskNotClaimed {
		return false, err
	}

	return c.Store.DeleteTask(ctx, id)
}

func (c *Coordinator) DeleteOrg(ctx context.Context, orgID platform.ID) error {
	orgTasks, err := c.Store.ListTasks(ctx, backend.TaskSearchParams{
		Org: orgID,
	})
	if err != nil {
		return err
	}

	for _, orgTask := range orgTasks {
		if err := c.sch.ReleaseTask(orgTask.Task.ID); err != nil {
			return err
		}
	}

	return c.Store.DeleteOrg(ctx, orgID)
}

func (c *Coordinator) DeleteUser(ctx context.Context, userID platform.ID) error {
	userTasks, err := c.Store.ListTasks(ctx, backend.TaskSearchParams{
		User: userID,
	})
	if err != nil {
		return err
	}

	for _, userTask := range userTasks {
		if err := c.sch.ReleaseTask(userTask.Task.ID); err != nil {
			return err
		}
	}

	return c.Store.DeleteUser(ctx, userID)
}
