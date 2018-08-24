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

func (c *Coordinator) CreateTask(ctx context.Context, org, user platform.ID, script string, scheduleAfter int64) (platform.ID, error) {
	id, err := c.Store.CreateTask(ctx, org, user, script, scheduleAfter)
	if err != nil {
		return id, err
	}

	task, err := c.Store.FindTaskByID(ctx, id)
	if err != nil {
		return id, err
	}

	meta, err := c.Store.FindTaskMetaByID(ctx, id)
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

func (c *Coordinator) ModifyTask(ctx context.Context, id platform.ID, newScript string) error {
	if err := c.Store.ModifyTask(ctx, id, newScript); err != nil {
		return err
	}

	task, err := c.Store.FindTaskByID(ctx, id)
	if err != nil {
		return err
	}

	meta, err := c.Store.FindTaskMetaByID(ctx, id)
	if err != nil {
		return err
	}

	if err := c.sch.ReleaseTask(id); err != nil {
		return err
	}

	if err := c.sch.ClaimTask(task, meta); err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) EnableTask(ctx context.Context, id platform.ID) error {
	if err := c.Store.EnableTask(ctx, id); err != nil {
		return err
	}

	task, err := c.Store.FindTaskByID(ctx, id)
	if err != nil {
		return err
	}

	meta, err := c.Store.FindTaskMetaByID(ctx, id)
	if err != nil {
		return err
	}

	if err := c.sch.ClaimTask(task, meta); err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) DisableTask(ctx context.Context, id platform.ID) error {
	if err := c.Store.DisableTask(ctx, id); err != nil {
		return err
	}

	return c.sch.ReleaseTask(id)
}

func (c *Coordinator) DeleteTask(ctx context.Context, id platform.ID) (deleted bool, err error) {
	if err := c.sch.ReleaseTask(id); err != nil {
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
		if err := c.sch.ReleaseTask(orgTask.ID); err != nil {
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
		if err := c.sch.ReleaseTask(userTask.ID); err != nil {
			return err
		}
	}

	return c.Store.DeleteUser(ctx, userID)
}
