package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/task/backend"
	"github.com/influxdata/platform/task/options"
)

type Coordinator struct {
	sch backend.Scheduler
	st  backend.Store

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
		st:    st,
		limit: 1000,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *Coordinator) CreateTask(ctx context.Context, org, user platform.ID, script string) (platform.ID, error) {
	id, err := c.st.CreateTask(ctx, org, user, script)
	if err != nil {
		return id, err
	}

	task, err := c.st.FindTaskByID(ctx, id)
	if err != nil {
		return id, err
	}

	opt, err := options.FromScript(script)
	if err != nil {
		return id, err
	}

	if err := c.sch.ClaimTask(task, time.Now().UTC().Unix(), &opt); err != nil {
		_, delErr := c.st.DeleteTask(ctx, id)
		if delErr != nil {
			return id, fmt.Errorf("schedule task failed: %s\n\tcleanup also failed: %s", err, delErr)
		}
		return id, err
	}

	return id, nil
}

func (c *Coordinator) ModifyTask(ctx context.Context, id platform.ID, newScript string) error {
	opt, err := options.FromScript(newScript)
	if err != nil {
		return err
	}

	if err := c.st.ModifyTask(ctx, id, newScript); err != nil {
		return err
	}

	task, err := c.st.FindTaskByID(ctx, id)
	if err != nil {
		return err
	}

	meta, err := c.st.FindTaskMetaByID(ctx, id)
	if err != nil {
		return err
	}

	if err := c.sch.ReleaseTask(id); err != nil {
		return err
	}

	if err := c.sch.ClaimTask(task, meta.LastCompleted, &opt); err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) ListTasks(ctx context.Context, params backend.TaskSearchParams) ([]backend.StoreTask, error) {
	return c.st.ListTasks(ctx, params)
}

func (c *Coordinator) FindTaskByID(ctx context.Context, id platform.ID) (*backend.StoreTask, error) {
	return c.st.FindTaskByID(ctx, id)
}

func (c *Coordinator) EnableTask(ctx context.Context, id platform.ID) error {
	if err := c.st.EnableTask(ctx, id); err != nil {
		return err
	}

	task, err := c.st.FindTaskByID(ctx, id)
	if err != nil {
		return err
	}

	opt, err := options.FromScript(task.Script)
	if err != nil {
		return err
	}

	if err := c.sch.ClaimTask(task, time.Now().UTC().Unix(), &opt); err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) DisableTask(ctx context.Context, id platform.ID) error {
	if err := c.st.DisableTask(ctx, id); err != nil {
		return err
	}

	return c.sch.ReleaseTask(id)
}

func (c *Coordinator) FindTaskMetaByID(ctx context.Context, id platform.ID) (*backend.StoreTaskMeta, error) {
	return c.st.FindTaskMetaByID(ctx, id)
}

func (c *Coordinator) DeleteTask(ctx context.Context, id platform.ID) (deleted bool, err error) {
	if err := c.sch.ReleaseTask(id); err != nil {
		return false, err
	}

	return c.st.DeleteTask(ctx, id)
}

func (c *Coordinator) CreateRun(ctx context.Context, taskID platform.ID, now int64) (backend.QueuedRun, error) {
	return c.CreateRun(ctx, taskID, now)
}

func (c *Coordinator) FinishRun(ctx context.Context, taskID, runID platform.ID) error {
	return c.FinishRun(ctx, taskID, runID)
}

func (c *Coordinator) DeleteOrg(ctx context.Context, orgID platform.ID) error {
	orgTasks, err := c.st.ListTasks(ctx, backend.TaskSearchParams{
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

	return c.st.DeleteOrg(ctx, orgID)
}

func (c *Coordinator) DeleteUser(ctx context.Context, userID platform.ID) error {
	userTasks, err := c.st.ListTasks(ctx, backend.TaskSearchParams{
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

	return c.st.DeleteUser(ctx, userID)
}

func (c *Coordinator) Close() error {
	return c.st.Close()
}
