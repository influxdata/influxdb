package coordinator

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/backend/middleware"
	"go.uber.org/zap"
)

var _ middleware.Coordinator = (*Coordinator)(nil)

type Coordinator struct {
	log *zap.Logger
	sch backend.Scheduler

	limit int
}

type Option func(*Coordinator)

func WithLimit(i int) Option {
	return func(c *Coordinator) {
		c.limit = i
	}
}

func New(log *zap.Logger, scheduler backend.Scheduler, opts ...Option) *Coordinator {
	c := &Coordinator{
		log:   log,
		sch:   scheduler,
		limit: 1000,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *Coordinator) TaskCreated(ctx context.Context, task *influxdb.Task) error {
	return c.sch.ClaimTask(ctx, task)
}

func (c *Coordinator) TaskUpdated(ctx context.Context, from, to *influxdb.Task) error {
	// if disabling the task release it before schedule update
	if to.Status != from.Status && to.Status == string(backend.TaskInactive) {
		if err := c.sch.ReleaseTask(to.ID); err != nil && err != influxdb.ErrTaskNotClaimed {
			return err
		}
	}

	if err := c.sch.UpdateTask(ctx, to); err != nil && err != influxdb.ErrTaskNotClaimed {
		return err
	}

	// if enabling the task then claim it
	if to.Status != from.Status && to.Status == string(backend.TaskActive) {
		if err := c.sch.ClaimTask(ctx, to); err != nil && err != influxdb.ErrTaskAlreadyClaimed {
			return err
		}
	}

	return nil
}

func (c *Coordinator) TaskDeleted(ctx context.Context, id influxdb.ID) error {
	if err := c.sch.ReleaseTask(id); err != nil && err != influxdb.ErrTaskNotClaimed {
		return err
	}

	return nil
}

func (c *Coordinator) RunCancelled(ctx context.Context, taskID, runID influxdb.ID) error {
	return c.sch.CancelRun(ctx, taskID, runID)
}

func (c *Coordinator) RunRetried(ctx context.Context, task *influxdb.Task, run *influxdb.Run) error {
	return c.sch.UpdateTask(ctx, task)
}

func (c *Coordinator) RunForced(ctx context.Context, task *influxdb.Task, run *influxdb.Run) error {
	return c.sch.UpdateTask(ctx, task)
}
