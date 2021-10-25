package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

// Coordinator is a type which is used to react to
// task related actions
type Coordinator interface {
	TaskCreated(context.Context, *taskmodel.Task) error
	TaskUpdated(ctx context.Context, from, to *taskmodel.Task) error
	TaskDeleted(context.Context, platform.ID) error
	RunCancelled(ctx context.Context, runID platform.ID) error
	RunForced(ctx context.Context, task *taskmodel.Task, run *taskmodel.Run) error
}

// CoordinatingTaskService acts as a TaskService decorator that handles coordinating the api request
// with the required task control actions asynchronously via a message dispatcher
type CoordinatingTaskService struct {
	taskmodel.TaskService
	coordinator Coordinator

	now func() time.Time
}

// New constructs a new coordinating task service
func New(service taskmodel.TaskService, coordinator Coordinator, opts ...Option) *CoordinatingTaskService {
	c := &CoordinatingTaskService{
		TaskService: service,
		coordinator: coordinator,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// CreateTask Creates a task in the existing task service and Publishes the change so any TaskD service can lease it.
func (s *CoordinatingTaskService) CreateTask(ctx context.Context, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {
	t, err := s.TaskService.CreateTask(ctx, tc)
	if err != nil {
		return t, err
	}

	if err := s.coordinator.TaskCreated(ctx, t); err != nil {
		if derr := s.TaskService.DeleteTask(ctx, t.ID); derr != nil {
			return t, fmt.Errorf("schedule task failed: %s\n\tcleanup also failed: %s", err, derr)
		}

		return t, err
	}

	return t, nil
}

// UpdateTask Updates a task and publishes the change so the task owner can act on the update
func (s *CoordinatingTaskService) UpdateTask(ctx context.Context, id platform.ID, upd taskmodel.TaskUpdate) (*taskmodel.Task, error) {
	from, err := s.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return nil, err
	}

	to, err := s.TaskService.UpdateTask(ctx, id, upd)
	if err != nil {
		return to, err
	}

	return to, s.coordinator.TaskUpdated(ctx, from, to)
}

// DeleteTask delete the task and publishes the change, to allow the task owner to find out about this change faster.
func (s *CoordinatingTaskService) DeleteTask(ctx context.Context, id platform.ID) error {
	if err := s.coordinator.TaskDeleted(ctx, id); err != nil {
		return err
	}

	return s.TaskService.DeleteTask(ctx, id)
}

// CancelRun Cancel the run and publish the cancellation.
func (s *CoordinatingTaskService) CancelRun(ctx context.Context, taskID, runID platform.ID) error {
	if err := s.TaskService.CancelRun(ctx, taskID, runID); err != nil {
		return err
	}

	return s.coordinator.RunCancelled(ctx, runID)
}

// RetryRun calls retry on the task service and publishes the retry.
func (s *CoordinatingTaskService) RetryRun(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	t, err := s.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	r, err := s.TaskService.RetryRun(ctx, taskID, runID)
	if err != nil {
		return r, err
	}

	return r, s.coordinator.RunForced(ctx, t, r)
}

// ForceRun create the forced run in the task system and publish to the pubSub.
func (s *CoordinatingTaskService) ForceRun(ctx context.Context, taskID platform.ID, scheduledFor int64) (*taskmodel.Run, error) {
	t, err := s.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	r, err := s.TaskService.ForceRun(ctx, taskID, scheduledFor)
	if err != nil {
		return r, err
	}

	return r, s.coordinator.RunForced(ctx, t, r)
}
