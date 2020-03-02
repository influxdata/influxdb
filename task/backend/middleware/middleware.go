package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb"
)

// Coordinator is a type which is used to react to
// task related actions
type Coordinator interface {
	TaskCreated(context.Context, *influxdb.Task) error
	TaskUpdated(ctx context.Context, from, to *influxdb.Task) error
	TaskDeleted(context.Context, influxdb.ID) error
	RunCancelled(ctx context.Context, runID influxdb.ID) error
	RunRetried(ctx context.Context, task *influxdb.Task, run *influxdb.Run) error
	RunForced(ctx context.Context, task *influxdb.Task, run *influxdb.Run) error
}

// CoordinatingTaskService acts as a TaskService decorator that handles coordinating the api request
// with the required task control actions asynchronously via a message dispatcher
type CoordinatingTaskService struct {
	influxdb.TaskService
	coordinator Coordinator

	now func() time.Time
}

// New constructs a new coordinating task service
func New(service influxdb.TaskService, coordinator Coordinator, opts ...Option) *CoordinatingTaskService {
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
func (s *CoordinatingTaskService) CreateTask(ctx context.Context, tc influxdb.TaskCreate) (*influxdb.Task, error) {
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
func (s *CoordinatingTaskService) UpdateTask(ctx context.Context, id influxdb.ID, upd influxdb.TaskUpdate) (*influxdb.Task, error) {
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
func (s *CoordinatingTaskService) DeleteTask(ctx context.Context, id influxdb.ID) error {
	if err := s.coordinator.TaskDeleted(ctx, id); err != nil {
		return err
	}

	return s.TaskService.DeleteTask(ctx, id)
}

// CancelRun Cancel the run and publish the cancelation.
func (s *CoordinatingTaskService) CancelRun(ctx context.Context, taskID, runID influxdb.ID) error {
	if err := s.TaskService.CancelRun(ctx, taskID, runID); err != nil {
		return err
	}

	return s.coordinator.RunCancelled(ctx, runID)
}

// RetryRun calls retry on the task service and publishes the retry.
func (s *CoordinatingTaskService) RetryRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	t, err := s.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	r, err := s.TaskService.RetryRun(ctx, taskID, runID)
	if err != nil {
		return r, err
	}

	return r, s.coordinator.RunRetried(ctx, t, r)
}

// ForceRun create the forced run in the task system and publish to the pubSub.
func (s *CoordinatingTaskService) ForceRun(ctx context.Context, taskID influxdb.ID, scheduledFor int64) (*influxdb.Run, error) {
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
