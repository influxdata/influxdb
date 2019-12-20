package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/task/backend/scheduler"
)

var _ scheduler.SchedulableService = (*SchedulableTaskService)(nil)

// UpdateTaskService provides an API to update the LatestScheduled time of a task
type UpdateTaskService interface {
	UpdateTask(ctx context.Context, id influxdb.ID, upd influxdb.TaskUpdate) (*influxdb.Task, error)
}

// SchedulableTaskService implements the SchedulableService interface
type SchedulableTaskService struct {
	UpdateTaskService
}

// NewSchedulableTaskService initializes a new SchedulableTaskService given an UpdateTaskService
func NewSchedulableTaskService(ts UpdateTaskService) SchedulableTaskService {
	return SchedulableTaskService{ts}
}

// UpdateLastScheduled uses the task service to store the latest time a task was scheduled to run
func (s SchedulableTaskService) UpdateLastScheduled(ctx context.Context, id scheduler.ID, t time.Time) error {
	_, err := s.UpdateTask(ctx, influxdb.ID(id), influxdb.TaskUpdate{
		LatestScheduled: &t,
	})

	if err != nil {
		return fmt.Errorf("could not update last scheduled for task; Err: %v", err)
	}
	return nil
}
