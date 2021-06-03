package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

// CoordinatingCheckService acts as a CheckService decorator that handles coordinating the api request
// with the required task control actions asynchronously via a message dispatcher
type CoordinatingCheckService struct {
	influxdb.CheckService
	coordinator Coordinator
	taskService taskmodel.TaskService
	Now         func() time.Time
}

// NewCheckService constructs a new coordinating check service
func NewCheckService(cs influxdb.CheckService, ts taskmodel.TaskService, coordinator Coordinator) *CoordinatingCheckService {
	c := &CoordinatingCheckService{
		CheckService: cs,
		taskService:  ts,
		coordinator:  coordinator,
		Now: func() time.Time {
			return time.Now().UTC()
		},
	}

	return c
}

// CreateCheck Creates a check and Publishes the change it can be scheduled.
func (cs *CoordinatingCheckService) CreateCheck(ctx context.Context, c influxdb.CheckCreate, userID platform.ID) error {

	if err := cs.CheckService.CreateCheck(ctx, c, userID); err != nil {
		return err
	}

	t, err := cs.taskService.FindTaskByID(ctx, c.GetTaskID())
	if err != nil {
		return err
	}

	if err := cs.coordinator.TaskCreated(ctx, t); err != nil {
		if derr := cs.CheckService.DeleteCheck(ctx, c.GetID()); derr != nil {
			return fmt.Errorf("schedule task failed: %s\n\tcleanup also failed: %s", err, derr)
		}

		return err
	}

	return nil
}

// UpdateCheck Updates a check and publishes the change so the task owner can act on the update
func (cs *CoordinatingCheckService) UpdateCheck(ctx context.Context, id platform.ID, c influxdb.CheckCreate) (influxdb.Check, error) {
	from, err := cs.CheckService.FindCheckByID(ctx, id)
	if err != nil {
		return nil, err
	}

	fromTask, err := cs.taskService.FindTaskByID(ctx, from.GetTaskID())
	if err != nil {
		return nil, err
	}

	to, err := cs.CheckService.UpdateCheck(ctx, id, c)
	if err != nil {
		return to, err
	}

	toTask, err := cs.taskService.FindTaskByID(ctx, to.GetTaskID())
	if err != nil {
		return nil, err
	}

	// if the update is to activate and the previous task was inactive we should add a "latest completed" update
	// this allows us to see not run the task for inactive time
	if fromTask.Status == string(taskmodel.TaskInactive) && toTask.Status == string(taskmodel.TaskActive) {
		toTask.LatestCompleted = cs.Now()
	}

	return to, cs.coordinator.TaskUpdated(ctx, fromTask, toTask)
}

// PatchCheck Updates a check and publishes the change so the task owner can act on the update
func (cs *CoordinatingCheckService) PatchCheck(ctx context.Context, id platform.ID, upd influxdb.CheckUpdate) (influxdb.Check, error) {
	from, err := cs.CheckService.FindCheckByID(ctx, id)
	if err != nil {
		return nil, err
	}

	fromTask, err := cs.taskService.FindTaskByID(ctx, from.GetTaskID())
	if err != nil {
		return nil, err
	}

	to, err := cs.CheckService.PatchCheck(ctx, id, upd)
	if err != nil {
		return to, err
	}

	toTask, err := cs.taskService.FindTaskByID(ctx, to.GetTaskID())
	if err != nil {
		return nil, err
	}

	// if the update is to activate and the previous task was inactive we should add a "latest completed" update
	// this allows us to see not run the task for inactive time
	if fromTask.Status == string(taskmodel.TaskInactive) && toTask.Status == string(taskmodel.TaskActive) {
		toTask.LatestCompleted = cs.Now()
	}

	return to, cs.coordinator.TaskUpdated(ctx, fromTask, toTask)

}

// DeleteCheck delete the check and publishes the change, to allow the task owner to find out about this change faster.
func (cs *CoordinatingCheckService) DeleteCheck(ctx context.Context, id platform.ID) error {
	check, err := cs.CheckService.FindCheckByID(ctx, id)
	if err != nil {
		return err
	}

	t, err := cs.taskService.FindTaskByID(ctx, check.GetTaskID())
	if err != nil {
		return err
	}

	if err := cs.coordinator.TaskDeleted(ctx, t.ID); err != nil {
		return err
	}

	return cs.CheckService.DeleteCheck(ctx, id)
}
