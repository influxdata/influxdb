package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

// CoordinatingNotificationRuleStore acts as a NotificationRuleStore decorator that handles coordinating the api request
// with the required task control actions asynchronously via a message dispatcher
type CoordinatingNotificationRuleStore struct {
	influxdb.NotificationRuleStore
	coordinator Coordinator
	taskService taskmodel.TaskService
	Now         func() time.Time
}

// NewNotificationRuleStore constructs a new coordinating notification service
func NewNotificationRuleStore(ns influxdb.NotificationRuleStore, ts taskmodel.TaskService, coordinator Coordinator) *CoordinatingNotificationRuleStore {
	c := &CoordinatingNotificationRuleStore{
		NotificationRuleStore: ns,
		taskService:           ts,
		coordinator:           coordinator,
		Now: func() time.Time {
			return time.Now().UTC()
		},
	}

	return c
}

// CreateNotificationRule Creates a notification and Publishes the change it can be scheduled.
func (ns *CoordinatingNotificationRuleStore) CreateNotificationRule(ctx context.Context, nr influxdb.NotificationRuleCreate, userID platform.ID) error {

	if err := ns.NotificationRuleStore.CreateNotificationRule(ctx, nr, userID); err != nil {
		return err
	}

	t, err := ns.taskService.FindTaskByID(ctx, nr.GetTaskID())
	if err != nil {
		return err
	}

	if err := ns.coordinator.TaskCreated(ctx, t); err != nil {
		if derr := ns.NotificationRuleStore.DeleteNotificationRule(ctx, nr.GetID()); derr != nil {
			return fmt.Errorf("schedule task failed: %s\n\tcleanup also failed: %s", err, derr)
		}

		return err
	}

	return nil
}

// UpdateNotificationRule Updates a notification and publishes the change so the task owner can act on the update
func (ns *CoordinatingNotificationRuleStore) UpdateNotificationRule(ctx context.Context, id platform.ID, nr influxdb.NotificationRuleCreate, uid platform.ID) (influxdb.NotificationRule, error) {
	from, err := ns.NotificationRuleStore.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return nil, err
	}

	fromTask, err := ns.taskService.FindTaskByID(ctx, from.GetTaskID())
	if err != nil {
		return nil, err
	}

	to, err := ns.NotificationRuleStore.UpdateNotificationRule(ctx, id, nr, uid)
	if err != nil {
		return to, err
	}

	toTask, err := ns.taskService.FindTaskByID(ctx, to.GetTaskID())
	if err != nil {
		return nil, err
	}
	// if the update is to activate and the previous task was inactive we should add a "latest completed" update
	// this allows us to see not run the task for inactive time
	if fromTask.Status == string(taskmodel.TaskInactive) && toTask.Status == string(taskmodel.TaskActive) {
		toTask.LatestCompleted = ns.Now()
	}

	return to, ns.coordinator.TaskUpdated(ctx, fromTask, toTask)
}

// PatchNotificationRule Updates a notification and publishes the change so the task owner can act on the update
func (ns *CoordinatingNotificationRuleStore) PatchNotificationRule(ctx context.Context, id platform.ID, upd influxdb.NotificationRuleUpdate) (influxdb.NotificationRule, error) {
	from, err := ns.NotificationRuleStore.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return nil, err
	}

	fromTask, err := ns.taskService.FindTaskByID(ctx, from.GetTaskID())
	if err != nil {
		return nil, err
	}

	to, err := ns.NotificationRuleStore.PatchNotificationRule(ctx, id, upd)
	if err != nil {
		return to, err
	}

	toTask, err := ns.taskService.FindTaskByID(ctx, to.GetTaskID())
	if err != nil {
		return nil, err
	}

	// if the update is to activate and the previous task was inactive we should add a "latest completed" update
	// this allows us to see not run the task for inactive time
	if fromTask.Status == string(taskmodel.TaskInactive) && toTask.Status == string(taskmodel.TaskActive) {
		toTask.LatestCompleted = ns.Now()
	}

	return to, ns.coordinator.TaskUpdated(ctx, fromTask, toTask)

}

// DeleteNotificationRule delete the notification and publishes the change, to allow the task owner to find out about this change faster.
func (ns *CoordinatingNotificationRuleStore) DeleteNotificationRule(ctx context.Context, id platform.ID) error {
	notification, err := ns.NotificationRuleStore.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return err
	}

	t, err := ns.taskService.FindTaskByID(ctx, notification.GetTaskID())
	if err != nil {
		return err
	}

	if err := ns.coordinator.TaskDeleted(ctx, t.ID); err != nil {
		return err
	}

	return ns.NotificationRuleStore.DeleteNotificationRule(ctx, id)
}
