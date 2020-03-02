package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend/scheduler"
	"go.uber.org/zap/zaptest"
)

func Test_Coordinator_Executor_Methods(t *testing.T) {
	var (
		one     = influxdb.ID(1)
		taskOne = &influxdb.Task{ID: one}

		runOne = &influxdb.Run{
			ID:           one,
			TaskID:       one,
			ScheduledFor: time.Now(),
		}

		allowUnexported = cmp.AllowUnexported(executorE{}, schedulerC{}, SchedulableTask{})
	)

	for _, test := range []struct {
		name       string
		claimErr   error
		updateErr  error
		releaseErr error
		call       func(*testing.T, *Coordinator)
		executor   *executorE
	}{
		{
			name: "RunForced",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.RunForced(context.Background(), taskOne, runOne); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			executor: &executorE{
				calls: []interface{}{
					manualRunCall{taskOne.ID, runOne.ID},
				},
			},
		},
		{
			name: "RunRetried",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.RunRetried(context.Background(), taskOne, runOne); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			executor: &executorE{
				calls: []interface{}{
					manualRunCall{taskOne.ID, runOne.ID},
				},
			},
		},
		{
			name: "RunCancelled",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.RunCancelled(context.Background(), runOne.ID); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			executor: &executorE{
				calls: []interface{}{
					cancelCallC{runOne.ID},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				executor  = &executorE{}
				scheduler = &schedulerC{}
				coord     = NewCoordinator(zaptest.NewLogger(t), scheduler, executor)
			)

			test.call(t, coord)

			if diff := cmp.Diff(
				test.executor.calls,
				executor.calls,
				allowUnexported); diff != "" {
				t.Errorf("unexpected executor contents %s", diff)
			}
		})
	}
}

func Test_Coordinator_Scheduler_Methods(t *testing.T) {

	var (
		one   = influxdb.ID(1)
		two   = influxdb.ID(2)
		three = influxdb.ID(3)
		now   = time.Now().UTC()

		taskOne           = &influxdb.Task{ID: one, CreatedAt: now, Cron: "* * * * *"}
		taskTwo           = &influxdb.Task{ID: two, Status: "active", CreatedAt: now, Cron: "* * * * *"}
		taskTwoInactive   = &influxdb.Task{ID: two, Status: "inactive", CreatedAt: now, Cron: "* * * * *"}
		taskThreeOriginal = &influxdb.Task{
			ID:        three,
			Status:    "active",
			Name:      "Previous",
			CreatedAt: now,
			Cron:      "* * * * *",
		}
		taskThreeNew = &influxdb.Task{
			ID:        three,
			Status:    "active",
			Name:      "Renamed",
			CreatedAt: now,
			Cron:      "* * * * *",
		}
	)

	schedulableT, err := NewSchedulableTask(taskOne)
	if err != nil {
		t.Fatal(err)
	}
	schedulableTaskTwo, err := NewSchedulableTask(taskTwo)
	if err != nil {
		t.Fatal(err)
	}

	schedulableTaskThree, err := NewSchedulableTask(taskThreeNew)
	if err != nil {
		t.Fatal(err)
	}

	runOne := &influxdb.Run{
		ID:           one,
		TaskID:       one,
		ScheduledFor: time.Now().UTC(),
	}

	for _, test := range []struct {
		name       string
		claimErr   error
		updateErr  error
		releaseErr error
		call       func(*testing.T, *Coordinator)
		scheduler  *schedulerC
	}{
		{
			name: "TaskCreated",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskCreated(context.Background(), taskOne); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerC{
				calls: []interface{}{
					scheduleCall{schedulableT},
				},
			},
		},
		{
			name: "TaskUpdated - deactivate task",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskUpdated(context.Background(), taskTwo, taskTwoInactive); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerC{
				calls: []interface{}{
					releaseCallC{scheduler.ID(taskTwo.ID)},
				},
			},
		},
		{
			name: "TaskUpdated - activate task",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskUpdated(context.Background(), taskTwoInactive, taskTwo); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerC{
				calls: []interface{}{
					scheduleCall{schedulableTaskTwo},
				},
			},
		},
		{
			name: "TaskUpdated - change name",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskUpdated(context.Background(), taskThreeOriginal, taskThreeNew); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerC{
				calls: []interface{}{
					scheduleCall{schedulableTaskThree},
				},
			},
		},
		{
			name: "TaskDeleted",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskDeleted(context.Background(), runOne.ID); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerC{
				calls: []interface{}{
					releaseCallC{scheduler.ID(taskOne.ID)},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				executor = &executorE{}
				sch      = &schedulerC{}
				coord    = NewCoordinator(zaptest.NewLogger(t), sch, executor)
			)

			test.call(t, coord)

			if diff := cmp.Diff(
				test.scheduler.calls,
				sch.calls,
				cmp.AllowUnexported(executorE{}, schedulerC{}, SchedulableTask{}, *coord),
				cmpopts.IgnoreUnexported(scheduler.Schedule{}),
			); diff != "" {
				t.Errorf("unexpected scheduler contents %s", diff)
			}
		})
	}
}
