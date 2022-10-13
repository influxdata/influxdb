package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/task/backend/scheduler"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"go.uber.org/zap/zaptest"
)

func Test_Coordinator_Executor_Methods(t *testing.T) {
	var (
		one     = platform.ID(1)
		taskOne = &taskmodel.Task{ID: one}

		runOne = &taskmodel.Run{
			ID:     one,
			TaskID: one,
		}

		allowUnexported = cmp.AllowUnexported(executorE{}, schedulerC{}, SchedulableTask{})

		scheduledTime = time.Now()
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
					manualRunCall{taskOne.ID, runOne.ID, false},
				},
			},
		},
		{
			name: "RunForcedScheduled",
			call: func(t *testing.T, c *Coordinator) {
				rr := runOne
				rr.ScheduledFor = scheduledTime
				if err := c.RunForced(context.Background(), taskOne, runOne); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			executor: &executorE{
				calls: []interface{}{
					manualRunCall{taskOne.ID, runOne.ID, true},
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

func TestNewSchedulableTask(t *testing.T) {
	now := time.Now().UTC()
	one := platform.ID(1)
	taskOne := &taskmodel.Task{ID: one, CreatedAt: now, Cron: "* * * * *", LatestCompleted: now}
	schedulableT, err := NewSchedulableTask(taskOne)
	if err != nil {
		t.Fatal(err)
	}
	if !schedulableT.LastScheduled().Truncate(time.Second).Equal(now.Truncate(time.Second)) {
		fmt.Println(schedulableT.LastScheduled())
		t.Fatalf("expected SchedulableTask's LatestScheduled to equal %s but it was %s", now.Truncate(time.Second), schedulableT.LastScheduled())
	}

	taskTwo := &taskmodel.Task{ID: one, CreatedAt: now, Cron: "* * * * *", LatestCompleted: now, LatestScheduled: now.Add(-10 * time.Second)}
	schedulableT, err = NewSchedulableTask(taskTwo)
	if err != nil {
		t.Fatal(err)
	}
	if !schedulableT.LastScheduled().Truncate(time.Second).Equal(now.Truncate(time.Second)) {
		t.Fatalf("expected SchedulableTask's LatestScheduled to equal %s but it was %s", now.Truncate(time.Second), schedulableT.LastScheduled())
	}

}

func Test_Coordinator_Scheduler_Methods(t *testing.T) {

	var (
		one   = platform.ID(1)
		two   = platform.ID(2)
		three = platform.ID(3)
		now   = time.Now().UTC()

		taskOne           = &taskmodel.Task{ID: one, CreatedAt: now, Cron: "* * * * *"}
		taskTwo           = &taskmodel.Task{ID: two, Status: "active", CreatedAt: now, Cron: "* * * * *"}
		taskTwoInactive   = &taskmodel.Task{ID: two, Status: "inactive", CreatedAt: now, Cron: "* * * * *"}
		taskThreeOriginal = &taskmodel.Task{
			ID:        three,
			Status:    "active",
			Name:      "Previous",
			CreatedAt: now,
			Cron:      "* * * * *",
		}
		taskThreeNew = &taskmodel.Task{
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

	runOne := &taskmodel.Run{
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
			name: "TaskUpdated - inactive task is not scheduled",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskUpdated(context.Background(), taskTwoInactive, taskTwoInactive); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerC{},
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
