package coordinator

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/task/backend/scheduler"
	"go.uber.org/zap/zaptest"
)

var (
	one   = influxdb.ID(1)
	two   = influxdb.ID(2)
	three = influxdb.ID(3)

	taskOne     = &influxdb.Task{ID: one}
	taskTwo     = &influxdb.Task{ID: two, Status: "active"}
	taskThree   = &influxdb.Task{ID: three, Status: "inactive"}
	activeThree = &influxdb.Task{
		ID:     three,
		Status: "active",
	}

	runOne = &influxdb.Run{
		ID:     one,
		TaskID: one,
	}

	allowUnexported = cmp.AllowUnexported(schedulerS{}, scheduler.Schedule{}, SchedulableTask{})
)

func Test_Coordinator(t *testing.T) {
	for _, test := range []struct {
		name       string
		claimErr   error
		updateErr  error
		releaseErr error
		call       func(*testing.T, *Coordinator)
		scheduler  *schedulerS
	}{
		{
			name: "TaskCreated",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskCreated(context.Background(), taskOne); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerS{
				calls: []interface{}{
					claimCall{taskOne},
				},
			},
		},
		{
			name: "TaskUpdated from inactive to active",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskUpdated(context.Background(), taskThree, activeThree); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerS{
				calls: []interface{}{
					updateCall{activeThree},
					claimCall{activeThree},
				},
			},
		},
		{
			name: "TaskUpdated from active to inactive",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskUpdated(context.Background(), activeThree, taskThree); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerS{
				calls: []interface{}{
					releaseCall{three},
					updateCall{taskThree},
				},
			},
		},
		{
			name:       "TaskUpdated from active to inactive task not claimed error on release",
			releaseErr: influxdb.ErrTaskNotClaimed,
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskUpdated(context.Background(), activeThree, taskThree); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerS{
				calls: []interface{}{
					releaseCall{three},
					updateCall{taskThree},
				},
			},
		},
		{
			name:      "TaskUpdated from active to inactive task not claimed error on update",
			updateErr: influxdb.ErrTaskNotClaimed,
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskUpdated(context.Background(), activeThree, taskThree); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerS{
				calls: []interface{}{
					releaseCall{three},
					updateCall{taskThree},
				},
			},
		},
		{
			name: "TaskUpdated with no status change",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskUpdated(context.Background(), taskTwo, taskTwo); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerS{
				calls: []interface{}{
					updateCall{taskTwo},
				},
			},
		},
		{
			name: "TaskDeleted releases the task ID",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.TaskDeleted(context.Background(), two); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerS{
				calls: []interface{}{
					releaseCall{two},
				},
			},
		},
		{
			name: "RunCancelled delegates to the scheduler",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.RunCancelled(context.Background(), one, one); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerS{
				calls: []interface{}{
					cancelCall{one, one},
				},
			},
		},
		{
			name: "RunRetried delegates to Update",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.RunRetried(context.Background(), taskOne, runOne); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerS{
				calls: []interface{}{
					updateCall{taskOne},
				},
			},
		},
		{
			name: "RunForced delegates to Update",
			call: func(t *testing.T, c *Coordinator) {
				if err := c.RunForced(context.Background(), taskOne, runOne); err != nil {
					t.Errorf("expected nil error found %q", err)
				}
			},
			scheduler: &schedulerS{
				calls: []interface{}{
					updateCall{taskOne},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				scheduler = &schedulerS{
					claimErr:   test.claimErr,
					updateErr:  test.updateErr,
					releaseErr: test.releaseErr,
				}
				coord = New(zaptest.NewLogger(t), scheduler)
			)

			test.call(t, coord)

			if diff := cmp.Diff(
				test.scheduler.calls,
				scheduler.calls,
				allowUnexported); diff != "" {
				t.Errorf("unexpected scheduler contents %s", diff)
			}
		})
	}
}
