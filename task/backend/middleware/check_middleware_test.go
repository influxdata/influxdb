package middleware_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification/check"
	"github.com/influxdata/influxdb/notification/rule"
	"github.com/influxdata/influxdb/task/backend/middleware"
)

type pipingCoordinator struct {
	err             error
	taskCreatedPipe chan *influxdb.Task
	taskUpdatedPipe chan *influxdb.Task
	taskDeletedPipe chan influxdb.ID
}

func (p *pipingCoordinator) taskCreatedChan() <-chan *influxdb.Task {
	if p.taskCreatedPipe == nil {
		p.taskCreatedPipe = make(chan *influxdb.Task, 1)
	}
	return p.taskCreatedPipe
}
func (p *pipingCoordinator) taskUpdatedChan() <-chan *influxdb.Task {
	if p.taskUpdatedPipe == nil {
		p.taskUpdatedPipe = make(chan *influxdb.Task, 1)
	}
	return p.taskUpdatedPipe
}
func (p *pipingCoordinator) taskDeletedChan() <-chan influxdb.ID {
	if p.taskDeletedPipe == nil {
		p.taskDeletedPipe = make(chan influxdb.ID, 1)
	}
	return p.taskDeletedPipe
}

func (p *pipingCoordinator) TaskCreated(_ context.Context, t *influxdb.Task) error {
	if p.taskCreatedPipe != nil {
		p.taskCreatedPipe <- t
	}
	return p.err
}
func (p *pipingCoordinator) TaskUpdated(_ context.Context, from, to *influxdb.Task) error {
	if p.taskUpdatedPipe != nil {
		p.taskUpdatedPipe <- to
	}
	return p.err
}
func (p *pipingCoordinator) TaskDeleted(_ context.Context, id influxdb.ID) error {
	if p.taskDeletedPipe != nil {
		p.taskDeletedPipe <- id
	}
	return p.err
}
func (p *pipingCoordinator) RunCancelled(ctx context.Context, taskID, runID influxdb.ID) error {
	return p.err
}
func (p *pipingCoordinator) RunRetried(ctx context.Context, task *influxdb.Task, run *influxdb.Run) error {
	return p.err
}
func (p *pipingCoordinator) RunForced(ctx context.Context, task *influxdb.Task, run *influxdb.Run) error {
	return p.err
}

type mockedSvc struct {
	taskSvc           *mock.TaskService
	checkSvc          *mock.CheckService
	notificationSvc   *mock.NotificationRuleStore
	pipingCoordinator *pipingCoordinator
}

func newMockServices() mockedSvc {
	return mockedSvc{
		taskSvc: &mock.TaskService{
			FindTaskByIDFn: func(_ context.Context, id influxdb.ID) (*influxdb.Task, error) { return &influxdb.Task{ID: id}, nil },
			CreateTaskFn:   func(context.Context, influxdb.TaskCreate) (*influxdb.Task, error) { return &influxdb.Task{ID: 1}, nil },
			UpdateTaskFn: func(_ context.Context, id influxdb.ID, _ influxdb.TaskUpdate) (*influxdb.Task, error) {
				return &influxdb.Task{ID: id}, nil
			},
			DeleteTaskFn: func(context.Context, influxdb.ID) error { return nil },
		},
		checkSvc: &mock.CheckService{
			FindCheckByIDFn: func(_ context.Context, id influxdb.ID) (influxdb.Check, error) {
				c := &check.Deadman{}
				c.SetID(id)
				return c, nil
			},
			CreateCheckFn: func(context.Context, influxdb.Check, influxdb.ID) error { return nil },
			UpdateCheckFn: func(_ context.Context, _ influxdb.ID, c influxdb.Check) (influxdb.Check, error) { return c, nil },
			PatchCheckFn: func(_ context.Context, id influxdb.ID, _ influxdb.CheckUpdate) (influxdb.Check, error) {
				c := &check.Deadman{}
				c.SetID(id)
				return c, nil
			},
			DeleteCheckFn: func(context.Context, influxdb.ID) error { return nil },
		},
		notificationSvc: &mock.NotificationRuleStore{
			FindNotificationRuleByIDF: func(_ context.Context, id influxdb.ID) (influxdb.NotificationRule, error) {
				c := &rule.HTTP{}
				c.SetID(id)
				return c, nil
			},
			CreateNotificationRuleF: func(context.Context, influxdb.NotificationRule, influxdb.ID) error { return nil },
			UpdateNotificationRuleF: func(_ context.Context, _ influxdb.ID, c influxdb.NotificationRule, _ influxdb.ID) (influxdb.NotificationRule, error) {
				return c, nil
			},
			PatchNotificationRuleF: func(_ context.Context, id influxdb.ID, _ influxdb.NotificationRuleUpdate) (influxdb.NotificationRule, error) {
				c := &rule.HTTP{}
				c.SetID(id)
				return c, nil
			},
			DeleteNotificationRuleF: func(context.Context, influxdb.ID) error { return nil },
		},
		pipingCoordinator: &pipingCoordinator{},
	}
}

func newCheckSvcStack() (mockedSvc, *middleware.CoordinatingCheckService) {
	msvcs := newMockServices()
	return msvcs, middleware.NewCheckService(msvcs.checkSvc, msvcs.taskSvc, msvcs.pipingCoordinator)
}

func TestCheckCreate(t *testing.T) {
	mocks, checkService := newCheckSvcStack()
	ch := mocks.pipingCoordinator.taskCreatedChan()

	check := &check.Deadman{}
	check.SetTaskID(4)

	err := checkService.CreateCheck(context.Background(), check, 1)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case task := <-ch:
		if task.ID != check.GetTaskID() {
			t.Fatalf("task sent to coordinator doesn't match expected")
		}
	default:
		t.Fatal("didn't receive task")
	}

	mocks.pipingCoordinator.err = fmt.Errorf("bad")
	mocks.checkSvc.DeleteCheckFn = func(context.Context, influxdb.ID) error { return fmt.Errorf("AARGH") }

	err = checkService.CreateCheck(context.Background(), check, 1)
	if err.Error() != "schedule task failed: bad\n\tcleanup also failed: AARGH" {
		t.Fatal(err)
	}
}

func TestCheckUpdate(t *testing.T) {
	mocks, checkService := newCheckSvcStack()
	ch := mocks.pipingCoordinator.taskUpdatedChan()

	mocks.checkSvc.UpdateCheckFn = func(_ context.Context, _ influxdb.ID, c influxdb.Check) (influxdb.Check, error) {
		c.SetTaskID(10)
		return c, nil
	}

	deadman := &check.Deadman{}
	deadman.SetTaskID(4)

	check, err := checkService.UpdateCheck(context.Background(), 1, deadman)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case task := <-ch:
		if task.ID != check.GetTaskID() {
			t.Fatalf("task sent to coordinator doesn't match expected")
		}
	default:
		t.Fatal("didn't receive task")
	}
}

func TestCheckPatch(t *testing.T) {
	mocks, checkService := newCheckSvcStack()
	ch := mocks.pipingCoordinator.taskUpdatedChan()

	deadman := &check.Deadman{}
	deadman.SetTaskID(4)

	mocks.checkSvc.PatchCheckFn = func(context.Context, influxdb.ID, influxdb.CheckUpdate) (influxdb.Check, error) {
		return deadman, nil
	}

	check, err := checkService.PatchCheck(context.Background(), 1, influxdb.CheckUpdate{})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case task := <-ch:
		if task.ID != check.GetTaskID() {
			t.Fatalf("task sent to coordinator doesn't match expected")
		}
	default:
		t.Fatal("didn't receive task")
	}
}

func TestCheckDelete(t *testing.T) {
	mocks, checkService := newCheckSvcStack()
	ch := mocks.pipingCoordinator.taskDeletedChan()

	mocks.checkSvc.FindCheckByIDFn = func(_ context.Context, id influxdb.ID) (influxdb.Check, error) {
		c := &check.Deadman{}
		c.SetID(id)
		c.SetTaskID(21)
		return c, nil
	}

	err := checkService.DeleteCheck(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case id := <-ch:
		if id != influxdb.ID(21) {
			t.Fatalf("task sent to coordinator doesn't match expected")
		}
	default:
		t.Fatal("didn't receive task")
	}
}
