package middleware_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/notification/rule"
	"github.com/influxdata/influxdb/v2/task/backend/middleware"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

func newNotificationRuleSvcStack() (mockedSvc, *middleware.CoordinatingNotificationRuleStore) {
	msvcs := newMockServices()
	return msvcs, middleware.NewNotificationRuleStore(msvcs.notificationSvc, msvcs.taskSvc, msvcs.pipingCoordinator)
}

func TestNotificationRuleCreate(t *testing.T) {
	mocks, nrService := newNotificationRuleSvcStack()
	ch := mocks.pipingCoordinator.taskCreatedChan()

	nr := &rule.HTTP{}
	nr.SetTaskID(4)

	nrc := influxdb.NotificationRuleCreate{
		NotificationRule: nr,
		Status:           influxdb.Active,
	}

	err := nrService.CreateNotificationRule(context.Background(), nrc, 1)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case task := <-ch:
		if task.ID != nr.GetTaskID() {
			t.Fatalf("task sent to coordinator doesn't match expected")
		}
	default:
		t.Fatal("didn't receive task")
	}

	mocks.pipingCoordinator.err = fmt.Errorf("bad")
	mocks.notificationSvc.DeleteNotificationRuleF = func(context.Context, platform.ID) error { return fmt.Errorf("AARGH") }

	err = nrService.CreateNotificationRule(context.Background(), nrc, 1)
	if err.Error() != "schedule task failed: bad\n\tcleanup also failed: AARGH" {
		t.Fatal(err)
	}
}

func TestNotificationRuleUpdateFromInactive(t *testing.T) {
	mocks, nrService := newNotificationRuleSvcStack()
	latest := time.Now().UTC()
	nrService.Now = func() time.Time {
		return latest
	}
	ch := mocks.pipingCoordinator.taskUpdatedChan()

	mocks.notificationSvc.UpdateNotificationRuleF = func(_ context.Context, _ platform.ID, c influxdb.NotificationRuleCreate, _ platform.ID) (influxdb.NotificationRule, error) {
		c.SetTaskID(10)
		c.SetUpdatedAt(latest.Add(-20 * time.Hour))
		return c, nil
	}

	mocks.notificationSvc.PatchNotificationRuleF = func(_ context.Context, id platform.ID, _ influxdb.NotificationRuleUpdate) (influxdb.NotificationRule, error) {
		ic := &rule.HTTP{}
		ic.SetTaskID(10)
		ic.SetUpdatedAt(latest.Add(-20 * time.Hour))
		return ic, nil
	}

	mocks.notificationSvc.FindNotificationRuleByIDF = func(_ context.Context, id platform.ID) (influxdb.NotificationRule, error) {
		c := &rule.HTTP{}
		c.SetID(id)
		c.SetTaskID(1)
		return c, nil
	}

	mocks.taskSvc.FindTaskByIDFn = func(_ context.Context, id platform.ID) (*taskmodel.Task, error) {
		if id == 1 {
			return &taskmodel.Task{ID: id, Status: string(taskmodel.TaskInactive)}, nil
		} else if id == 10 {
			return &taskmodel.Task{ID: id, Status: string(taskmodel.TaskActive)}, nil
		}
		return &taskmodel.Task{ID: id}, nil
	}

	deadman := &rule.HTTP{}
	deadman.SetTaskID(10)

	nrc := influxdb.NotificationRuleCreate{
		NotificationRule: deadman,
		Status:           influxdb.Active,
	}

	therule, err := nrService.UpdateNotificationRule(context.Background(), 1, nrc, 11)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case task := <-ch:
		if task.ID != therule.GetTaskID() {
			t.Fatalf("task sent to coordinator doesn't match expected")
		}
		if task.LatestCompleted != latest {
			t.Fatalf("update returned incorrect LatestCompleted, expected %s got %s, or ", latest.Format(time.RFC3339), task.LatestCompleted)
		}
	default:
		t.Fatal("didn't receive task")
	}

	action := influxdb.Active
	therule, err = nrService.PatchNotificationRule(context.Background(), 1, influxdb.NotificationRuleUpdate{Status: &action})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case task := <-ch:
		if task.ID != therule.GetTaskID() {
			t.Fatalf("task sent to coordinator doesn't match expected")
		}
		if task.LatestCompleted != latest {
			t.Fatalf("update returned incorrect LatestCompleted, expected %s got %s, or ", latest.Format(time.RFC3339), task.LatestCompleted)
		}
	default:
		t.Fatal("didn't receive task")
	}

}
func TestNotificationRuleUpdate(t *testing.T) {
	mocks, nrService := newNotificationRuleSvcStack()
	ch := mocks.pipingCoordinator.taskUpdatedChan()

	mocks.notificationSvc.UpdateNotificationRuleF = func(_ context.Context, _ platform.ID, c influxdb.NotificationRuleCreate, _ platform.ID) (influxdb.NotificationRule, error) {
		c.SetTaskID(10)
		return c, nil
	}

	deadman := &rule.HTTP{}
	deadman.SetTaskID(4)

	nrc := influxdb.NotificationRuleCreate{
		NotificationRule: deadman,
		Status:           influxdb.Active,
	}

	nr, err := nrService.UpdateNotificationRule(context.Background(), 1, nrc, 2)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case task := <-ch:
		if task.ID != nr.GetTaskID() {
			t.Fatalf("task sent to coordinator doesn't match expected")
		}
	default:
		t.Fatal("didn't receive task")
	}
}

func TestNotificationRulePatch(t *testing.T) {
	mocks, nrService := newNotificationRuleSvcStack()
	ch := mocks.pipingCoordinator.taskUpdatedChan()

	deadman := &rule.HTTP{}
	deadman.SetTaskID(4)

	mocks.notificationSvc.PatchNotificationRuleF = func(context.Context, platform.ID, influxdb.NotificationRuleUpdate) (influxdb.NotificationRule, error) {
		return deadman, nil
	}

	nr, err := nrService.PatchNotificationRule(context.Background(), 1, influxdb.NotificationRuleUpdate{})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case task := <-ch:
		if task.ID != nr.GetTaskID() {
			t.Fatalf("task sent to coordinator doesn't match expected")
		}
	default:
		t.Fatal("didn't receive task")
	}
}

func TestNotificationRuleDelete(t *testing.T) {
	mocks, nrService := newNotificationRuleSvcStack()
	ch := mocks.pipingCoordinator.taskDeletedChan()

	mocks.notificationSvc.FindNotificationRuleByIDF = func(_ context.Context, id platform.ID) (influxdb.NotificationRule, error) {
		c := &rule.HTTP{}
		c.SetID(id)
		c.SetTaskID(21)
		return c, nil
	}

	err := nrService.DeleteNotificationRule(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case id := <-ch:
		if id != platform.ID(21) {
			t.Fatalf("task sent to coordinator doesn't match expected")
		}
	default:
		t.Fatal("didn't receive task")
	}
}
