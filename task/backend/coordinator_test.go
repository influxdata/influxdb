package backend

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"go.uber.org/zap/zaptest"
)

var (
	one   = influxdb.ID(1)
	two   = influxdb.ID(2)
	three = influxdb.ID(3)
	four  = influxdb.ID(4)

	aTime = time.Now().UTC()

	taskOne   = &influxdb.Task{ID: one}
	taskTwo   = &influxdb.Task{ID: two, Status: "active"}
	taskThree = &influxdb.Task{ID: three, Status: "inactive"}
	taskFour  = &influxdb.Task{ID: four}

	allTasks = map[influxdb.ID]*influxdb.Task{
		one:   taskOne,
		two:   taskTwo,
		three: taskThree,
		four:  taskFour,
	}
)

func Test_NotifyCoordinatorOfCreated(t *testing.T) {
	var (
		coordinator = &coordinator{}
		tasks       = &taskService{
			// paginated reponses
			pageOne: []*influxdb.Task{taskOne},
			otherPages: map[influxdb.ID][]*influxdb.Task{
				one:   []*influxdb.Task{taskTwo, taskThree},
				three: []*influxdb.Task{taskFour},
			},
		}
	)

	defer func(old func() time.Time) {
		now = old
	}(now)

	now = func() time.Time { return aTime }

	if err := NotifyCoordinatorOfExisting(context.Background(), zaptest.NewLogger(t), tasks, coordinator); err != nil {
		t.Errorf("expected nil, found %q", err)
	}

	if diff := cmp.Diff([]update{
		{two, influxdb.TaskUpdate{LatestCompleted: &aTime, LatestScheduled: &aTime}},
	}, tasks.updates); diff != "" {
		t.Errorf("unexpected updates to task service %v", diff)
	}

	if diff := cmp.Diff([]*influxdb.Task{
		taskTwo,
	}, coordinator.tasks); diff != "" {
		t.Errorf("unexpected tasks sent to coordinator %v", diff)
	}
}

type coordinator struct {
	tasks []*influxdb.Task
}

func (c *coordinator) TaskCreated(_ context.Context, task *influxdb.Task) error {
	c.tasks = append(c.tasks, task)

	return nil
}

// TasksService mocking
type taskService struct {
	// paginated tasks
	pageOne    []*influxdb.Task
	otherPages map[influxdb.ID][]*influxdb.Task

	// find tasks call
	filter influxdb.TaskFilter
	// update call
	updates []update
}

type update struct {
	ID     influxdb.ID
	Update influxdb.TaskUpdate
}

func (t *taskService) UpdateTask(_ context.Context, id influxdb.ID, upd influxdb.TaskUpdate) (*influxdb.Task, error) {
	t.updates = append(t.updates, update{id, upd})

	return allTasks[id], nil
}

func (t *taskService) FindTasks(_ context.Context, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	t.filter = filter

	if filter.After == nil {
		return t.pageOne, len(t.pageOne), nil
	}

	tasks := t.otherPages[*filter.After]
	return tasks, len(tasks), nil
}
