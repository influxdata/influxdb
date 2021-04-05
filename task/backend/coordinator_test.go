package backend

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"go.uber.org/zap/zaptest"
)

var (
	one   = platform.ID(1)
	two   = platform.ID(2)
	three = platform.ID(3)
	four  = platform.ID(4)

	aTime = time.Now().UTC()

	taskOne   = &taskmodel.Task{ID: one}
	taskTwo   = &taskmodel.Task{ID: two, Status: "active"}
	taskThree = &taskmodel.Task{ID: three, Status: "inactive"}
	taskFour  = &taskmodel.Task{ID: four}

	allTasks = map[platform.ID]*taskmodel.Task{
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
			// paginated responses
			pageOne: []*taskmodel.Task{taskOne},
			otherPages: map[platform.ID][]*taskmodel.Task{
				one:   {taskTwo, taskThree},
				three: {taskFour},
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
		{two, taskmodel.TaskUpdate{LatestCompleted: &aTime, LatestScheduled: &aTime}},
	}, tasks.updates); diff != "" {
		t.Errorf("unexpected updates to task service %v", diff)
	}

	if diff := cmp.Diff([]*taskmodel.Task{
		taskTwo,
	}, coordinator.tasks); diff != "" {
		t.Errorf("unexpected tasks sent to coordinator %v", diff)
	}
}

type coordinator struct {
	tasks []*taskmodel.Task
}

func (c *coordinator) TaskCreated(_ context.Context, task *taskmodel.Task) error {
	c.tasks = append(c.tasks, task)

	return nil
}

// TasksService mocking
type taskService struct {
	// paginated tasks
	pageOne    []*taskmodel.Task
	otherPages map[platform.ID][]*taskmodel.Task

	// find tasks call
	filter taskmodel.TaskFilter
	// update call
	updates []update
}

type update struct {
	ID     platform.ID
	Update taskmodel.TaskUpdate
}

func (t *taskService) UpdateTask(_ context.Context, id platform.ID, upd taskmodel.TaskUpdate) (*taskmodel.Task, error) {
	t.updates = append(t.updates, update{id, upd})

	return allTasks[id], nil
}

func (t *taskService) FindTasks(_ context.Context, filter taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
	t.filter = filter

	if filter.After == nil {
		return t.pageOne, len(t.pageOne), nil
	}

	tasks := t.otherPages[*filter.After]
	return tasks, len(tasks), nil
}
