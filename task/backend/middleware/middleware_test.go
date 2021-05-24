package middleware_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/kit/platform"
	pmock "github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/task/backend"
	"github.com/influxdata/influxdb/v2/task/backend/coordinator"
	"github.com/influxdata/influxdb/v2/task/backend/middleware"
	"github.com/influxdata/influxdb/v2/task/backend/scheduler"
	"github.com/influxdata/influxdb/v2/task/mock"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"go.uber.org/zap/zaptest"
)

func timeoutSelector(ch <-chan scheduler.ID) (scheduler.ID, error) {
	select {
	case id := <-ch:
		return id, nil
	case <-time.After(10 * time.Second):
		return 0, errors.New("timeout on select")
	}
}

const script = `option task = {name: "a task",cron: "* * * * *"} from(bucket:"test") |> range(start:-1h)`

func inmemTaskService() taskmodel.TaskService {
	gen := snowflake.NewDefaultIDGenerator()
	tasks := map[platform.ID]*taskmodel.Task{}
	mu := sync.Mutex{}

	ts := &pmock.TaskService{
		CreateTaskFn: func(ctx context.Context, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {
			mu.Lock()
			defer mu.Unlock()
			id := gen.ID()
			task := &taskmodel.Task{ID: id, Flux: tc.Flux, Cron: "* * * * *", Status: tc.Status, OrganizationID: tc.OrganizationID, Organization: tc.Organization}
			if task.Status == "" {
				task.Status = string(taskmodel.TaskActive)
			}
			tasks[id] = task

			return tasks[id], nil
		},
		DeleteTaskFn: func(ctx context.Context, id platform.ID) error {
			mu.Lock()
			defer mu.Unlock()
			delete(tasks, id)
			return nil
		},
		UpdateTaskFn: func(ctx context.Context, id platform.ID, upd taskmodel.TaskUpdate) (*taskmodel.Task, error) {
			mu.Lock()
			defer mu.Unlock()
			t, ok := tasks[id]
			if !ok {
				return nil, taskmodel.ErrTaskNotFound
			}
			if upd.Flux != nil {
				t.Flux = *upd.Flux

			}
			if upd.Status != nil {
				t.Status = *upd.Status
			}
			if upd.LatestCompleted != nil {
				t.LatestCompleted = *upd.LatestCompleted
			}

			return t, nil
		},
		FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
			mu.Lock()
			defer mu.Unlock()
			t, ok := tasks[id]
			if !ok {
				return nil, taskmodel.ErrTaskNotFound
			}
			newt := *t
			return &newt, nil
		},
		FindTasksFn: func(ctx context.Context, tf taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
			mu.Lock()
			defer mu.Unlock()
			if tf.After != nil {
				return []*taskmodel.Task{}, 0, nil
			}
			rtn := []*taskmodel.Task{}
			for _, task := range tasks {
				rtn = append(rtn, task)
			}
			return rtn, len(rtn), nil
		},
		ForceRunFn: func(ctx context.Context, id platform.ID, scheduledFor int64) (*taskmodel.Run, error) {
			mu.Lock()
			defer mu.Unlock()
			t, ok := tasks[id]
			if !ok {
				return nil, taskmodel.ErrTaskNotFound
			}

			return &taskmodel.Run{ID: id, TaskID: t.ID, ScheduledFor: time.Unix(scheduledFor, 0)}, nil
		},
	}
	return ts

}

func TestCoordinatingTaskService(t *testing.T) {
	var (
		ts         = inmemTaskService()
		ex         = mock.NewExecutor()
		sch, _, _  = scheduler.NewScheduler(ex, backend.NewSchedulableTaskService(ts))
		coord      = coordinator.NewCoordinator(zaptest.NewLogger(t), sch, ex)
		middleware = middleware.New(ts, coord)
	)

	task, err := middleware.CreateTask(context.Background(), taskmodel.TaskCreate{OrganizationID: 1, Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	id, err := timeoutSelector(ex.ExecutedChan)
	if err != nil {
		t.Fatal(err)
	}

	if id != scheduler.ID(task.ID) {
		t.Fatalf("task given to scheduler not the same as task created. expected: %v, got: %v", task.ID, id)
	}

	if task.Flux != script {
		t.Fatal("task sent to scheduler doesnt match task created")
	}

	if err := middleware.DeleteTask(context.Background(), task.ID); err != nil {
		t.Fatal(err)
	}

	if task.Flux != script {
		t.Fatal("task sent to scheduler doesn't match task created")
	}

	task, err = middleware.CreateTask(context.Background(), taskmodel.TaskCreate{OrganizationID: 1, Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	inactive := string(taskmodel.TaskInactive)
	res, err := middleware.UpdateTask(context.Background(), task.ID, taskmodel.TaskUpdate{Status: &inactive})
	if err != nil {
		t.Fatal(err)
	}
	// Only validating res on the first update.
	if res.ID != task.ID {
		t.Fatalf("unexpected ID on update result: got %s, want %s", res.ID.String(), task.ID.String())
	}
	if res.Flux != task.Flux {
		t.Fatalf("unexpected script on update result: got %q, want %q", res.Flux, task.Flux)
	}
	if res.Status != inactive {
		t.Fatalf("unexpected meta status on update result: got %q, want %q", res.Status, inactive)
	}

	if task.Flux != script {
		t.Fatal("task sent to scheduler doesnt match task created")
	}

	active := string(taskmodel.TaskActive)
	if _, err := middleware.UpdateTask(context.Background(), task.ID, taskmodel.TaskUpdate{Status: &active}); err != nil {
		t.Fatal(err)
	}

	if task.Flux != script {
		t.Fatal("task sent to scheduler doesnt match task created")
	}

	newScript := `option task = {name: "a task",cron: "1 * * * *"} from(bucket:"test") |> range(start:-2h)`
	if _, err := middleware.UpdateTask(context.Background(), task.ID, taskmodel.TaskUpdate{Flux: &newScript}); err != nil {
		t.Fatal(err)
	}

	if task.Flux != newScript {
		t.Fatal("task sent to scheduler doesnt match task created")
	}
}

func TestCoordinatingTaskService_ForceRun(t *testing.T) {
	var (
		ts         = inmemTaskService()
		ex         = mock.NewExecutor()
		sch, _, _  = scheduler.NewScheduler(ex, backend.NewSchedulableTaskService(ts))
		coord      = coordinator.NewCoordinator(zaptest.NewLogger(t), sch, ex)
		middleware = middleware.New(ts, coord)
	)

	// Create an isolated task directly through the store so the coordinator doesn't know about it.
	task, err := middleware.CreateTask(context.Background(), taskmodel.TaskCreate{OrganizationID: 1, Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	id, err := timeoutSelector(ex.ExecutedChan)
	if err != nil {
		t.Fatal(err)
	}

	task, err = middleware.FindTaskByID(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}

	manualRunTime := time.Now().Unix()
	if _, err = middleware.ForceRun(context.Background(), task.ID, manualRunTime); err != nil {
		t.Fatal(err)
	}

	if platform.ID(id) != task.ID {
		t.Fatalf("expected task ID passed to scheduler to match create task ID %v, got: %v", task.ID, id)
	}

}
