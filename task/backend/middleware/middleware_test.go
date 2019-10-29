package middleware_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	platform "github.com/influxdata/influxdb"
	pmock "github.com/influxdata/influxdb/mock"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/snowflake"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/backend/coordinator"
	"github.com/influxdata/influxdb/task/backend/middleware"
	"github.com/influxdata/influxdb/task/mock"
	"go.uber.org/zap/zaptest"
)

func timeoutSelector(ch <-chan *platform.Task) (*platform.Task, error) {
	select {
	case task := <-ch:
		return task, nil
	case <-time.After(10 * time.Second):
		return nil, errors.New("timeout on select")
	}
}

const script = `option task = {name: "a task",cron: "* * * * *"} from(bucket:"test") |> range(start:-1h)`

// TODO(lh): Once we have a kv.TaskService this entire part can be replaced with kv.TaskService using a inmem kv.Store
func inmemTaskService() platform.TaskService {
	gen := snowflake.NewDefaultIDGenerator()
	tasks := map[platform.ID]*platform.Task{}
	mu := sync.Mutex{}

	ts := &pmock.TaskService{
		CreateTaskFn: func(ctx context.Context, tc platform.TaskCreate) (*platform.Task, error) {
			mu.Lock()
			defer mu.Unlock()
			id := gen.ID()
			task := &platform.Task{ID: id, Flux: tc.Flux, Status: tc.Status, OrganizationID: tc.OrganizationID, Organization: tc.Organization}
			if task.Status == "" {
				task.Status = string(backend.TaskActive)
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
		UpdateTaskFn: func(ctx context.Context, id platform.ID, upd platform.TaskUpdate) (*platform.Task, error) {
			mu.Lock()
			defer mu.Unlock()
			t, ok := tasks[id]
			if !ok {
				return nil, platform.ErrTaskNotFound
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
		FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*platform.Task, error) {
			mu.Lock()
			defer mu.Unlock()
			t, ok := tasks[id]
			if !ok {
				return nil, platform.ErrTaskNotFound
			}
			newt := *t
			return &newt, nil
		},
		FindTasksFn: func(ctx context.Context, tf platform.TaskFilter) ([]*platform.Task, int, error) {
			mu.Lock()
			defer mu.Unlock()
			if tf.After != nil {
				return []*platform.Task{}, 0, nil
			}
			rtn := []*platform.Task{}
			for _, task := range tasks {
				rtn = append(rtn, task)
			}
			return rtn, len(rtn), nil
		},
		ForceRunFn: func(ctx context.Context, id platform.ID, scheduledFor int64) (*platform.Run, error) {
			mu.Lock()
			defer mu.Unlock()
			t, ok := tasks[id]
			if !ok {
				return nil, platform.ErrTaskNotFound
			}

			return &platform.Run{ID: id, TaskID: t.ID, ScheduledFor: time.Unix(scheduledFor, 0)}, nil
		},
	}
	return ts

}

func TestCoordinatingTaskService(t *testing.T) {
	var (
		ts          = inmemTaskService()
		sched       = mock.NewScheduler()
		coord       = coordinator.New(zaptest.NewLogger(t), sched)
		middleware  = middleware.New(ts, coord)
		createChan  = sched.TaskCreateChan()
		releaseChan = sched.TaskReleaseChan()
		updateChan  = sched.TaskUpdateChan()
	)

	task, err := middleware.CreateTask(context.Background(), platform.TaskCreate{OrganizationID: 1, Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	createdTask, err := timeoutSelector(createChan)
	if err != nil {
		t.Fatal(err)
	}
	if task.ID != createdTask.ID {
		t.Fatal("task given to scheduler not the same as task created")
	}

	if task.Flux != script {
		t.Fatal("task sent to scheduler doesnt match task created")
	}

	if err := middleware.DeleteTask(context.Background(), task.ID); err != nil {
		t.Fatal(err)
	}

	task, err = timeoutSelector(releaseChan)
	if err != nil {
		t.Fatal(err)
	}

	if task.Flux != script {
		t.Fatal("task sent to scheduler doesn't match task created")
	}

	task, err = middleware.CreateTask(context.Background(), platform.TaskCreate{OrganizationID: 1, Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	_, err = timeoutSelector(createChan)
	if err != nil {
		t.Fatal(err)
	}
	inactive := string(backend.TaskInactive)
	res, err := middleware.UpdateTask(context.Background(), task.ID, platform.TaskUpdate{Status: &inactive})
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

	task, err = timeoutSelector(releaseChan)
	if err != nil {
		t.Fatal(err)
	}

	if task.Flux != script {
		t.Fatal("task sent to scheduler doesnt match task created")
	}

	active := string(backend.TaskActive)
	if _, err := middleware.UpdateTask(context.Background(), task.ID, platform.TaskUpdate{Status: &active}); err != nil {
		t.Fatal(err)
	}

	task, err = timeoutSelector(createChan)
	if err != nil {
		t.Fatal(err)
	}

	if task.Flux != script {
		t.Fatal("task sent to scheduler doesnt match task created")
	}

	newScript := `option task = {name: "a task",cron: "1 * * * *"} from(bucket:"test") |> range(start:-2h)`
	if _, err := middleware.UpdateTask(context.Background(), task.ID, platform.TaskUpdate{Flux: &newScript}); err != nil {
		t.Fatal(err)
	}

	task, err = timeoutSelector(updateChan)
	if err != nil {
		t.Fatal(err)
	}

	if task.Flux != newScript {
		t.Fatal("task sent to scheduler doesnt match task created")
	}
}

func TestCoordinatingTaskService_ClaimTaskUpdatesLatestCompleted(t *testing.T) {
	t.Skip("https://github.com/influxdata/influxdb/issues/14797")
	t.Parallel()
	var (
		ts         = inmemTaskService()
		sched      = mock.NewScheduler()
		coord      = coordinator.New(zaptest.NewLogger(t), sched)
		latest     = time.Now().UTC().Add(time.Second)
		middleware = middleware.New(ts, coord, middleware.WithNowFunc(func() time.Time {
			return latest
		}))
	)

	task, err := middleware.CreateTask(context.Background(), platform.TaskCreate{OrganizationID: 1, Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	rchan := sched.TaskReleaseChan()
	activeStr := string(backend.TaskActive)
	inactiveStr := string(backend.TaskInactive)

	task, err = middleware.UpdateTask(context.Background(), task.ID, platform.TaskUpdate{Status: &inactiveStr})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-rchan:
	case <-time.After(time.Second):
		t.Fatal("failed to release claimed task")
	}

	cchan := sched.TaskCreateChan()

	_, err = middleware.UpdateTask(context.Background(), task.ID, platform.TaskUpdate{Status: &activeStr})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case claimedTask := <-cchan:
		if claimedTask.LatestCompleted != latest.UTC() {
			t.Fatal("failed up update latest completed in claimed task")
		}
	case <-time.After(time.Second):
		t.Fatal("failed to release claimed task")
	}

}

func TestCoordinatingTaskService_DeleteUnclaimedTask(t *testing.T) {
	var (
		ts         = inmemTaskService()
		sched      = mock.NewScheduler()
		coord      = coordinator.New(zaptest.NewLogger(t), sched)
		middleware = middleware.New(ts, coord)
	)

	// Create an isolated task directly through the store so the coordinator doesn't know about it.
	task, err := ts.CreateTask(context.Background(), platform.TaskCreate{OrganizationID: 1, Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	// Deleting the task through the coordinator should succeed.
	if err := middleware.DeleteTask(context.Background(), task.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := ts.FindTaskByID(context.Background(), task.ID); err != platform.ErrTaskNotFound {
		t.Fatalf("expected deleted task not to be found; got %v", err)
	}
}

func TestCoordinatingTaskService_ForceRun(t *testing.T) {
	var (
		ts         = inmemTaskService()
		sched      = mock.NewScheduler()
		coord      = coordinator.New(zaptest.NewLogger(t), sched)
		middleware = middleware.New(ts, coord)
	)

	// Create an isolated task directly through the store so the coordinator doesn't know about it.
	task, err := middleware.CreateTask(context.Background(), platform.TaskCreate{OrganizationID: 1, Flux: script})
	if err != nil {
		t.Fatal(err)
	}

	task, err = middleware.FindTaskByID(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}

	ch := sched.TaskUpdateChan()
	manualRunTime := time.Now().Unix()
	if _, err := middleware.ForceRun(context.Background(), task.ID, manualRunTime); err != nil {
		t.Fatal(err)
	}

	select {
	case <-ch:
		// great!
	case <-time.After(time.Second):
		t.Fatal("didn't receive task update in time")
	}
}
