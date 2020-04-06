package kv_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kv"
	_ "github.com/influxdata/influxdb/v2/query/builtin"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/task/servicetest"
	"go.uber.org/zap/zaptest"
)

func TestBoltTaskService(t *testing.T) {
	servicetest.TestTaskService(
		t,
		func(t *testing.T) (*servicetest.System, context.CancelFunc) {
			store, close, err := NewTestBoltStore(t)
			if err != nil {
				t.Fatal(err)
			}

			service := kv.NewService(zaptest.NewLogger(t), store, kv.ServiceConfig{
				FluxLanguageService: fluxlang.DefaultService,
			})
			ctx, cancelFunc := context.WithCancel(context.Background())
			if err := service.Initialize(ctx); err != nil {
				t.Fatalf("error initializing urm service: %v", err)
			}

			go func() {
				<-ctx.Done()
				close()
			}()

			return &servicetest.System{
				TaskControlService: service,
				TaskService:        service,
				I:                  service,
				Ctx:                ctx,
			}, cancelFunc
		},
		"transactional",
	)
}

type testService struct {
	Store   kv.Store
	Service *kv.Service
	Org     influxdb.Organization
	User    influxdb.User
	Auth    influxdb.Authorization
	Clock   clock.Clock

	storeCloseFn func()
}

func (s *testService) Close() {
	s.storeCloseFn()
}

func newService(t *testing.T, ctx context.Context, c clock.Clock) *testService {
	t.Helper()

	if c == nil {
		c = clock.New()
	}

	ts := &testService{}
	var err error
	ts.Store, ts.storeCloseFn, err = NewTestInmemStore(t)
	if err != nil {
		t.Fatal("failed to create InmemStore", err)
	}

	ts.Service = kv.NewService(zaptest.NewLogger(t), ts.Store, kv.ServiceConfig{
		Clock:               c,
		FluxLanguageService: fluxlang.DefaultService,
	})
	err = ts.Service.Initialize(ctx)
	if err != nil {
		t.Fatal("Service.Initialize", err)
	}

	ts.User = influxdb.User{Name: t.Name() + "-user"}
	if err := ts.Service.CreateUser(ctx, &ts.User); err != nil {
		t.Fatal(err)
	}
	ts.Org = influxdb.Organization{Name: t.Name() + "-org"}
	if err := ts.Service.CreateOrganization(ctx, &ts.Org); err != nil {
		t.Fatal(err)
	}

	if err := ts.Service.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   ts.Org.ID,
		UserID:       ts.User.ID,
		UserType:     influxdb.Owner,
	}); err != nil {
		t.Fatal(err)
	}

	ts.Auth = influxdb.Authorization{
		OrgID:       ts.Org.ID,
		UserID:      ts.User.ID,
		Permissions: influxdb.OperPermissions(),
	}
	if err := ts.Service.CreateAuthorization(context.Background(), &ts.Auth); err != nil {
		t.Fatal(err)
	}

	return ts
}

func TestRetrieveTaskWithBadAuth(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	ts := newService(t, ctx, nil)
	defer ts.Close()

	ctx = icontext.SetAuthorizer(ctx, &ts.Auth)

	task, err := ts.Service.CreateTask(ctx, influxdb.TaskCreate{
		Flux:           `option task = {name: "a task",every: 1h} from(bucket:"test") |> range(start:-1h)`,
		OrganizationID: ts.Org.ID,
		OwnerID:        ts.User.ID,
		Status:         string(influxdb.TaskActive),
	})
	if err != nil {
		t.Fatal(err)
	}

	// convert task to old one with a bad auth
	err = ts.Store.Update(ctx, func(tx kv.Tx) error {
		b, err := tx.Bucket([]byte("tasksv1"))
		if err != nil {
			return err
		}
		bID, err := task.ID.Encode()
		if err != nil {
			return err
		}
		task.OwnerID = influxdb.ID(1)
		task.AuthorizationID = influxdb.ID(132) // bad id or an id that doesnt match any auth
		tbyte, err := json.Marshal(task)
		if err != nil {
			return err
		}
		// have to actually hack the bytes here because the system doesnt like us to encode bad id's.
		tbyte = bytes.Replace(tbyte, []byte(`,"ownerID":"0000000000000001"`), []byte{}, 1)
		if err := b.Put(bID, tbyte); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// lets see if we can list and find the task
	newTask, err := ts.Service.FindTaskByID(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if newTask.ID != task.ID {
		t.Fatal("miss matching taskID's")
	}

	tasks, _, err := ts.Service.FindTasks(ctx, influxdb.TaskFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatal("failed to return task")
	}

	// test status filter
	active := string(influxdb.TaskActive)
	tasksWithActiveFilter, _, err := ts.Service.FindTasks(ctx, influxdb.TaskFilter{Status: &active})
	if err != nil {
		t.Fatal("could not find tasks")
	}
	if len(tasksWithActiveFilter) != 1 {
		t.Fatal("failed to find active task with filter")
	}
}

func TestService_UpdateTask_InactiveToActive(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	c := clock.NewMock()
	c.Set(time.Unix(1000, 0))

	ts := newService(t, ctx, c)
	defer ts.Close()

	ctx = icontext.SetAuthorizer(ctx, &ts.Auth)

	originalTask, err := ts.Service.CreateTask(ctx, influxdb.TaskCreate{
		Flux:           `option task = {name: "a task",every: 1h} from(bucket:"test") |> range(start:-1h)`,
		OrganizationID: ts.Org.ID,
		OwnerID:        ts.User.ID,
		Status:         string(influxdb.TaskActive),
	})
	if err != nil {
		t.Fatal("CreateTask", err)
	}

	v := influxdb.TaskStatusInactive
	c.Add(1 * time.Second)
	exp := c.Now()
	updatedTask, err := ts.Service.UpdateTask(ctx, originalTask.ID, influxdb.TaskUpdate{Status: &v, LatestCompleted: &exp, LatestScheduled: &exp})
	if err != nil {
		t.Fatal("UpdateTask", err)
	}

	if got := updatedTask.LatestScheduled; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
	if got := updatedTask.LatestCompleted; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}

	c.Add(10 * time.Second)
	exp = c.Now()
	v = influxdb.TaskStatusActive
	updatedTask, err = ts.Service.UpdateTask(ctx, originalTask.ID, influxdb.TaskUpdate{Status: &v})
	if err != nil {
		t.Fatal("UpdateTask", err)
	}

	if got := updatedTask.LatestScheduled; !got.Equal(exp) {
		t.Fatalf("unexpected -got/+exp\n%s", cmp.Diff(got.String(), exp.String()))
	}
}

func TestTaskRunCancellation(t *testing.T) {
	store, close, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	service := kv.NewService(zaptest.NewLogger(t), store, kv.ServiceConfig{
		FluxLanguageService: fluxlang.DefaultService,
	})
	ctx, cancelFunc := context.WithCancel(context.Background())
	if err := service.Initialize(ctx); err != nil {
		t.Fatalf("error initializing urm service: %v", err)
	}
	defer cancelFunc()
	u := &influxdb.User{Name: t.Name() + "-user"}
	if err := service.CreateUser(ctx, u); err != nil {
		t.Fatal(err)
	}
	o := &influxdb.Organization{Name: t.Name() + "-org"}
	if err := service.CreateOrganization(ctx, o); err != nil {
		t.Fatal(err)
	}

	if err := service.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   o.ID,
		UserID:       u.ID,
		UserType:     influxdb.Owner,
	}); err != nil {
		t.Fatal(err)
	}

	authz := influxdb.Authorization{
		OrgID:       o.ID,
		UserID:      u.ID,
		Permissions: influxdb.OperPermissions(),
	}
	if err := service.CreateAuthorization(context.Background(), &authz); err != nil {
		t.Fatal(err)
	}

	ctx = icontext.SetAuthorizer(ctx, &authz)

	task, err := service.CreateTask(ctx, influxdb.TaskCreate{
		Flux:           `option task = {name: "a task",cron: "0 * * * *", offset: 20s} from(bucket:"test") |> range(start:-1h)`,
		OrganizationID: o.ID,
		OwnerID:        u.ID,
	})
	if err != nil {
		t.Fatal(err)
	}

	run, err := service.CreateRun(ctx, task.ID, time.Now().Add(time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	if err := service.CancelRun(ctx, run.TaskID, run.ID); err != nil {
		t.Fatal(err)
	}

	canceled, err := service.FindRunByID(ctx, run.TaskID, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if canceled.Status != influxdb.RunCanceled.String() {
		t.Fatalf("expected task run to be cancelled")
	}
}
