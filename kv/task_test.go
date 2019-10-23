package kv_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kv"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/servicetest"
)

func TestInmemTaskService(t *testing.T) {
	servicetest.TestTaskService(
		t,
		func(t *testing.T) (*servicetest.System, context.CancelFunc) {
			store, close, err := NewTestInmemStore()
			if err != nil {
				t.Fatal(err)
			}

			service := kv.NewService(store)
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

func TestBoltTaskService(t *testing.T) {
	servicetest.TestTaskService(
		t,
		func(t *testing.T) (*servicetest.System, context.CancelFunc) {
			store, close, err := NewTestBoltStore()
			if err != nil {
				t.Fatal(err)
			}

			service := kv.NewService(store)
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

func TestNextRunDue(t *testing.T) {
	store, close, err := NewTestBoltStore()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	service := kv.NewService(store)
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

	task1, err := service.CreateTask(ctx, influxdb.TaskCreate{
		Flux:           `option task = {name: "a task",cron: "0 * * * *", offset: 20s} from(bucket:"test") |> range(start:-1h)`,
		OrganizationID: o.ID,
		OwnerID:        u.ID,
	})
	if err != nil {
		t.Fatal(err)
	}

	task2, err := service.CreateTask(ctx, influxdb.TaskCreate{
		Flux:           `option task = {name: "a task",every: 1h, offset: 20s} from(bucket:"test") |> range(start:-1h)`,
		OrganizationID: o.ID,
		OwnerID:        u.ID,
	})
	if err != nil {
		t.Fatal(err)
	}

	task3, err := service.CreateTask(ctx, influxdb.TaskCreate{
		Flux:           `option task = {name: "a task",every: 1h} from(bucket:"test") |> range(start:-1h)`,
		OrganizationID: o.ID,
		OwnerID:        u.ID,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, task := range []*influxdb.Task{task1, task2, task3} {
		nd, err := service.NextDueRun(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}

		run, err := service.CreateNextRun(ctx, task.ID, time.Now().Add(time.Hour).Unix())
		if err != nil {
			t.Fatal(err)
		}

		// +20 to account for the 20 second offset in the flux script
		oldNextDue := run.Created.Now
		if task.Offset != 0 {
			oldNextDue += 20
		}
		if oldNextDue != nd {
			t.Fatalf("expected nextRunDue and created run to match, %d, %d", nd, run.Created.Now)
		}
		nd1, err := service.NextDueRun(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}

		if run.NextDue != nd1 {
			t.Fatalf("expected returned next run to be the same as teh next due after scheduling %d, %d", run.NextDue, nd1)
		}
	}

}

func TestRetrieveTaskWithBadAuth(t *testing.T) {
	store, close, err := NewTestInmemStore()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	service := kv.NewService(store)
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
		Flux:           `option task = {name: "a task",every: 1h} from(bucket:"test") |> range(start:-1h)`,
		OrganizationID: o.ID,
		OwnerID:        u.ID,
		Status:         string(backend.TaskActive),
	})
	if err != nil {
		t.Fatal(err)
	}

	// convert task to old one with a bad auth
	err = store.Update(ctx, func(tx kv.Tx) error {
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
	newTask, err := service.FindTaskByID(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if newTask.ID != task.ID {
		t.Fatal("miss matching taskID's")
	}

	tasks, _, err := service.FindTasks(ctx, influxdb.TaskFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatal("failed to return task")
	}

	// test status filter
	active := true
	tasksWithActiveFilter, _, err := service.FindTasks(ctx, influxdb.TaskFilter{Active: &active})
	if err != nil {
		t.Fatal("could not find tasks")
	}
	if len(tasksWithActiveFilter) != 1 {
		t.Fatal("failed to find active task with filter")
	}
}
