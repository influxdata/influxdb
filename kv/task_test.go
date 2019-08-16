package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kv"
	_ "github.com/influxdata/influxdb/query/builtin"
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

	task, err := service.CreateTask(ctx, influxdb.TaskCreate{
		Flux:           `option task = {name: "a task",every: 1h} from(bucket:"test") |> range(start:-1h)`,
		OrganizationID: o.ID,
		OwnerID:        u.ID,
	})
	if err != nil {
		t.Fatal(err)
	}

	nd, err := service.NextDueRun(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}

	run, err := service.CreateNextRun(ctx, task.ID, time.Now().Add(time.Hour).Unix())
	if err != nil {
		t.Fatal(err)
	}

	if run.Created.Now != nd {
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
