package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
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

			auth := &influxdb.Authorization{
				OrgID:       o.ID,
				UserID:      u.ID,
				Permissions: influxdb.OperPermissions(),
			}
			if err := service.CreateAuthorization(context.Background(), auth); err != nil {
				t.Fatal(err)
			}

			return &servicetest.System{
				TaskControlService: service,
				TaskService:        service,
				I:                  service,
				Ctx:                ctx,
				CredsFunc:          credsFunc(u, o, auth),
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

			auth := &influxdb.Authorization{
				OrgID:       o.ID,
				UserID:      u.ID,
				Permissions: influxdb.OperPermissions(),
			}
			if err := service.CreateAuthorization(context.Background(), auth); err != nil {
				t.Fatal(err)
			}

			return &servicetest.System{
				TaskControlService: service,
				TaskService:        service,
				I:                  service,
				Ctx:                ctx,
				CredsFunc:          credsFunc(u, o, auth),
			}, cancelFunc
		},
		"transactional",
	)
}

func credsFunc(u *influxdb.User, o *influxdb.Organization, auth *influxdb.Authorization) func() (servicetest.TestCreds, error) {

	return func() (servicetest.TestCreds, error) {
		return servicetest.TestCreds{
			OrgID:           o.ID,
			Org:             o.Name,
			UserID:          u.ID,
			AuthorizationID: auth.ID,
			Token:           auth.Token,
		}, nil
	}
}
