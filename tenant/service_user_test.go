package tenant_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestBoltUserService(t *testing.T) {
	influxdbtesting.UserService(initBoltUserService, t)
}

func initBoltUserService(f influxdbtesting.UserFields, t *testing.T) (influxdb.UserService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initUserService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initUserService(s kv.Store, f influxdbtesting.UserFields, t *testing.T) (influxdb.UserService, string, func()) {
	storage := tenant.NewStore(s)
	svc := tenant.NewService(storage)

	for _, u := range f.Users {
		if err := svc.CreateUser(context.Background(), u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	return svc, "tenant", func() {
		for _, u := range f.Users {
			if err := svc.DeleteUser(context.Background(), u.ID); err != nil {
				t.Logf("failed to remove users: %v", err)
			}
		}
	}
}

func TestBoltPasswordService(t *testing.T) {
	influxdbtesting.PasswordsService(initBoltPasswordsService, t)
}

func initBoltPasswordsService(f influxdbtesting.PasswordFields, t *testing.T) (influxdb.PasswordsService, func()) {
	s, closeStore, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new bolt kv store: %v", err)
	}

	svc, closeSvc := initPasswordsService(s, f, t)
	return svc, func() {
		closeSvc()
		closeStore()
	}
}

func initPasswordsService(s kv.Store, f influxdbtesting.PasswordFields, t *testing.T) (influxdb.PasswordsService, func()) {
	storage := tenant.NewStore(s)
	svc := tenant.NewService(storage)

	for _, u := range f.Users {
		if err := svc.CreateUser(context.Background(), u); err != nil {
			t.Fatalf("error populating users: %v", err)
		}
	}

	for i := range f.Passwords {
		if err := svc.SetPassword(context.Background(), f.Users[i].ID, f.Passwords[i]); err != nil {
			t.Fatalf("error setting passsword user, %s %s: %v", f.Users[i].Name, f.Passwords[i], err)
		}
	}

	return svc, func() {
		for _, u := range f.Users {
			if err := svc.DeleteUser(context.Background(), u.ID); err != nil {
				t.Logf("error removing users: %v", err)
			}
		}
	}
}
