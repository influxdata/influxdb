package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltAuthorizationService(t *testing.T) {
	influxdbtesting.AuthorizationService(initBoltAuthorizationService, t)
}

func TestInmemAuthorizationService(t *testing.T) {
	influxdbtesting.AuthorizationService(initInmemAuthorizationService, t)
}

func initBoltAuthorizationService(f influxdbtesting.AuthorizationFields, t *testing.T) (influxdb.AuthorizationService, string, func()) {
	s, closeBolt, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initAuthorizationService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initInmemAuthorizationService(f influxdbtesting.AuthorizationFields, t *testing.T) (influxdb.AuthorizationService, string, func()) {
	s, closeBolt, err := NewTestInmemStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initAuthorizationService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initAuthorizationService(s kv.Store, f influxdbtesting.AuthorizationFields, t *testing.T) (influxdb.AuthorizationService, string, func()) {
	svc := kv.NewService(s)
	svc.IDGenerator = f.IDGenerator
	svc.TokenGenerator = f.TokenGenerator

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing authorization service: %v", err)
	}

	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	for _, o := range f.Orgs {
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate orgs")
		}
	}

	for _, a := range f.Authorizations {
		if err := svc.PutAuthorization(ctx, a); err != nil {
			t.Fatalf("failed to populate authorizations %s", err)
		}
	}

	return svc, kv.OpPrefix, func() {
		for _, u := range f.Users {
			if err := svc.DeleteUser(ctx, u.ID); err != nil {
				t.Logf("failed to remove user: %v", err)
			}
		}

		for _, o := range f.Orgs {
			if err := svc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove org: %v", err)
			}
		}

		for _, a := range f.Authorizations {
			if err := svc.DeleteAuthorization(ctx, a.ID); err != nil {
				t.Logf("failed to remove authorizations: %v", err)
			}
		}
	}
}
