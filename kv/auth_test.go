package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltAuthorizationService(t *testing.T) {
	influxdbtesting.AuthorizationService(initBoltAuthorizationService, t)
}

func initBoltAuthorizationService(f influxdbtesting.AuthorizationFields, t *testing.T) (influxdb.AuthorizationService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initAuthorizationService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initAuthorizationService(s kv.SchemaStore, f influxdbtesting.AuthorizationFields, t *testing.T) (influxdb.AuthorizationService, string, func()) {
	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.IDGenerator = f.IDGenerator
	svc.OrgIDs = f.OrgIDGenerator
	svc.TokenGenerator = f.TokenGenerator
	svc.TimeGenerator = f.TimeGenerator

	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	for _, o := range f.Orgs {
		o.ID = svc.OrgIDs.ID()
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
