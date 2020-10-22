package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltCheckService(t *testing.T) {
	influxdbtesting.CheckService(initBoltCheckService, t)
}

func initBoltCheckService(f influxdbtesting.CheckFields, t *testing.T) (influxdb.CheckService, influxdb.UserResourceMappingService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, urmSvc, op, closeSvc := initCheckService(s, f, t)
	return svc, urmSvc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initCheckService(s kv.SchemaStore, f influxdbtesting.CheckFields, t *testing.T) (influxdb.CheckService, influxdb.UserResourceMappingService, string, func()) {
	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s, kv.ServiceConfig{
		FluxLanguageService: fluxlang.DefaultService,
	})
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	tenantStore := tenant.NewStore(s)
	tenantSvc := tenant.NewService(tenantStore)

	for _, m := range f.UserResourceMappings {
		if err := tenantSvc.CreateUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate user resource mapping: %v", err)
		}
	}
	for _, o := range f.Organizations {
		withOrgID(tenantStore, o.ID, func() {
			if err := tenantSvc.CreateOrganization(ctx, o); err != nil {
				t.Fatalf("failed to populate org: %v", err)
			}
		})
	}
	for _, c := range f.Checks {
		if err := svc.PutCheck(ctx, c); err != nil {
			t.Fatalf("failed to populate checks")
		}
	}
	for _, tc := range f.Tasks {
		if _, err := svc.CreateTask(ctx, tc); err != nil {
			t.Fatalf("failed to populate tasks: %v", err)
		}
	}
	return svc, tenantSvc, kv.OpPrefix, func() {
		for _, o := range f.Organizations {
			if err := tenantSvc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove organization: %v", err)
			}
		}
		for _, urm := range f.UserResourceMappings {
			if err := tenantSvc.DeleteUserResourceMapping(ctx, urm.ResourceID, urm.UserID); err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
				t.Logf("failed to remove urm rule: %v", err)
			}
		}
		for _, c := range f.Checks {
			if err := svc.DeleteCheck(ctx, c.GetID()); err != nil {
				t.Logf("failed to remove check: %v", err)
			}
		}
	}
}

func withOrgID(store *tenant.Store, orgID influxdb.ID, fn func()) {
	backup := store.OrgIDGen
	defer func() { store.OrgIDGen = backup }()

	store.OrgIDGen = mock.NewStaticIDGenerator(orgID)

	fn()
}
