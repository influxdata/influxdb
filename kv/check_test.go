package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltCheckService(t *testing.T) {
	influxdbtesting.CheckService(initBoltCheckService, t)
}

func initBoltCheckService(f influxdbtesting.CheckFields, t *testing.T) (influxdb.CheckService, *kv.Service, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initCheckService(s, f, t)
	return svc, svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initCheckService(s kv.Store, f influxdbtesting.CheckFields, t *testing.T) (*kv.Service, string, func()) {
	svc := kv.NewService(zaptest.NewLogger(t), s, kv.ServiceConfig{
		FluxLanguageService: fluxlang.DefaultService,
	})
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing check service: %v", err)
	}
	for _, m := range f.UserResourceMappings {
		if err := svc.CreateUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate user resource mapping: %v", err)
		}
	}
	for _, o := range f.Organizations {
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
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
	return svc, kv.OpPrefix, func() {
		for _, o := range f.Organizations {
			if err := svc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove organization: %v", err)
			}
		}
		for _, urm := range f.UserResourceMappings {
			if err := svc.DeleteUserResourceMapping(ctx, urm.ResourceID, urm.UserID); err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
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
