package checks

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	_ "github.com/influxdata/influxdb/v2/query/builtin"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"go.uber.org/zap/zaptest"
)

func NewKVTestStore(t *testing.T) (kv.Store, func()) {
	return inmem.NewKVStore(), func() {}
}

func TestCheckService(t *testing.T) {
	CheckService(initCheckService, t)
}

func initCheckService(f CheckFields, t *testing.T) (influxdb.CheckService, influxdb.TaskService, string, func()) {
	store, closeKVStore := NewKVTestStore(t)
	logger := zaptest.NewLogger(t)
	svc := kv.NewService(logger, store, kv.ServiceConfig{
		FluxLanguageService: fluxlang.DefaultService,
	})
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	checkService := NewService(logger, store, svc, svc)
	checkService.idGenerator = f.IDGenerator
	if f.TimeGenerator != nil {
		checkService.timeGenerator = f.TimeGenerator
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
		if err := checkService.PutCheck(ctx, c); err != nil {
			t.Fatalf("failed to populate checks")
		}
	}
	for _, tc := range f.Tasks {
		if _, err := svc.CreateTask(ctx, tc); err != nil {
			t.Fatalf("failed to populate tasks: %v", err)
		}
	}
	return checkService, svc, kv.OpPrefix, func() {
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
			if err := checkService.DeleteCheck(ctx, c.GetID()); err != nil {
				t.Logf("failed to remove check: %v", err)
			}
		}

		closeKVStore()
	}
}
