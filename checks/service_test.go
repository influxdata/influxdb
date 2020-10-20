package checks

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/tenant"
	"go.uber.org/zap/zaptest"
)

func NewKVTestStore(t *testing.T) (kv.Store, func()) {
	t.Helper()

	store := inmem.NewKVStore()

	if err := all.Up(context.Background(), zaptest.NewLogger(t), store); err != nil {
		t.Fatal(err)
	}

	return store, func() {}
}

func TestCheckService(t *testing.T) {
	CheckService(initCheckService, t)
}

func initCheckService(f CheckFields, t *testing.T) (influxdb.CheckService, influxdb.TaskService, string, func()) {
	store, closeKVStore := NewKVTestStore(t)
	logger := zaptest.NewLogger(t)

	tenantStore := tenant.NewStore(store)
	tenantSvc := tenant.NewService(tenantStore)

	svc := kv.NewService(logger, store, tenantSvc, kv.ServiceConfig{
		FluxLanguageService: fluxlang.DefaultService,
	})
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	checkService := NewService(logger, store, tenantSvc, svc)
	checkService.idGenerator = f.IDGenerator
	if f.TimeGenerator != nil {
		checkService.timeGenerator = f.TimeGenerator
	}

	ctx := context.Background()
	for _, o := range f.Organizations {
		mock.SetIDForFunc(&tenantStore.OrgIDGen, o.ID, func() {
			if err := tenantSvc.CreateOrganization(ctx, o); err != nil {
				t.Fatalf("failed to populate organizations")
			}
		})
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
			if err := tenantSvc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove organization: %v", err)
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
