package service

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	endpointservice "github.com/influxdata/influxdb/v2/notification/endpoint/service"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/secret"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestInmemNotificationRuleStore(t *testing.T) {
	NotificationRuleStore(initInmemNotificationRuleStore, t)
}

func initInmemNotificationRuleStore(f NotificationRuleFields, t *testing.T) (influxdb.NotificationRuleStore, taskmodel.TaskService, func()) {
	store := itesting.NewTestInmemStore(t)
	return initNotificationRuleStore(store, f, t)
}

func initBoltNotificationRuleStore(f NotificationRuleFields, t *testing.T) (influxdb.NotificationRuleStore, taskmodel.TaskService, func()) {
	store, closeBolt := itesting.NewTestBoltStore(t)
	svc, tsvc, closeSvc := initNotificationRuleStore(store, f, t)
	return svc, tsvc, func() {
		closeSvc()
		closeBolt()
	}
}

func TestBoltNotificationRuleStore(t *testing.T) {
	NotificationRuleStore(initBoltNotificationRuleStore, t)
}

func initNotificationRuleStore(s kv.Store, f NotificationRuleFields, t *testing.T) (influxdb.NotificationRuleStore, taskmodel.TaskService, func()) {
	logger := zaptest.NewLogger(t)

	var (
		tenantStore = tenant.NewStore(s)
		tenantSvc   = tenant.NewService(tenantStore)
	)

	kvsvc := kv.NewService(logger, s, tenantSvc, kv.ServiceConfig{
		FluxLanguageService: fluxlang.DefaultService,
	})
	kvsvc.IDGenerator = f.IDGenerator
	kvsvc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		kvsvc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	secretStore, err := secret.NewStore(s)
	require.NoError(t, err)
	secretSvc := secret.NewService(secretStore)

	endpStore := endpointservice.NewStore(s)
	endpStore.IDGenerator = f.IDGenerator
	endpStore.TimeGenerator = f.TimeGenerator
	endp := endpointservice.New(endpStore, secretSvc)

	svc, err := New(logger, s, kvsvc, tenantSvc, endp)
	if err != nil {
		t.Fatal(err)
	}

	svc.idGenerator = f.IDGenerator
	if f.TimeGenerator != nil {
		svc.timeGenerator = f.TimeGenerator
	}

	ctx := context.Background()
	for _, o := range f.Orgs {
		withOrgID(tenantStore, o.ID, func() {
			if err := tenantSvc.CreateOrganization(ctx, o); err != nil {
				t.Fatalf("failed to populate org: %v", err)
			}
		})
	}

	for _, e := range f.Endpoints {
		if err := endp.CreateNotificationEndpoint(ctx, e, 1); err != nil {
			t.Fatalf("failed to populate notification endpoint: %v", err)
		}
	}

	for _, nr := range f.NotificationRules {
		nrc := influxdb.NotificationRuleCreate{
			NotificationRule: nr,
			Status:           influxdb.Active,
		}
		if err := svc.PutNotificationRule(ctx, nrc); err != nil {
			t.Fatalf("failed to populate notification rule: %v", err)
		}
	}

	for _, c := range f.Tasks {
		if _, err := kvsvc.CreateTask(ctx, c); err != nil {
			t.Fatalf("failed to populate task: %v", err)
		}
	}

	return svc, kvsvc, func() {
		for _, nr := range f.NotificationRules {
			if err := svc.DeleteNotificationRule(ctx, nr.GetID()); err != nil {
				t.Logf("failed to remove notification rule: %v", err)
			}
		}
		for _, o := range f.Orgs {
			if err := tenantSvc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Fatalf("failed to remove org: %v", err)
			}
		}
	}
}

func withOrgID(store *tenant.Store, orgID platform.ID, fn func()) {
	backup := store.OrgIDGen
	defer func() { store.OrgIDGen = backup }()

	store.OrgIDGen = mock.NewStaticIDGenerator(orgID)

	fn()
}
