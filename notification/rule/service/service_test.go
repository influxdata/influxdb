package service

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	influxdb "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	endpointservice "github.com/influxdata/influxdb/v2/notification/endpoint/service"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/secret"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestInmemNotificationRuleStore(t *testing.T) {
	NotificationRuleStore(initInmemNotificationRuleStore, t)
}

func initInmemNotificationRuleStore(f NotificationRuleFields, t *testing.T) (influxdb.NotificationRuleStore, influxdb.TaskService, func()) {
	store := inmem.NewKVStore()
	if err := all.Up(context.Background(), zaptest.NewLogger(t), store); err != nil {
		t.Fatal(err)
	}

	svc, tsvc, closeSvc := initNotificationRuleStore(store, f, t)
	return svc, tsvc, func() {
		closeSvc()
	}
}

func initBoltNotificationRuleStore(f NotificationRuleFields, t *testing.T) (influxdb.NotificationRuleStore, influxdb.TaskService, func()) {
	store, closeBolt, err := newTestBoltStore(t)
	if err != nil {
		t.Fatal(err)
	}

	svc, tsvc, closeSvc := initNotificationRuleStore(store, f, t)
	return svc, tsvc, func() {
		closeSvc()
		closeBolt()
	}
}

func TestBoltNotificationRuleStore(t *testing.T) {
	NotificationRuleStore(initBoltNotificationRuleStore, t)
}

func initNotificationRuleStore(s kv.Store, f NotificationRuleFields, t *testing.T) (influxdb.NotificationRuleStore, influxdb.TaskService, func()) {
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

func withOrgID(store *tenant.Store, orgID influxdb.ID, fn func()) {
	backup := store.OrgIDGen
	defer func() { store.OrgIDGen = backup }()

	store.OrgIDGen = mock.NewStaticIDGenerator(orgID)

	fn()
}

func newTestBoltStore(t *testing.T) (kv.SchemaStore, func(), error) {
	f, err := ioutil.TempFile("", "influxdata-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	ctx := context.Background()
	logger := zaptest.NewLogger(t)
	path := f.Name()

	// skip fsync to improve test performance
	s := bolt.NewKVStore(logger, path, bolt.WithNoSync)
	if err := s.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	if err := all.Up(ctx, logger, s); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}
