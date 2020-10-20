package service_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	influxdb "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/notification/endpoint/service"
	endpointsTesting "github.com/influxdata/influxdb/v2/notification/endpoint/service/testing"
	"github.com/influxdata/influxdb/v2/secret"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestNotificationEndpointService_WithInmem(t *testing.T) {
	endpointsTesting.NotificationEndpointService(initInmemNotificationEndpointService, t)
}

func TestNotificationEndpointService_WithBolt(t *testing.T) {
	endpointsTesting.NotificationEndpointService(initBoltNotificationEndpointService, t)
}

func NewTestBoltStore(t *testing.T) (kv.SchemaStore, func(), error) {
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

func initBoltNotificationEndpointService(f endpointsTesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, influxdb.SecretService, func()) {
	store, closeStore, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatal(err)
	}

	svc, secretSVC, closeSvc := initNotificationEndpointService(store, f, t)
	return svc, secretSVC, func() {
		closeSvc()
		closeStore()
	}
}

func initInmemNotificationEndpointService(f endpointsTesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, influxdb.SecretService, func()) {
	store := inmem.NewKVStore()
	if err := all.Up(context.Background(), zaptest.NewLogger(t), store); err != nil {
		t.Fatal(err)
	}

	svc, secretSVC, closeSvc := initNotificationEndpointService(store, f, t)
	return svc, secretSVC, closeSvc
}

func initNotificationEndpointService(s kv.SchemaStore, f endpointsTesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, influxdb.SecretService, func()) {
	ctx := context.Background()

	tenantStore := tenant.NewStore(s)
	if f.IDGenerator != nil {
		tenantStore.OrgIDGen = f.IDGenerator
		tenantStore.IDGen = f.IDGenerator
	}

	tenantSvc := tenant.NewService(tenantStore)

	secretStore, err := secret.NewStore(s)
	require.NoError(t, err)
	secretSvc := secret.NewService(secretStore)

	store := service.NewStore(s)
	store.IDGenerator = f.IDGenerator
	if f.TimeGenerator != nil {
		store.TimeGenerator = f.TimeGenerator
	}

	endpointSvc := service.New(store, secretSvc)

	for _, edp := range f.NotificationEndpoints {
		if err := store.PutNotificationEndpoint(ctx, edp); err != nil {
			t.Fatalf("failed to populate notification endpoint: %v", err)
		}
	}

	for _, o := range f.Orgs {
		if err := tenantSvc.CreateOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate org: %v", err)
		}
	}

	return endpointSvc, secretSvc, func() {
		for _, edp := range f.NotificationEndpoints {
			if _, _, err := endpointSvc.DeleteNotificationEndpoint(ctx, edp.GetID()); err != nil && err != service.ErrNotificationEndpointNotFound {
				t.Logf("failed to remove notification endpoint: %v", err)
			}
		}
		for _, o := range f.Orgs {
			if err := tenantSvc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Fatalf("failed to remove org: %v", err)
			}
		}
	}
}
