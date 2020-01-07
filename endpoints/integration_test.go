//+build integration

package endpoints_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/endpoints"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestService(t *testing.T) {
	influxdbtesting.NotificationEndpointService(initBoltNotificationEndpointService, t)
}

func initNotificationEndpointService(s kv.Store, f influxdbtesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, influxdbtesting.NotificationEndpointDeps, func()) {
	kvSVC := kv.NewService(zaptest.NewLogger(t), s)
	kvSVC.IDGenerator = f.IDGenerator
	kvSVC.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		kvSVC.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	ctx := context.Background()
	require.NoError(t, kvSVC.Initialize(ctx))

	store := endpoints.NewStore(s)
	require.NoError(t, store.Init(ctx))

	endpointSVC := endpoints.NewService(
		endpoints.WithStore(store),
		endpoints.WithIDGenerator(f.IDGenerator),
		endpoints.WithTimeGenerator(f.TimeGenerator),
		endpoints.WithOrgSVC(kvSVC),
		endpoints.WithSecretSVC(kvSVC),
		endpoints.WithUserResourceMappingSVC(kvSVC),
	)

	for _, edp := range f.NotificationEndpoints {
		require.NoError(t, store.Put(ctx, edp))
	}

	for _, o := range f.Orgs {
		require.NoError(t, kvSVC.PutOrganization(ctx, o))
	}

	for _, m := range f.UserResourceMappings {
		require.NoError(t, kvSVC.CreateUserResourceMapping(ctx, m), "failed to populate user resource mapping")
	}

	deps := influxdbtesting.NotificationEndpointDeps{
		SecretSVC:              kvSVC,
		UserResourceMappingSVC: kvSVC,
	}

	doneFn := func() {
		for _, edp := range f.NotificationEndpoints {
			if err := endpointSVC.Delete(ctx, edp.GetID()); err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
				t.Logf("failed to remove notification endpoint: %v", err)
			}
		}
		for _, urm := range f.UserResourceMappings {
			if err := kvSVC.DeleteUserResourceMapping(ctx, urm.ResourceID, urm.UserID); err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
				t.Logf("failed to remove urm rule: %v", err)
			}
		}
		for _, o := range f.Orgs {
			require.NoError(t, kvSVC.DeleteOrganization(ctx, o.ID), "failed to remove org")
		}
	}

	return endpointSVC, deps, doneFn
}

func initBoltNotificationEndpointService(f influxdbtesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, influxdbtesting.NotificationEndpointDeps, func()) {
	store, closeBolt := newTestBoltStore(t)

	svc, secretSVC, closeSvc := initNotificationEndpointService(store, f, t)
	return svc, secretSVC, func() {
		closeSvc()
		closeBolt()
	}
}

func newTestBoltStore(t *testing.T) (kv.Store, func()) {
	t.Helper()

	f, err := ioutil.TempFile("", "influxdata-bolt-")
	require.NoError(t, err, "unable to open temporary boltdb file")
	require.NoError(t, f.Close())

	path := f.Name()
	s := bolt.NewKVStore(zaptest.NewLogger(t), path)
	require.NoError(t, s.Open(context.Background()))

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close
}
