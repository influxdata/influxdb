package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/endpoints"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestBoltNotificationRuleStore(t *testing.T) {
	influxdbtesting.NotificationRuleStore(initBoltNotificationRuleStore, t)
}

func initBoltNotificationRuleStore(f influxdbtesting.NotificationRuleFields, t *testing.T) (influxdb.NotificationRuleStore, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initNotificationRuleStore(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initNotificationRuleStore(s kv.Store, f influxdbtesting.NotificationRuleFields, t *testing.T) (influxdb.NotificationRuleStore, func()) {
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

	for _, o := range f.Orgs {
		require.NoError(t, kvSVC.PutOrganization(ctx, o))
	}

	for _, m := range f.UserResourceMappings {
		require.NoError(t, kvSVC.CreateUserResourceMapping(ctx, m))
	}

	for _, e := range f.Endpoints {
		require.NoError(t, endpointSVC.Create(ctx, 1, e))
	}

	for _, nr := range f.NotificationRules {
		nrc := influxdb.NotificationRuleCreate{
			NotificationRule: nr,
			Status:           influxdb.Active,
		}
		require.NoError(t, kvSVC.PutNotificationRule(ctx, nrc))
	}

	for _, c := range f.Tasks {
		_, err := kvSVC.CreateTask(ctx, c)
		require.NoError(t, err)
	}

	return kvSVC, func() {
		for _, nr := range f.NotificationRules {
			if err := kvSVC.DeleteNotificationRule(ctx, nr.GetID()); err != nil {
				t.Logf("failed to remove notification rule: %v", err)
			}
		}
		for _, urm := range f.UserResourceMappings {
			if err := kvSVC.DeleteUserResourceMapping(ctx, urm.ResourceID, urm.UserID); err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
				t.Logf("failed to remove urm rule: %v", err)
			}
		}
		for _, o := range f.Orgs {
			require.NoError(t, kvSVC.DeleteOrganization(ctx, o.ID))
		}
	}
}
