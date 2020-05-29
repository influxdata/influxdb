package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/endpoints"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestNotificationEndpointService(t *testing.T) {
	influxdbtesting.NotificationEndpointService(initBoltNotificationEndpointService, t)
}

func initBoltNotificationEndpointService(f influxdbtesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, influxdb.SecretService, func()) {
	store, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, secretSVC, closeSvc := initNotificationEndpointService(store, f, t)
	return svc, secretSVC, func() {
		closeSvc()
		closeBolt()
	}
}

func initNotificationEndpointService(s kv.Store, f influxdbtesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, influxdb.SecretService, func()) {
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing user service: %v", err)
	}

	for _, edp := range f.NotificationEndpoints {
		if err := svc.PutNotificationEndpoint(ctx, edp); err != nil {
			t.Fatalf("failed to populate notification endpoint: %v", err)
		}
	}

	for _, o := range f.Orgs {
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate org: %v", err)
		}
	}

	for _, m := range f.UserResourceMappings {
		if err := svc.CreateUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate user resource mapping: %v", err)
		}
	}

	endpointSVC := endpoints.NewService(svc, svc, svc, svc)
	return endpointSVC, svc, func() {
		for _, edp := range f.NotificationEndpoints {
			if _, _, err := svc.DeleteNotificationEndpoint(ctx, edp.GetID()); err != nil && err != kv.ErrNotificationEndpointNotFound {
				t.Logf("failed to remove notification endpoint: %v", err)
			}
		}
		for _, o := range f.Orgs {
			if err := svc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Fatalf("failed to remove org: %v", err)
			}
		}

		for _, urm := range f.UserResourceMappings {
			if err := svc.DeleteUserResourceMapping(ctx, urm.ResourceID, urm.UserID); err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
				t.Logf("failed to remove urm rule: %v", err)
			}
		}
	}
}
