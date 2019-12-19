package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/endpoints"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
	"go.uber.org/zap/zaptest"
)

func TestNotificationEndpointService(t *testing.T) {
	tests := []struct {
		name string
		fn   func(f influxdbtesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, influxdb.SecretService, func())
	}{
		{
			name: "bolt",
			fn:   initBoltNotificationEndpointService,
		},
		{
			name: "inmem",
			fn:   initInmemNotificationEndpointService,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			influxdbtesting.NotificationEndpointService(tt.fn, t)
		})
	}
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

func initInmemNotificationEndpointService(f influxdbtesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, influxdb.SecretService, func()) {
	s, closeInmem, err := NewTestInmemStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, secretSVC, closeSvc := initNotificationEndpointService(s, f, t)
	return svc, secretSVC, func() {
		closeSvc()
		closeInmem()
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
