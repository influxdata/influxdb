package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltNotificationEndpointService(t *testing.T) {
	influxdbtesting.NotificationEndpointService(initBoltNotificationEndpointService, t)
}

func TestNotificationEndpointService(t *testing.T) {
	influxdbtesting.NotificationEndpointService(initInmemNotificationEndpointService, t)
}

func initBoltNotificationEndpointService(f influxdbtesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, func()) {
	s, closeBolt, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initNotificationEndpointService(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initInmemNotificationEndpointService(f influxdbtesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, func()) {
	s, closeInmem, err := NewTestInmemStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initNotificationEndpointService(s, f, t)
	return svc, func() {
		closeSvc()
		closeInmem()
	}
}

func initNotificationEndpointService(s kv.Store, f influxdbtesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, func()) {
	svc := kv.NewService(s)
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing user service: %v", err)
	}

	for _, s := range f.Secrets {
		for k, v := range s.Env {
			if err := svc.PutSecret(ctx, s.OrganizationID, k, v); err != nil {
				t.Fatalf("failed to populate secrets")
			}
		}
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

	return svc, func() {
		for _, edp := range f.NotificationEndpoints {
			if err := svc.DeleteNotificationEndpoint(ctx, edp.GetID()); err != nil {
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

		for _, s := range f.Secrets {
			for k, v := range s.Env {
				if err := svc.DeleteSecret(ctx, s.OrganizationID, k, v); err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
					t.Fatalf("failed to populate secrets")
				}
			}
		}
	}
}
