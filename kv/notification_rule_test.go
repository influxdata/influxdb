package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltNotificationRuleStore(t *testing.T) {
	influxdbtesting.NotificationRuleStore(initBoltNotificationRuleStore, t)
}

func TestNotificationRuleStore(t *testing.T) {
	influxdbtesting.NotificationRuleStore(initInmemNotificationRuleStore, t)
}

func initBoltNotificationRuleStore(f influxdbtesting.NotificationRuleFields, t *testing.T) (influxdb.NotificationRuleStore, func()) {
	s, closeBolt, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initNotificationRuleStore(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initInmemNotificationRuleStore(f influxdbtesting.NotificationRuleFields, t *testing.T) (influxdb.NotificationRuleStore, func()) {
	s, closeBolt, err := NewTestInmemStore()
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

	for _, nr := range f.NotificationRules {
		if err := svc.PutNotificationRule(ctx, nr); err != nil {
			t.Fatalf("failed to populate notification rule: %v", err)
		}
	}

	for _, m := range f.UserResourceMappings {
		if err := svc.CreateUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate user resource mapping: %v", err)
		}
	}

	for _, o := range f.Orgs {
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate org: %v", err)
		}
	}

	return svc, func() {
		for _, nr := range f.NotificationRules {
			if err := svc.DeleteNotificationRule(ctx, nr.GetID()); err != nil {
				t.Logf("failed to remove notification rule: %v", err)
			}
		}
		for _, urm := range f.UserResourceMappings {
			if err := svc.DeleteUserResourceMapping(ctx, urm.ResourceID, urm.UserID); err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
				t.Logf("failed to remove urm rule: %v", err)
			}
		}
		for _, o := range f.Orgs {
			if err := svc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Fatalf("failed to remove org: %v", err)
			}
		}
	}
}
