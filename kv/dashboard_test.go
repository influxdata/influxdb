package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltDashboardService(t *testing.T) {
	influxdbtesting.DashboardService(initBoltDashboardService, t)
}

func TestInmemDashboardService(t *testing.T) {
	influxdbtesting.DashboardService(initInmemDashboardService, t)
}

func initBoltDashboardService(f influxdbtesting.DashboardFields, t *testing.T) (influxdb.DashboardService, string, func()) {
	s, closeBolt, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initDashboardService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initInmemDashboardService(f influxdbtesting.DashboardFields, t *testing.T) (influxdb.DashboardService, string, func()) {
	s, closeBolt, err := NewTestInmemStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initDashboardService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initDashboardService(s kv.Store, f influxdbtesting.DashboardFields, t *testing.T) (influxdb.DashboardService, string, func()) {

	if f.NowFn == nil {
		f.NowFn = time.Now
	}
	svc := kv.NewService(s)
	svc.IDGenerator = f.IDGenerator
	svc.WithTime(f.NowFn)

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing organization service: %v", err)
	}

	for _, b := range f.Dashboards {
		if err := svc.PutDashboard(ctx, b); err != nil {
			t.Fatalf("failed to populate dashboards")
		}
	}
	return svc, kv.OpPrefix, func() {
		for _, b := range f.Dashboards {
			if err := svc.DeleteDashboard(ctx, b.ID); err != nil {
				t.Logf("failed to remove dashboard: %v", err)
			}
		}
	}
}
