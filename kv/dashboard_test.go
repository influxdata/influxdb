package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltDashboardService(t *testing.T) {
	influxdbtesting.DashboardService(initBoltDashboardService, t)
}

func initBoltDashboardService(f influxdbtesting.DashboardFields, t *testing.T) (influxdb.DashboardService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initDashboardService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initDashboardService(s kv.SchemaStore, f influxdbtesting.DashboardFields, t *testing.T) (influxdb.DashboardService, string, func()) {

	if f.TimeGenerator == nil {
		f.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator

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
