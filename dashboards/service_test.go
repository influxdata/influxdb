package dashboards

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	dashboardtesting "github.com/influxdata/influxdb/v2/dashboards/testing"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltDashboardService(t *testing.T) {
	dashboardtesting.DashboardService(initBoltDashboardService, t)
}

func initBoltDashboardService(f dashboardtesting.DashboardFields, t *testing.T) (influxdb.DashboardService, string, func()) {
	s, closeBolt := itesting.NewTestBoltStore(t)
	svc, op, closeSvc := initDashboardService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initDashboardService(s kv.SchemaStore, f dashboardtesting.DashboardFields, t *testing.T) (influxdb.DashboardService, string, func()) {
	if f.TimeGenerator == nil {
		f.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	ctx := context.Background()
	kvSvc := kv.NewService(zaptest.NewLogger(t), s, &mock.OrganizationService{})
	kvSvc.IDGenerator = f.IDGenerator
	kvSvc.TimeGenerator = f.TimeGenerator

	svc := NewService(s, kvSvc)
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
