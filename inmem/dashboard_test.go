package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initDashboardService(f platformtesting.DashboardFields, t *testing.T) (platform.DashboardService, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.Background()
	s.WithTime(f.NowFn)
	for _, b := range f.Dashboards {
		if err := s.PutDashboard(ctx, b); err != nil {
			t.Fatalf("failed to populate Dashboards")
		}
	}
	for _, b := range f.Views {
		if err := s.PutView(ctx, b); err != nil {
			t.Fatalf("failed to populate views")
		}
	}
	return s, func() {}
}

func TestDashboardService_CreateDashboard(t *testing.T) {
	platformtesting.CreateDashboard(initDashboardService, t)
}

func TestDashboardService_FindDashboardByID(t *testing.T) {
	platformtesting.FindDashboardByID(initDashboardService, t)
}
func TestDashboardService_FindDashboards(t *testing.T) {
	platformtesting.FindDashboards(initDashboardService, t)
}

func TestDashboardService_DeleteDashboard(t *testing.T) {
	platformtesting.DeleteDashboard(initDashboardService, t)
}

func TestDashboardService_UpdateDashboard(t *testing.T) {
	platformtesting.UpdateDashboard(initDashboardService, t)
}

func TestDashboardService_AddDashboardCell(t *testing.T) {
	platformtesting.AddDashboardCell(initDashboardService, t)
}

func TestDashboardService_RemoveDashboardCell(t *testing.T) {
	platformtesting.RemoveDashboardCell(initDashboardService, t)
}

func TestDashboardService_UpdateDashboardCell(t *testing.T) {
	platformtesting.UpdateDashboardCell(initDashboardService, t)
}

func TestDashboardService_ReplaceDashboardCells(t *testing.T) {
	platformtesting.ReplaceDashboardCells(initDashboardService, t)
}
