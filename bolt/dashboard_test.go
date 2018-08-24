package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initDashboardService(f platformtesting.DashboardFields, t *testing.T) (platform.DashboardService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	ctx := context.TODO()
	for _, b := range f.Dashboards {
		if err := c.PutDashboard(ctx, b); err != nil {
			t.Fatalf("failed to populate dashboards")
		}
	}
	for _, b := range f.Views {
		if err := c.PutView(ctx, b); err != nil {
			t.Fatalf("failed to populate views")
		}
	}
	return c, func() {
		defer closeFn()
		for _, b := range f.Dashboards {
			if err := c.DeleteDashboard(ctx, b.ID); err != nil {
				t.Logf("failed to remove dashboard: %v", err)
			}
		}
		for _, b := range f.Views {
			if err := c.DeleteView(ctx, b.ID); err != nil {
				t.Logf("failed to remove view: %v", err)
			}
		}
	}
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
