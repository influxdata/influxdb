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
	for _, o := range f.Organizations {
		if err := c.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}
	for _, b := range f.Dashboards {
		if err := c.PutDashboard(ctx, b); err != nil {
			t.Fatalf("failed to populate dashboards")
		}
	}
	return c, func() {
		defer closeFn()
		for _, o := range f.Organizations {
			if err := c.DeleteOrganization(ctx, *o.ID); err != nil {
				t.Logf("failed to remove organization: %v", err)
			}
		}
		for _, b := range f.Dashboards {
			if err := c.DeleteDashboard(ctx, *b.ID); err != nil {
				t.Logf("failed to remove dashboard: %v", err)
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

func TestDashboardService_FindDashboardsByOrganizationID(t *testing.T) {
	platformtesting.FindDashboardsByOrganizationID(initDashboardService, t)
}

func TestDashboardService_FindDashboardsByOrganizationName(t *testing.T) {
	platformtesting.FindDashboardsByOrganizationName(initDashboardService, t)
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

func TestDashboardService_ReplaceDashboardCell(t *testing.T) {
	platformtesting.ReplaceDashboardCell(initDashboardService, t)
}

func TestDashboardService_RemoveDashboardCell(t *testing.T) {
	platformtesting.RemoveDashboardCell(initDashboardService, t)
}
