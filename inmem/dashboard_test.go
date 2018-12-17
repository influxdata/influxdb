package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initDashboardService(f platformtesting.DashboardFields, t *testing.T) (platform.DashboardService, string, func()) {
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
	return s, OpPrefix, func() {}
}

func TestDashboardService(t *testing.T) {
	platformtesting.DashboardService(initDashboardService, t)
}
