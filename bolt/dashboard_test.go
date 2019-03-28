package bolt_test

import (
	"context"
	"testing"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initDashboardService(f platformtesting.DashboardFields, t *testing.T) (platform.DashboardService, string, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}

	if f.NowFn == nil {
		f.NowFn = time.Now
	}

	c.IDGenerator = f.IDGenerator
	c.WithTime(f.NowFn)
	ctx := context.TODO()
	for _, b := range f.Dashboards {
		if err := c.PutDashboard(ctx, b); err != nil {
			t.Fatalf("failed to populate dashboards")
		}
	}
	return c, bolt.OpPrefix, func() {
		defer closeFn()
		for _, b := range f.Dashboards {
			if err := c.DeleteDashboard(ctx, b.ID); err != nil {
				t.Logf("failed to remove dashboard: %v", err)
			}
		}
	}
}

func TestDashboardService(t *testing.T) {
	platformtesting.DashboardService(initDashboardService, t)
}
