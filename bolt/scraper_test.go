package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initScraperTargetStoreService(f platformtesting.TargetFields, t *testing.T) (platform.ScraperTargetStoreService, string, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, target := range f.Targets {
		if err := c.PutTarget(ctx, target); err != nil {
			t.Fatalf("failed to populate targets: %v", err)
		}
	}
	return c, bolt.OpPrefix, func() {
		defer closeFn()
		for _, target := range f.Targets {
			if err := c.RemoveTarget(ctx, target.ID); err != nil {
				t.Logf("failed to remove targets: %v", err)
			}
		}
	}
}

func TestScraperTargetStoreService_AddTarget(t *testing.T) {
	platformtesting.AddTarget(initScraperTargetStoreService, t)
}

func TestScraperTargetStoreService_ListTargets(t *testing.T) {
	platformtesting.ListTargets(initScraperTargetStoreService, t)
}

func TestScraperTargetStoreService_RemoveTarget(t *testing.T) {
	platformtesting.RemoveTarget(initScraperTargetStoreService, t)
}

func TestScraperTargetStoreService_UpdateTarget(t *testing.T) {
	platformtesting.UpdateTarget(initScraperTargetStoreService, t)
}

func TestScraperTargetStoreService_GetTargetByID(t *testing.T) {
	platformtesting.GetTargetByID(initScraperTargetStoreService, t)
}
