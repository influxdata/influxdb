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
	for _, m := range f.UserResourceMappings {
		if err := c.CreateUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate user resource mapping")
		}
	}
	for _, o := range f.Organizations {
		if err := c.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate orgs: %v", err)
		}
	}
	return c, bolt.OpPrefix, func() {
		defer closeFn()
		for _, target := range f.Targets {
			if err := c.RemoveTarget(ctx, target.ID); err != nil {
				t.Logf("failed to remove targets: %v", err)
			}
		}
		for _, o := range f.Organizations {
			if err := c.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove org: %v", err)
			}
		}
	}
}

func TestScraperTargetStoreService(t *testing.T) {
	platformtesting.ScraperService(initScraperTargetStoreService, t)
}
