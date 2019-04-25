package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initScraperTargetStoreService(f platformtesting.TargetFields, t *testing.T) (influxdb.ScraperTargetStoreService, string, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, target := range f.Targets {
		if err := s.PutTarget(ctx, target); err != nil {
			t.Fatalf("failed to populate scraper targets")
		}
	}
	for _, m := range f.UserResourceMappings {
		if err := s.PutUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate user resource mapping")
		}
	}
	for _, o := range f.Organizations {
		if err := s.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate orgs")
		}
	}
	return s, OpPrefix, func() {}
}

func TestScraperTargetStoreService(t *testing.T) {
	platformtesting.ScraperService(initScraperTargetStoreService, t)
}
