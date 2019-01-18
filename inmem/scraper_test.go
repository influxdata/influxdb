package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initScraperTargetStoreService(f platformtesting.TargetFields, t *testing.T) (platform.ScraperTargetStoreService, string, func()) {
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
	return s, OpPrefix, func() {}
}

func TestScraperTargetStoreService(t *testing.T) {
	platformtesting.ScraperService(initScraperTargetStoreService, t)
}
