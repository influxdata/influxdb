package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initScraperTargetStoreService(f platformtesting.TargetFields, t *testing.T) (platform.ScraperTargetStoreService, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, target := range f.Targets {
		if err := s.PutTarget(ctx, target); err != nil {
			t.Fatalf("failed to populate scraper targets")
		}
	}
	return s, func() {}
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
