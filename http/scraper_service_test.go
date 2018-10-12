package http

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/inmem"
	platformtesting "github.com/influxdata/platform/testing"
)

func initScraperService(f platformtesting.TargetFields, t *testing.T) (platform.ScraperTargetStoreService, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	for _, target := range f.Targets {
		if err := svc.PutTarget(ctx, target); err != nil {
			t.Fatalf("failed to populate scraper targets")
		}
	}

	handler := NewScraperHandler()
	handler.ScraperStorageService = svc
	server := httptest.NewServer(handler)
	client := ScraperService{
		Addr: server.URL,
	}
	done := server.Close

	return &client, done
}

func TestScraperService(t *testing.T) {
	platformtesting.ScraperService(initScraperService, t)
}
