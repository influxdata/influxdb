package http

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/inmem"
	"github.com/influxdata/platform/mock"
	platformtesting "github.com/influxdata/platform/testing"
)

func initOrganizationService(f platformtesting.OrganizationFields, t *testing.T) (platform.OrganizationService, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	for _, o := range f.Organizations {
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}

	mappingService := mock.NewUserResourceMappingService()
	handler := NewOrgHandler(mappingService)
	handler.OrganizationService = svc
	handler.BucketService = svc
	server := httptest.NewServer(handler)
	client := OrganizationService{
		Addr: server.URL,
	}
	done := server.Close

	return &client, done
}
func TestOrganizationService(t *testing.T) {
	t.Parallel()
	platformtesting.OrganizationService(initOrganizationService, t)
}
