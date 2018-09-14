package http

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/inmem"
	platformtesting "github.com/influxdata/platform/testing"
)

func initOrganizationService(f platformtesting.OrganizationFields, t *testing.T) (platform.OrganizationService, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator

	ctx := context.TODO()
	for _, o := range f.Organizations {
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}

	handler := NewOrgHandler()
	handler.OrganizationService = svc
	handler.BucketService = svc
	server := httptest.NewServer(handler)
	client := OrganizationService{
		Addr: server.URL,
	}
	done := func() {
		server.Close()
	}

	return &client, done
}

func TestOrganizationService_CreateOrganization(t *testing.T) {
	platformtesting.CreateOrganization(initOrganizationService, t)
}

func TestOrganizationService_FindOrganizationByID(t *testing.T) {
	platformtesting.FindOrganizationByID(initOrganizationService, t)
}

func TestOrganizationService_FindOrganizations(t *testing.T) {
	platformtesting.FindOrganizations(initOrganizationService, t)
}

func TestOrganizationService_DeleteOrganization(t *testing.T) {
	platformtesting.DeleteOrganization(initOrganizationService, t)
}

func TestOrganizationService_FindOrganization(t *testing.T) {
	platformtesting.FindOrganization(initOrganizationService, t)
}

func TestOrganizationService_UpdateOrganization(t *testing.T) {
	platformtesting.UpdateOrganization(initOrganizationService, t)
}
