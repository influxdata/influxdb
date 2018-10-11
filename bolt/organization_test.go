package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initOrganizationService(f platformtesting.OrganizationFields, t *testing.T) (platform.OrganizationService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	ctx := context.TODO()
	for _, u := range f.Organizations {
		if err := c.PutOrganization(ctx, u); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}
	return c, func() {
		defer closeFn()
		for _, o := range f.Organizations {
			if err := c.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove organizations: %v", err)
			}
		}
	}
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
