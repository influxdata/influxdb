package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initOrganizationService(f platformtesting.OrganizationFields, t *testing.T) (platform.OrganizationService, string, func()) {
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
	return c, bolt.OpPrefix, func() {
		defer closeFn()
		for _, o := range f.Organizations {
			if err := c.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove organizations: %v", err)
			}
		}
	}
}

func TestOrganizationService(t *testing.T) {
	platformtesting.OrganizationService(initOrganizationService, t)
}
