package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
)

func initOrganizationService(f platformtesting.OrganizationFields, t *testing.T) (platform.OrganizationService, string, func()) {
	c, closeFn, err := NewTestClient(t)
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	c.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		c.TimeGenerator = platform.RealTimeGenerator{}
	}
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
	t.Skip("organization service no longer used.  Remove all of this bolt stuff")
	platformtesting.OrganizationService(initOrganizationService, t)
}
