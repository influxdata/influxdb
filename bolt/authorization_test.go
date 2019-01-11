package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initAuthorizationService(f platformtesting.AuthorizationFields, t *testing.T) (platform.AuthorizationService, string, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	c.TokenGenerator = f.TokenGenerator
	ctx := context.Background()

	for _, u := range f.Users {
		if err := c.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	for _, o := range f.Orgs {
		if err := c.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate orgs")
		}
	}

	for _, a := range f.Authorizations {
		if err := c.PutAuthorization(ctx, a); err != nil {
			t.Fatalf("failed to populate authorizations %s", err)
		}
	}

	return c, bolt.OpPrefix, func() {
		defer closeFn()
		for _, u := range f.Users {
			if err := c.DeleteUser(ctx, u.ID); err != nil {
				t.Logf("failed to remove user: %v", err)
			}
		}

		for _, o := range f.Orgs {
			if err := c.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove org: %v", err)
			}
		}

		for _, a := range f.Authorizations {
			if err := c.DeleteAuthorization(ctx, a.ID); err != nil {
				t.Logf("failed to remove authorizations: %v", err)
			}
		}
	}
}

func TestAuthorizationService(t *testing.T) {
	platformtesting.AuthorizationService(initAuthorizationService, t)
}
