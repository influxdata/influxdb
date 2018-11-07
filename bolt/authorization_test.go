package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/bolt"
	platformtesting "github.com/influxdata/platform/testing"
)

func initAuthorizationService(f platformtesting.AuthorizationFields, t *testing.T) (platform.AuthorizationService, string, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	c.TokenGenerator = f.TokenGenerator
	ctx := context.TODO()
	for _, u := range f.Users {
		if err := c.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
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
