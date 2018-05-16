package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initAuthorizationService(f platformtesting.AuthorizationFields, t *testing.T) (platform.AuthorizationService, func()) {
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
			t.Fatalf("failed to populate authorizations")
		}
	}
	return c, func() {
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

func TestAuthorizationService_CreateAuthorization(t *testing.T) {
	platformtesting.CreateAuthorization(initAuthorizationService, t)
}

func TestAuthorizationService_FindAuthorizationByID(t *testing.T) {
	platformtesting.FindAuthorizationByID(initAuthorizationService, t)
}

func TestAuthorizationService_FindAuthorizationByToken(t *testing.T) {
	platformtesting.FindAuthorizationByToken(initAuthorizationService, t)
}

func TestAuthorizationService_FindAuthorizations(t *testing.T) {
	platformtesting.FindAuthorizations(initAuthorizationService, t)
}

func TestAuthorizationService_DeleteAuthorization(t *testing.T) {
	platformtesting.DeleteAuthorization(initAuthorizationService, t)
}
