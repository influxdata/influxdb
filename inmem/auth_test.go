package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initAuthorizationService(f platformtesting.AuthorizationFields, t *testing.T) (platform.AuthorizationService, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	s.TokenGenerator = f.TokenGenerator
	ctx := context.TODO()
	for _, u := range f.Users {
		if err := s.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}
	for _, u := range f.Authorizations {
		if err := s.PutAuthorization(ctx, u); err != nil {
			t.Fatalf("failed to populate authorizations")
		}
	}
	return s, func() {}
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
