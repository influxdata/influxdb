package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initAuthorizationService(f platformtesting.AuthorizationFields, t *testing.T) (platform.AuthorizationService, string, func()) {
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
	return s, OpPrefix, func() {}
}

func TestAuthorizationService(t *testing.T) {
	platformtesting.AuthorizationService(initAuthorizationService, t)
}
