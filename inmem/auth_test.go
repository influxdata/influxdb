package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initAuthorizationService(f platformtesting.AuthorizationFields, t *testing.T) (platform.AuthorizationService, string, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	s.TokenGenerator = f.TokenGenerator
	ctx := context.Background()

	for _, u := range f.Users {
		if err := s.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	for _, o := range f.Orgs {
		if err := s.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
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
