package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
)

func initAuthorizationService(f platformtesting.AuthorizationFields, t *testing.T) (platform.AuthorizationService, string, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	s.TokenGenerator = f.TokenGenerator
	s.TimeGenerator = f.TimeGenerator
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
	t.Skip("This service is not used, we use the kv inmem implementation")
	platformtesting.AuthorizationService(initAuthorizationService, t)
}
