package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initBasicAuthService(f platformtesting.UserFields, t *testing.T) (platform.BasicAuthService, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, u := range f.Users {
		if err := s.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}
	return s, func() {}
}

func TestBasicAuth(t *testing.T) {
	t.Parallel()
	platformtesting.BasicAuth(initBasicAuthService, t)
}

func TestBasicAuth_CompareAndSet(t *testing.T) {
	t.Parallel()
	platformtesting.CompareAndSetPassword(initBasicAuthService, t)
}
