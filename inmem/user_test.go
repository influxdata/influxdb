package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initUserService(f platformtesting.UserFields, t *testing.T) (platform.UserService, string, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, u := range f.Users {
		if err := s.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}
	return s, OpPrefix, func() {}
}

func TestUserService(t *testing.T) {
	t.Parallel()
	platformtesting.UserService(initUserService, t)
}
