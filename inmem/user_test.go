package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initUserService(f platformtesting.UserFields, t *testing.T) (platform.UserService, func()) {
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

func TestUserService_CreateUser(t *testing.T) {
	t.Parallel()
	platformtesting.CreateUser(initUserService, t)
}

func TestUserService_FindUserByID(t *testing.T) {
	t.Parallel()
	platformtesting.FindUserByID(initUserService, t)
}

func TestUserService_FindUsers(t *testing.T) {
	t.Parallel()
	platformtesting.FindUsers(initUserService, t)
}

func TestUserService_DeleteUser(t *testing.T) {
	t.Parallel()
	platformtesting.DeleteUser(initUserService, t)
}

func TestUserService_FindUser(t *testing.T) {
	t.Parallel()
	platformtesting.FindUser(initUserService, t)
}

func TestUserService_UpdateUser(t *testing.T) {
	t.Parallel()
	platformtesting.UpdateUser(initUserService, t)
}
