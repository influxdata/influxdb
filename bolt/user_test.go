package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initUserService(f platformtesting.UserFields, t *testing.T) (platform.UserService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, u := range f.Users {
		if err := c.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}
	return c, func() {
		defer closeFn()
		for _, u := range f.Users {
			if err := c.DeleteUser(ctx, u.ID); err != nil {
				t.Logf("failed to remove users: %v", err)
			}
		}
	}
}

func TestUserService_CreateUser(t *testing.T) {
	platformtesting.CreateUser(initUserService, t)
}

func TestUserService_FindUserByID(t *testing.T) {
	platformtesting.FindUserByID(initUserService, t)
}

func TestUserService_FindUsers(t *testing.T) {
	platformtesting.FindUsers(initUserService, t)
}

func TestUserService_DeleteUser(t *testing.T) {
	platformtesting.DeleteUser(initUserService, t)
}

func TestUserService_FindUser(t *testing.T) {
	platformtesting.FindUser(initUserService, t)
}

func TestUserService_UpdateUser(t *testing.T) {
	platformtesting.UpdateUser(initUserService, t)
}
