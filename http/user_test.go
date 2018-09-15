package http

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/inmem"
	platformtesting "github.com/influxdata/platform/testing"
)

func initUserService(f platformtesting.UserFields, t *testing.T) (platform.UserService, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	handler := NewUserHandler()
	handler.UserService = svc
	server := httptest.NewServer(handler)
	client := UserService{
		Addr: server.URL,
	}
	done := server.Close

	return &client, done
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
