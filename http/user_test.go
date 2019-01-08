package http

import (
	"context"
	"net/http/httptest"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/mock"
	platformtesting "github.com/influxdata/influxdb/testing"
	"go.uber.org/zap"
)

// NewMockUserBackend returns a UserBackend with mock services.
func NewMockUserBackend() *UserBackend {
	return &UserBackend{
		Logger: zap.NewNop().With(zap.String("handler", "user")),

		UserService: mock.NewUserService(),

		UserOperationLogService: mock.NewUserOperationLogService(),
		BasicAuthService:        mock.NewBasicAuthService("", ""),
	}
}

func initUserService(f platformtesting.UserFields, t *testing.T) (platform.UserService, string, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	userBackend := NewMockUserBackend()
	userBackend.UserService = svc
	handler := NewUserHandler(userBackend)
	server := httptest.NewServer(handler)
	client := UserService{
		Addr:     server.URL,
		OpPrefix: inmem.OpPrefix,
	}

	done := server.Close

	return &client, inmem.OpPrefix, done
}

func TestUserService(t *testing.T) {
	t.Parallel()
	platformtesting.UserService(initUserService, t)
}
