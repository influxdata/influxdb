package tenant_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	platform "github.com/influxdata/influxdb"
	ihttp "github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/tenant"
	platformtesting "github.com/influxdata/influxdb/testing"
	"go.uber.org/zap/zaptest"
)

func initHttpUserService(f platformtesting.UserFields, t *testing.T) (platform.UserService, string, func()) {
	t.Helper()

	s, stCloser, err := NewTestInmemStore(t)
	if err != nil {
		t.Fatal(err)
	}
	storage, err := tenant.NewStore(s)
	if err != nil {
		t.Fatal(err)
	}

	svc := tenant.NewService(storage)

	ctx := context.Background()
	for _, u := range f.Users {
		if err := svc.CreateUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	handler := tenant.NewHTTPUserHandler(zaptest.NewLogger(t), svc, svc)
	r := chi.NewRouter()
	r.Mount("/api/v2/users", handler)
	r.Mount("/api/v2/me", handler)
	server := httptest.NewServer(r)

	httpClient, err := ihttp.NewHTTPClient(server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}

	client := tenant.UserClientService{
		Client: httpClient,
	}

	return &client, "http_tenant", func() {
		server.Close()
		stCloser()
	}
}

func TestUserService(t *testing.T) {
	t.Parallel()
	platformtesting.UserService(initHttpUserService, t)
}
