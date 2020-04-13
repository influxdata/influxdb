package authorization_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	ihttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func initAuthorizationService(f itesting.AuthorizationFields, t *testing.T) (influxdb.AuthorizationService, string, func()) {
	t.Helper()

	s, stCloser, err := NewTestInmemStore(t)
	if err != nil {
		t.Fatal(err)
	}

	storage, err := authorization.NewStore(s)
	if err != nil {
		t.Fatal(err)
	}

	// set up tenant service
	store, err := tenant.NewStore(s)
	if err != nil {
		t.Fatal(err)
	}
	ts := tenant.NewService(store)

	ctx := context.Background()
	svc := authorization.NewService(storage, ts)

	for _, u := range f.Users {
		if err := ts.CreateUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	for _, o := range f.Orgs {
		if err := ts.CreateOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate orgs")
		}
	}

	for _, a := range f.Authorizations {
		if err := svc.CreateAuthorization(ctx, a); err != nil {
			t.Fatalf("failed to populate authorizations: %v", err)
		}
	}

	handler := authorization.NewHTTPAuthHandler(zaptest.NewLogger(t), svc, ts, mock.NewLookupService())
	r := chi.NewRouter()
	r.Mount(handler.Prefix(), handler)
	server := httptest.NewServer(r)

	httpClient, err := ihttp.NewHTTPClient(server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}

	client := authorization.AuthorizationClientService{
		Client: httpClient,
	}

	return &client, "http_authorization", func() {
		server.Close()
		stCloser()
	}
}

func NewTestInmemStore(t *testing.T) (kv.Store, func(), error) {
	return inmem.NewKVStore(), func() {}, nil
}

func TestAuthorizationService(t *testing.T) {
	t.Parallel()
	// skip FindByToken test here because this function is not supported by the API
	itesting.AuthorizationService(initAuthorizationService, t, itesting.WithoutFindByToken())
}
