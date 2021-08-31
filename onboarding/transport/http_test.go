package transport_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	ihttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/onboarding"
	"github.com/influxdata/influxdb/v2/onboarding/transport"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func initOnboardHttpService(f itesting.OnboardingFields, t *testing.T) (influxdb.OnboardingService, func()) {
	t.Helper()

	s := itesting.NewTestInmemStore(t)
	storage := tenant.NewStore(s)

	ten := tenant.NewService(storage)

	authStore, err := authorization.NewStore(s)
	require.NoError(t, err)
	authSvc := authorization.NewService(authStore, ten)

	svc := onboarding.NewService(ten, authSvc)

	ctx := context.Background()
	if !f.IsOnboarding {
		// create a dummy so so we can no longer onboard
		err := ten.CreateUser(ctx, &influxdb.User{Name: "dummy", Status: influxdb.Active})
		if err != nil {
			t.Fatal(err)
		}
	}

	handler := transport.NewOnboardingHandler(zaptest.NewLogger(t), svc)
	r := chi.NewRouter()
	r.Mount(handler.Prefix(), handler)
	server := httptest.NewServer(r)
	httpClient, err := ihttp.NewHTTPClient(server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}

	client := transport.OnboardingClient{Client: httpClient}
	return &client, server.Close
}

func TestOnboardService(t *testing.T) {
	itesting.OnboardInitialUser(initOnboardHttpService, t)
}
