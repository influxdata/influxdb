package tenant_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func initHttpOrgService(f itesting.OrganizationFields, t *testing.T) (influxdb.OrganizationService, string, func()) {
	t.Helper()

	s, stCloser, err := itesting.NewTestInmemStore(t)
	if err != nil {
		t.Fatal(err)
	}

	storage := tenant.NewStore(s)

	if f.OrgBucketIDs != nil {
		storage.OrgIDGen = f.OrgBucketIDs
		storage.BucketIDGen = f.OrgBucketIDs
	}

	// go direct to storage for test data
	if err := s.Update(context.Background(), func(tx kv.Tx) error {
		for _, o := range f.Organizations {
			if err := storage.CreateOrg(tx.Context(), tx, o); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("failed to populate organizations: %s", err)
	}

	handler := tenant.NewHTTPOrgHandler(zaptest.NewLogger(t), tenant.NewService(storage), nil, nil)
	r := chi.NewRouter()
	r.Mount(handler.Prefix(), handler)
	server := httptest.NewServer(r)
	httpClient, err := http.NewHTTPClient(server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}

	orgClient := tenant.OrgClientService{
		Client: httpClient,
	}

	return &orgClient, "http_tenant", func() {
		server.Close()
		stCloser()
	}
}

func TestHTTPOrgService(t *testing.T) {
	itesting.OrganizationService(initHttpOrgService, t)
}
