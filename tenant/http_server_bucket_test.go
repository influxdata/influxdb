package tenant_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	ihttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func initBucketHttpService(f itesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
	t.Helper()

	s, stCloser, err := NewTestInmemStore(t)
	if err != nil {
		t.Fatal(err)
	}

	store := tenant.NewStore(s)
	svc := tenant.NewService(store)

	ctx := context.Background()
	for _, o := range f.Organizations {
		// use storage create org in order to avoid creating system buckets
		if err := s.Update(ctx, func(tx kv.Tx) error {
			return store.CreateOrg(tx.Context(), tx, o)
		}); err != nil {
			t.Fatalf("failed to populate organizations: %s", err)
		}
	}
	for _, b := range f.Buckets {
		if err := svc.CreateBucket(ctx, b); err != nil {
			t.Fatalf("failed to populate buckets")
		}
	}

	handler := tenant.NewHTTPBucketHandler(zaptest.NewLogger(t), svc, nil, nil, nil)
	r := chi.NewRouter()
	r.Mount(handler.Prefix(), handler)
	server := httptest.NewServer(r)
	httpClient, err := ihttp.NewHTTPClient(server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}

	client := tenant.BucketClientService{
		Client: httpClient,
	}

	return &client, "http_tenant", func() {
		server.Close()
		stCloser()
	}
}

func TestBucketService(t *testing.T) {
	itesting.BucketService(initBucketHttpService, t, itesting.WithoutHooks(), itesting.WithHTTPValidation())
}
