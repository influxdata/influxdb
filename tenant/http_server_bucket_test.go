package tenant_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb"
	ihttp "github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/tenant"
	itesting "github.com/influxdata/influxdb/testing"
	"go.uber.org/zap/zaptest"
)

func initBucketHttpService(f itesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
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
	for _, o := range f.Organizations {
		if err := svc.CreateOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}
	for _, b := range f.Buckets {
		if err := svc.CreateBucket(ctx, b); err != nil {
			t.Fatalf("failed to populate buckets")
		}
	}

	handler := tenant.NewHTTPBucketHandler(zaptest.NewLogger(t), svc, nil, nil)
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
	itesting.BucketService(initBucketHttpService, t, itesting.WithoutHooks())
}
