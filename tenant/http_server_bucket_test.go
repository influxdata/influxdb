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
	if f.IDGenerator != nil {
		store.IDGen = f.IDGenerator
	}

	if f.OrgIDs != nil {
		store.OrgIDGen = f.OrgIDs
	}

	if f.BucketIDs != nil {
		store.BucketIDGen = f.BucketIDs
	}

	ctx := context.Background()

	// go direct to storage for test data
	if err := s.Update(ctx, func(tx kv.Tx) error {
		for _, o := range f.Organizations {
			if err := store.CreateOrg(tx.Context(), tx, o); err != nil {
				return err
			}
		}

		for _, b := range f.Buckets {
			if err := store.CreateBucket(tx.Context(), tx, b); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatalf("failed to seed data: %s", err)
	}

	handler := tenant.NewHTTPBucketHandler(zaptest.NewLogger(t), tenant.NewService(store), nil, nil, nil)
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

func TestHTTPBucketService(t *testing.T) {
	itesting.BucketService(initBucketHttpService, t)
}
