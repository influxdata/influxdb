package dbrp_test

import (
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"go.uber.org/zap/zaptest"
)

func setup(t *testing.T) (*dbrp.Client, func()) {
	t.Helper()
	dbrpSvc := &mock.DBRPMappingService{
		CreateFn: func(ctx context.Context, dbrp *influxdb.DBRPMapping) error {
			dbrp.ID = 1
			return nil
		},
		FindByIDFn: func(ctx context.Context, orgID, id platform.ID) (*influxdb.DBRPMapping, error) {
			return &influxdb.DBRPMapping{
				ID:              id,
				Database:        "db",
				RetentionPolicy: "rp",
				Default:         false,
				OrganizationID:  id,
				BucketID:        1,
			}, nil
		},
		FindManyFn: func(ctx context.Context, dbrp influxdb.DBRPMappingFilter, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMapping, int, error) {
			return []*influxdb.DBRPMapping{}, 0, nil
		},
	}
	orgSvc := &mock.OrganizationService{
		FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
			return &influxdb.Organization{
				ID:   *filter.ID,
				Name: "org",
			}, nil
		},
	}
	server := httptest.NewServer(dbrp.NewHTTPHandler(zaptest.NewLogger(t), dbrpSvc, orgSvc))
	client, err := httpc.New(httpc.WithAddr(server.URL), httpc.WithStatusFn(http.CheckError))
	if err != nil {
		t.Fatal(err)
	}
	dbrpClient := dbrp.NewClient(client)
	dbrpClient.Prefix = ""
	return dbrpClient, func() {
		server.Close()
	}
}

func TestClient(t *testing.T) {
	t.Run("can create", func(t *testing.T) {
		client, shutdown := setup(t)
		defer shutdown()

		if err := client.Create(context.Background(), &influxdb.DBRPMapping{
			Database:        "db",
			RetentionPolicy: "rp",
			Default:         false,
			OrganizationID:  1,
			BucketID:        1,
		}); err != nil {
			t.Error(err)
		}
	})

	t.Run("can read", func(t *testing.T) {
		client, shutdown := setup(t)
		defer shutdown()

		if _, err := client.FindByID(context.Background(), 1, 1); err != nil {
			t.Error(err)
		}
		oid := platform.ID(1)
		if _, _, err := client.FindMany(context.Background(), influxdb.DBRPMappingFilter{OrgID: &oid}); err != nil {
			t.Error(err)
		}
	})

	t.Run("can update", func(t *testing.T) {
		client, shutdown := setup(t)
		defer shutdown()

		if err := client.Update(context.Background(), &influxdb.DBRPMapping{
			ID:              1,
			Database:        "db",
			RetentionPolicy: "rp",
			Default:         false,
			OrganizationID:  1,
			BucketID:        1,
		}); err != nil {
			t.Error(err)
		}
	})

	t.Run("can delete", func(t *testing.T) {
		client, shutdown := setup(t)
		defer shutdown()

		if err := client.Delete(context.Background(), 1, 1); err != nil {
			t.Error(err)
		}
	})
}
