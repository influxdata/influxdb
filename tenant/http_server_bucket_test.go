package tenant_test

import (
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-chi/chi"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	ihttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
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

const idOne = platform.ID(iota + 1)

func TestHTTPBucketService_InvalidRetention(t *testing.T) {
	type args struct {
		name          string
		id            platform.ID
		retention     int
		shardDuration int
		description   *string
	}
	type wants struct {
		err    error
		bucket *influxdb.Bucket
	}

	tests := []struct {
		name   string
		fields itesting.BucketFields
		args   args
		wants  wants
	}{
		{
			name: "update with negative retention",
			fields: itesting.BucketFields{
				OrgIDs:        mock.NewIncrementingIDGenerator(idOne),
				BucketIDs:     mock.NewIncrementingIDGenerator(idOne),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						// ID(1)
						Name: "theorg",
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						// ID(1)
						OrgID:              idOne,
						Name:               "bucket1",
						RetentionPeriod:    humanize.Day,
						ShardGroupDuration: time.Hour,
					},
					{
						// ID(2)
						OrgID: idOne,
						Name:  "bucket2",
					},
				},
			},
			args: args{
				id:        idOne,
				retention: -1,
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EUnprocessableEntity,
					Msg:  "expiration seconds cannot be negative",
				},
			},
		},
		{
			name: "update with negative shard-group duration",
			fields: itesting.BucketFields{
				OrgIDs:        mock.NewIncrementingIDGenerator(idOne),
				BucketIDs:     mock.NewIncrementingIDGenerator(idOne),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						// ID(1)
						Name: "theorg",
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						// ID(1)
						OrgID:              idOne,
						Name:               "bucket1",
						RetentionPeriod:    humanize.Day,
						ShardGroupDuration: time.Hour,
					},
					{
						// ID(2)
						OrgID: idOne,
						Name:  "bucket2",
					},
				},
			},
			args: args{
				id:            idOne,
				shardDuration: -1,
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EUnprocessableEntity,
					Msg:  "shard-group duration seconds cannot be negative",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := initBucketHttpService(tt.fields, t)
			defer done()
			ctx := context.Background()

			upd := influxdb.BucketUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}
			if tt.args.retention != 0 {
				d := time.Duration(tt.args.retention) * time.Minute
				upd.RetentionPeriod = &d
			}
			if tt.args.shardDuration != 0 {
				d := time.Duration(tt.args.shardDuration) * time.Minute
				upd.ShardGroupDuration = &d
			}

			upd.Description = tt.args.description

			bucket, err := s.UpdateBucket(ctx, tt.args.id, upd)
			itesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(bucket, tt.wants.bucket, bucketCmpOptions...); diff != "" {
				t.Errorf("bucket is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
