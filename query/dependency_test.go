package query_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/stretchr/testify/require"
)

// bucketLookupByIDCases are shared by the LookupByID and LookupName tests.
var bucketLookupByIDCases = []struct {
	name     string
	bucket   *influxdb.Bucket
	err      error
	wantOK   bool
	wantName string
}{
	{
		name:     "found in the request org",
		bucket:   &influxdb.Bucket{ID: 1, OrgID: orgID, Name: "b1"},
		wantOK:   true,
		wantName: "b1",
	},
	{
		name:   "org does not match",
		bucket: &influxdb.Bucket{ID: 1, OrgID: orgID + 1, Name: "b1"},
	},
	{
		name: "read denied",
		err:  &errors.Error{Code: errors.EUnauthorized, Msg: "read denied"},
	},
	{
		name: "not found",
		err:  &errors.Error{Code: errors.ENotFound, Msg: "bucket not found"},
	},
	{
		name: "nil bucket without error",
	},
}

func newFindBucketService(t *testing.T, bucket *influxdb.Bucket, err error) *mock.BucketService {
	svc := mock.NewBucketService()
	svc.FindBucketFn = func(_ context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
		require.NotNil(t, filter.ID)
		require.Equal(t, platform.ID(1), *filter.ID)
		return bucket, err
	}
	return svc
}

func TestBucketLookup_LookupByID(t *testing.T) {
	for _, tc := range bucketLookupByIDCases {
		t.Run(tc.name, func(t *testing.T) {
			lookup := query.FromBucketService(newFindBucketService(t, tc.bucket, tc.err))
			id, ok := lookup.LookupByID(context.Background(), orgID, 1)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, platform.ID(1), id)
			} else {
				require.False(t, id.Valid())
			}
		})
	}
}

func TestBucketLookup_LookupName(t *testing.T) {
	for _, tc := range bucketLookupByIDCases {
		t.Run(tc.name, func(t *testing.T) {
			lookup := query.FromBucketService(newFindBucketService(t, tc.bucket, tc.err))
			require.Equal(t, tc.wantName, lookup.LookupName(context.Background(), orgID, 1))
		})
	}
}

func TestSecretLookup(t *testing.T) {
	req := &query.Request{OrganizationID: orgID}
	ctx := query.ContextWithRequest(context.Background(), req)
	svc := &mock.SecretService{
		LoadSecretFn: func(ctx context.Context, orgID platform.ID, k string) (string, error) {
			if want, got := req.OrganizationID, orgID; want != got {
				t.Errorf("unexpected organization id -want/+got:\n\t- %v\n\t+ %v", want, got)
			}
			if want, got := "mysecret", k; want != got {
				t.Errorf("unexpected secret key -want/+got:\n\t- %v\n\t+ %v", want, got)
			}
			return "mypassword", nil
		},
	}

	dep := query.FromSecretService(svc)
	if val, err := dep.LoadSecret(ctx, "mysecret"); err != nil {
		t.Errorf("unexpected error: %s", err)
	} else if want, got := "mypassword", val; want != got {
		t.Errorf("unexpected secret value -want/+got:\n\t- %v\n\t+ %v", want, got)
	}
}
