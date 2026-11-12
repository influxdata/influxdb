package influxdb_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/stretchr/testify/require"
)

func TestFindBucketInOrg(t *testing.T) {
	const (
		orgID    = platform.ID(0x0a)
		bucketID = platform.ID(0xa0)
	)
	findErr := &errors.Error{Code: errors.EInternal, Msg: "find failed"}

	tests := []struct {
		name       string
		findBucket func(context.Context, platform.ID) (*influxdb.Bucket, error)
		want       *influxdb.Bucket
		wantErr    error
	}{
		{
			name: "bucket in org",
			findBucket: func(_ context.Context, id platform.ID) (*influxdb.Bucket, error) {
				return &influxdb.Bucket{ID: id, OrgID: orgID, Name: "b"}, nil
			},
			want: &influxdb.Bucket{ID: bucketID, OrgID: orgID, Name: "b"},
		},
		{
			name: "lookup error is returned",
			findBucket: func(context.Context, platform.ID) (*influxdb.Bucket, error) {
				return nil, findErr
			},
			wantErr: findErr,
		},
		{
			name: "nil bucket is not found",
			findBucket: func(context.Context, platform.ID) (*influxdb.Bucket, error) {
				return nil, nil
			},
			wantErr: influxdb.ErrBucketNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs := mock.NewBucketService()
			bs.FindBucketByIDFn = tt.findBucket

			got, err := influxdb.FindBucketInOrg(context.Background(), bs, orgID, bucketID)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestErrBucketNotFound(t *testing.T) {
	require.Same(t, influxdb.ErrBucketNotFound, tenant.ErrBucketNotFound)
}
