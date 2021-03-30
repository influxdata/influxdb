package authorizer_test

import (
	"bytes"
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

var bucketCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Bucket) []*influxdb.Bucket {
		out := append([]*influxdb.Bucket(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

func TestBucketService_FindBucketByID(t *testing.T) {
	type fields struct {
		BucketService influxdb.BucketService
	}
	type args struct {
		permission influxdb.Permission
		id         platform.ID
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to access id",
			fields: fields{
				BucketService: &mock.BucketService{
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:    id,
							OrgID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.BucketsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
				id: 1,
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to access id",
			fields: fields{
				BucketService: &mock.BucketService{
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:    id,
							OrgID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.BucketsResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/000000000000000a/buckets/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewBucketService(tt.fields.BucketService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindBucketByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestBucketService_FindBucket(t *testing.T) {
	type fields struct {
		BucketService influxdb.BucketService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to access bucket",
			fields: fields{
				BucketService: &mock.BucketService{
					FindBucketFn: func(ctx context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:    1,
							OrgID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.BucketsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to access bucket",
			fields: fields{
				BucketService: &mock.BucketService{
					FindBucketFn: func(ctx context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:    1,
							OrgID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.BucketsResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/000000000000000a/buckets/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewBucketService(tt.fields.BucketService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindBucket(ctx, influxdb.BucketFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestBucketService_FindBuckets(t *testing.T) {
	type fields struct {
		BucketService influxdb.BucketService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err     error
		buckets []*influxdb.Bucket
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all buckets",
			fields: fields{
				BucketService: &mock.BucketService{
					FindBucketsFn: func(ctx context.Context, filter influxdb.BucketFilter, opt ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
						return []*influxdb.Bucket{
							{
								ID:    1,
								OrgID: 10,
							},
							{
								ID:    2,
								OrgID: 10,
							},
							{
								ID:    3,
								OrgID: 11,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.BucketsResourceType,
					},
				},
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    1,
						OrgID: 10,
					},
					{
						ID:    2,
						OrgID: 10,
					},
					{
						ID:    3,
						OrgID: 11,
					},
				},
			},
		},
		{
			name: "authorized to access a single orgs buckets",
			fields: fields{
				BucketService: &mock.BucketService{

					FindBucketsFn: func(ctx context.Context, filter influxdb.BucketFilter, opt ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
						return []*influxdb.Bucket{
							{
								ID:    1,
								OrgID: 10,
							},
							{
								ID:    2,
								OrgID: 10,
							},
							{
								ID:    3,
								OrgID: 11,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.BucketsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    1,
						OrgID: 10,
					},
					{
						ID:    2,
						OrgID: 10,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewBucketService(tt.fields.BucketService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			buckets, _, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(buckets, tt.wants.buckets, bucketCmpOptions...); diff != "" {
				t.Errorf("buckets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestBucketService_UpdateBucket(t *testing.T) {
	type fields struct {
		BucketService influxdb.BucketService
	}
	type args struct {
		id          platform.ID
		permissions []influxdb.Permission
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to update bucket",
			fields: fields{
				BucketService: &mock.BucketService{
					FindBucketByIDFn: func(ctc context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:    1,
							OrgID: 10,
						}, nil
					},
					UpdateBucketFn: func(ctx context.Context, id platform.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:    1,
							OrgID: 10,
						}, nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: "write",
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to update bucket",
			fields: fields{
				BucketService: &mock.BucketService{
					FindBucketByIDFn: func(ctc context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:    1,
							OrgID: 10,
						}, nil
					},
					UpdateBucketFn: func(ctx context.Context, id platform.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:    1,
							OrgID: 10,
						}, nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/buckets/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewBucketService(tt.fields.BucketService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			_, err := s.UpdateBucket(ctx, tt.args.id, influxdb.BucketUpdate{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestBucketService_DeleteBucket(t *testing.T) {
	type fields struct {
		BucketService influxdb.BucketService
	}
	type args struct {
		id          platform.ID
		permissions []influxdb.Permission
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to delete bucket",
			fields: fields{
				BucketService: &mock.BucketService{
					FindBucketByIDFn: func(ctc context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:    1,
							OrgID: 10,
						}, nil
					},
					DeleteBucketFn: func(ctx context.Context, id platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: "write",
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to delete bucket",
			fields: fields{
				BucketService: &mock.BucketService{
					FindBucketByIDFn: func(ctc context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:    1,
							OrgID: 10,
						}, nil
					},
					DeleteBucketFn: func(ctx context.Context, id platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/buckets/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewBucketService(tt.fields.BucketService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.DeleteBucket(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestBucketService_CreateBucket(t *testing.T) {
	type fields struct {
		BucketService influxdb.BucketService
	}
	type args struct {
		permission influxdb.Permission
		orgID      platform.ID
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to create bucket",
			fields: fields{
				BucketService: &mock.BucketService{
					CreateBucketFn: func(ctx context.Context, b *influxdb.Bucket) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.BucketsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create bucket",
			fields: fields{
				BucketService: &mock.BucketService{
					CreateBucketFn: func(ctx context.Context, b *influxdb.Bucket) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.BucketsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/buckets is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewBucketService(tt.fields.BucketService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.CreateBucket(ctx, &influxdb.Bucket{OrgID: tt.args.orgID})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
