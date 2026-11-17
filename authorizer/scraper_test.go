package authorizer_test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

var scraperCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []influxdb.ScraperTarget) []influxdb.ScraperTarget {
		out := append([]influxdb.ScraperTarget(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// scraperBuckets returns a bucket service in which bucket 100 is in org 10, the
// bucket and org used by the scraper target fixtures below. Bucket 300 is a
// second bucket in org 10, for moving a target to another bucket.
func scraperBuckets() *mock.BucketService {
	bs := mock.NewBucketService()
	bs.FindBucketByIDFn = func(_ context.Context, id platform.ID) (*influxdb.Bucket, error) {
		if id == 100 || id == 300 {
			return &influxdb.Bucket{ID: id, OrgID: 10}, nil
		}
		return nil, influxdb.ErrBucketNotFound
	}
	return bs
}

func TestScraperTargetStoreService_GetTargetByID(t *testing.T) {
	type fields struct {
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
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
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctx context.Context, id platform.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:    id,
							OrgID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.ScraperResourceType,
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
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctx context.Context, id platform.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:    id,
							OrgID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.ScraperResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/000000000000000a/scrapers/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewScraperTargetStoreService(tt.fields.ScraperTargetStoreService, mock.NewUserResourceMappingService(), mock.NewOrganizationService(), scraperBuckets())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.GetTargetByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestScraperTargetStoreService_ListTargets(t *testing.T) {
	type fields struct {
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err      error
		scrapers []influxdb.ScraperTarget
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all scrapers",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					ListTargetsF: func(ctx context.Context, filter influxdb.ScraperTargetFilter) ([]influxdb.ScraperTarget, error) {
						return []influxdb.ScraperTarget{
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
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.ScraperResourceType,
					},
				},
			},
			wants: wants{
				scrapers: []influxdb.ScraperTarget{
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
			name: "authorized to access a single orgs scrapers",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					ListTargetsF: func(ctx context.Context, filter influxdb.ScraperTargetFilter) ([]influxdb.ScraperTarget, error) {
						return []influxdb.ScraperTarget{
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
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type:  influxdb.ScraperResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				scrapers: []influxdb.ScraperTarget{
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
			s := authorizer.NewScraperTargetStoreService(tt.fields.ScraperTargetStoreService, mock.NewUserResourceMappingService(),
				mock.NewOrganizationService(), scraperBuckets())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			ts, err := s.ListTargets(ctx, influxdb.ScraperTargetFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(ts, tt.wants.scrapers, scraperCmpOptions...); diff != "" {
				t.Errorf("scrapers are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestScraperTargetStoreService_UpdateTarget(t *testing.T) {
	type fields struct {
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
	}
	type args struct {
		id          platform.ID
		bucketID    platform.ID
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
			name: "authorized to update scraper",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctc context.Context, id platform.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:       1,
							OrgID:    10,
							BucketID: 100,
						}, nil
					},
					UpdateTargetF: func(ctx context.Context, upd *influxdb.ScraperTarget, userID platform.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:       1,
							OrgID:    10,
							BucketID: 100,
						}, nil
					},
				},
			},
			args: args{
				id:       1,
				bucketID: 100,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.ScraperResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.ScraperResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(100),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to update scraper",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctc context.Context, id platform.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:       1,
							OrgID:    10,
							BucketID: 100,
						}, nil
					},
					UpdateTargetF: func(ctx context.Context, upd *influxdb.ScraperTarget, userID platform.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:       1,
							OrgID:    10,
							BucketID: 100,
						}, nil
					},
				},
			},
			args: args{
				id:       1,
				bucketID: 100,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.ScraperResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/scrapers/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
		{
			name: "unauthorized to write to bucket",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctc context.Context, id platform.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:       1,
							OrgID:    10,
							BucketID: 100,
						}, nil
					},
					UpdateTargetF: func(ctx context.Context, upd *influxdb.ScraperTarget, userID platform.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:       1,
							OrgID:    10,
							BucketID: 100,
						}, nil
					},
				},
			},
			args: args{
				id:       1,
				bucketID: 100,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.ScraperResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/buckets/0000000000000064 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
		{
			name: "bucket does not exist",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctc context.Context, id platform.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:       1,
							OrgID:    10,
							BucketID: 100,
						}, nil
					},
					UpdateTargetF: func(ctx context.Context, upd *influxdb.ScraperTarget, userID platform.ID) (*influxdb.ScraperTarget, error) {
						return upd, nil
					},
				},
			},
			args: args{
				id:       1,
				bucketID: 200,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.ScraperResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.BucketsResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: influxdb.ErrBucketNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewScraperTargetStoreService(tt.fields.ScraperTargetStoreService, mock.NewUserResourceMappingService(),
				mock.NewOrganizationService(), scraperBuckets())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			_, err := s.UpdateTarget(ctx, &influxdb.ScraperTarget{ID: tt.args.id, BucketID: tt.args.bucketID}, platform.ID(1))
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

// TestScraperTargetStoreService_UpdateTarget_unsetIDs updates a target without
// setting its org or bucket. The update reaches the store with the target's
// current org and bucket, not with them left for the store to fill in.
func TestScraperTargetStoreService_UpdateTarget_unsetIDs(t *testing.T) {
	var got *influxdb.ScraperTarget
	store := &mock.ScraperTargetStoreService{
		GetTargetByIDF: func(context.Context, platform.ID) (*influxdb.ScraperTarget, error) {
			return &influxdb.ScraperTarget{ID: 1, OrgID: 10, BucketID: 100}, nil
		},
		UpdateTargetF: func(_ context.Context, upd *influxdb.ScraperTarget, _ platform.ID) (*influxdb.ScraperTarget, error) {
			got = upd
			return upd, nil
		},
	}
	s := authorizer.NewScraperTargetStoreService(store, mock.NewUserResourceMappingService(),
		mock.NewOrganizationService(), scraperBuckets())

	ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{
		{
			Action:   influxdb.WriteAction,
			Resource: influxdb.Resource{Type: influxdb.ScraperResourceType, ID: influxdbtesting.IDPtr(1)},
		},
		{
			Action:   influxdb.WriteAction,
			Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, ID: influxdbtesting.IDPtr(100)},
		},
	}))

	_, err := s.UpdateTarget(ctx, &influxdb.ScraperTarget{ID: 1, Name: "renamed"}, platform.ID(1))
	require.NoError(t, err)
	require.Equal(t, &influxdb.ScraperTarget{ID: 1, Name: "renamed", OrgID: 10, BucketID: 100}, got)
}

// TestScraperTargetStoreService_UpdateTarget_newBucket moves a target from
// bucket 100 to bucket 300, both in org 10. The caller must be able to write to
// the new bucket as well as the current one.
func TestScraperTargetStoreService_UpdateTarget_newBucket(t *testing.T) {
	write := func(rt influxdb.ResourceType, id platform.ID) influxdb.Permission {
		return influxdb.Permission{
			Action:   influxdb.WriteAction,
			Resource: influxdb.Resource{Type: rt, ID: influxdbtesting.IDPtr(id)},
		}
	}

	tests := []struct {
		name        string
		permissions []influxdb.Permission
		wantErr     error
	}{
		{
			name: "unauthorized to write to the new bucket",
			permissions: []influxdb.Permission{
				write(influxdb.ScraperResourceType, 1),
				write(influxdb.BucketsResourceType, 100),
			},
			wantErr: &errors.Error{
				Msg:  "write:orgs/000000000000000a/buckets/000000000000012c is unauthorized",
				Code: errors.EUnauthorized,
			},
		},
		{
			name: "authorized to write to the new bucket",
			permissions: []influxdb.Permission{
				write(influxdb.ScraperResourceType, 1),
				write(influxdb.BucketsResourceType, 100),
				write(influxdb.BucketsResourceType, 300),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got *influxdb.ScraperTarget
			store := &mock.ScraperTargetStoreService{
				GetTargetByIDF: func(context.Context, platform.ID) (*influxdb.ScraperTarget, error) {
					return &influxdb.ScraperTarget{ID: 1, OrgID: 10, BucketID: 100}, nil
				},
				UpdateTargetF: func(_ context.Context, upd *influxdb.ScraperTarget, _ platform.ID) (*influxdb.ScraperTarget, error) {
					got = upd
					return upd, nil
				},
			}
			s := authorizer.NewScraperTargetStoreService(store, mock.NewUserResourceMappingService(),
				mock.NewOrganizationService(), scraperBuckets())
			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, tt.permissions))

			_, err := s.UpdateTarget(ctx, &influxdb.ScraperTarget{ID: 1, BucketID: 300}, platform.ID(1))

			if tt.wantErr != nil {
				require.EqualError(t, err, tt.wantErr.Error())
				require.Equal(t, errors.ErrorCode(tt.wantErr), errors.ErrorCode(err))
				require.Nil(t, got, "the update must not reach the store")
				return
			}
			require.NoError(t, err)
			require.Equal(t, &influxdb.ScraperTarget{ID: 1, OrgID: 10, BucketID: 300}, got)
		})
	}
}

func TestScraperTargetStoreService_RemoveTarget(t *testing.T) {
	type fields struct {
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
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
			name: "authorized to delete scraper",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctc context.Context, id platform.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:    1,
							OrgID: 10,
						}, nil
					},
					RemoveTargetF: func(ctx context.Context, id platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.ScraperResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.ScraperResourceType,
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
			name: "unauthorized to delete scraper",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctc context.Context, id platform.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:    1,
							OrgID: 10,
						}, nil
					},
					RemoveTargetF: func(ctx context.Context, id platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.ScraperResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/scrapers/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewScraperTargetStoreService(tt.fields.ScraperTargetStoreService, mock.NewUserResourceMappingService(),
				mock.NewOrganizationService(), scraperBuckets())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.RemoveTarget(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestScraperTargetStoreService_AddTarget(t *testing.T) {
	type fields struct {
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
	}
	type args struct {
		permissions []influxdb.Permission
		orgID       platform.ID
		bucketID    platform.ID
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
			name: "authorized to create scraper",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					AddTargetF: func(ctx context.Context, st *influxdb.ScraperTarget, userID platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				orgID:    10,
				bucketID: 100,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.ScraperResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(100),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create scraper",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					AddTargetF: func(ctx context.Context, st *influxdb.ScraperTarget, userID platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				orgID:    10,
				bucketID: 100,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.ScraperResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(100),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/scrapers is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
		{
			name: "unauthorized to write to bucket",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					AddTargetF: func(ctx context.Context, st *influxdb.ScraperTarget, userID platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				orgID:    10,
				bucketID: 100,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.ScraperResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/buckets/0000000000000064 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
		{
			name: "bucket does not exist",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					AddTargetF: func(ctx context.Context, st *influxdb.ScraperTarget, userID platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				orgID:    10,
				bucketID: 200,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.ScraperResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.BucketsResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: influxdb.ErrBucketNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewScraperTargetStoreService(tt.fields.ScraperTargetStoreService, mock.NewUserResourceMappingService(),
				mock.NewOrganizationService(), scraperBuckets())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.AddTarget(ctx, &influxdb.ScraperTarget{OrgID: tt.args.orgID, BucketID: tt.args.bucketID}, platform.ID(1))
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
