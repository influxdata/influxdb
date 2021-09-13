package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
)

func Test_queryOrganization(t *testing.T) {
	type args struct {
		ctx context.Context
		r   *http.Request
		svc platform.OrganizationService
	}
	tests := []struct {
		name    string
		args    args
		want    *platform.Organization
		wantErr bool
	}{
		{
			name: "org id finds organization",
			want: &platform.Organization{
				ID: platform2.ID(1),
			},
			args: args{
				ctx: context.Background(),
				r:   httptest.NewRequest(http.MethodPost, "/api/v2/query?orgID=0000000000000001", nil),
				svc: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						if *filter.ID == platform2.ID(1) {
							return &platform.Organization{
								ID: platform2.ID(1),
							}, nil
						}
						return nil, &errors.Error{
							Code: errors.EInvalid,
							Msg:  "unknown org name",
						}
					},
				},
			},
		},
		{
			name:    "bad id returns error",
			wantErr: true,
			args: args{
				ctx: context.Background(),
				r:   httptest.NewRequest(http.MethodPost, "/api/v2/query?orgID=howdy", nil),
			},
		},
		{
			name: "org name finds organization",
			want: &platform.Organization{
				ID:   platform2.ID(1),
				Name: "org1",
			},
			args: args{
				ctx: context.Background(),
				r:   httptest.NewRequest(http.MethodPost, "/api/v2/query?org=org1", nil),
				svc: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						if *filter.Name == "org1" {
							return &platform.Organization{
								ID:   platform2.ID(1),
								Name: "org1",
							}, nil
						}
						return nil, &errors.Error{
							Code: errors.EInvalid,
							Msg:  "unknown org name",
						}
					},
				},
			},
		},
		{
			name: "org id as org finds organization",
			want: &platform.Organization{
				ID: platform2.ID(1),
			},
			args: args{
				ctx: context.Background(),
				r:   httptest.NewRequest(http.MethodPost, "/api/v2/query?org=0000000000000001", nil),
				svc: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						if *filter.ID == platform2.ID(1) {
							return &platform.Organization{
								ID: platform2.ID(1),
							}, nil
						}
						return nil, &errors.Error{
							Code: errors.EInvalid,
							Msg:  "unknown org name",
						}
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := queryOrganization(tt.args.ctx, tt.args.r, tt.args.svc)
			if (err != nil) != tt.wantErr {
				t.Errorf("queryOrganization() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("queryOrganization() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_queryBucket(t *testing.T) {
	type args struct {
		ctx context.Context
		r   *http.Request
		svc platform.BucketService
	}
	tests := []struct {
		name    string
		args    args
		want    *platform.Bucket
		wantErr bool
	}{
		{
			name: "bucket id finds bucket",
			want: &platform.Bucket{
				ID: platform2.ID(1),
			},
			args: args{
				ctx: context.Background(),
				r:   httptest.NewRequest(http.MethodPost, "/api/v2/query?bucketID=0000000000000001", nil),
				svc: &mock.BucketService{
					FindBucketFn: func(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
						if *filter.ID == platform2.ID(1) {
							return &platform.Bucket{
								ID: platform2.ID(1),
							}, nil
						}
						return nil, &errors.Error{
							Code: errors.EInvalid,
							Msg:  "unknown org name",
						}
					},
				},
			},
		},
		{
			name:    "bad id returns error",
			wantErr: true,
			args: args{
				ctx: context.Background(),
				r:   httptest.NewRequest(http.MethodPost, "/api/v2/query?bucketID=howdy", nil),
			},
		},
		{
			name: "bucket name finds bucket",
			want: &platform.Bucket{
				ID:   platform2.ID(1),
				Name: "bucket1",
			},
			args: args{
				ctx: context.Background(),
				r:   httptest.NewRequest(http.MethodPost, "/api/v2/query?bucket=bucket1", nil),
				svc: &mock.BucketService{
					FindBucketFn: func(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
						if *filter.Name == "bucket1" {
							return &platform.Bucket{
								ID:   platform2.ID(1),
								Name: "bucket1",
							}, nil
						}
						return nil, &errors.Error{
							Code: errors.EInvalid,
							Msg:  "unknown org name",
						}
					},
				},
			},
		},
		{
			name: "bucket id as bucket finds bucket",
			want: &platform.Bucket{
				ID: platform2.ID(1),
			},
			args: args{
				ctx: context.Background(),
				r:   httptest.NewRequest(http.MethodPost, "/api/v2/query?bucket=0000000000000001", nil),
				svc: &mock.BucketService{
					FindBucketFn: func(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
						if *filter.ID == platform2.ID(1) {
							return &platform.Bucket{
								ID: platform2.ID(1),
							}, nil
						}
						return nil, &errors.Error{
							Code: errors.EInvalid,
							Msg:  "unknown bucket name",
						}
					},
				},
			},
		},
		{
			name: "invalid orgID fails to return bucket",
			want: &platform.Bucket{
				ID: platform2.ID(1),
			},
			args: args{
				ctx: context.Background(),
				r:   httptest.NewRequest(http.MethodPost, "/api/v2/query?bucket=0000000000000001", nil),
				svc: &mock.BucketService{
					FindBucketFn: func(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
						if *filter.OrganizationID == platform2.ID(1) {
							return &platform.Bucket{
								ID: platform2.ID(1),
							}, nil
						}
						return nil, &errors.Error{
							Code: errors.EInvalid,
							Msg:  "unknown bucket",
						}
					},
				},
			},
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := queryBucket(tt.args.ctx, platform2.ID(1), tt.args.r, tt.args.svc)
			if (err != nil) != tt.wantErr {
				t.Errorf("queryBucket() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("queryBucket() = %v, want %v", got, tt.want)
			}
		})
	}
}
