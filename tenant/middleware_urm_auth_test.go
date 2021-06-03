package tenant

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/feature"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

var idOne platform.ID = 1
var idTwo platform.ID = 2
var idThree platform.ID = 3

func TestURMService_FindUserResourceMappings(t *testing.T) {
	type fields struct {
		UserResourceMappingService influxdb.UserResourceMappingService
		OrgService                 influxdb.OrganizationService
	}
	type args struct {
		permissions []influxdb.Permission
	}
	type wants struct {
		err  error
		urms []*influxdb.UserResourceMapping
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all users",
			fields: fields{
				UserResourceMappingService: &mock.UserResourceMappingService{
					FindMappingsFn: func(ctx context.Context, filter influxdb.UserResourceMappingFilter) ([]*influxdb.UserResourceMapping, int, error) {
						return []*influxdb.UserResourceMapping{
							{
								ResourceID:   1,
								ResourceType: influxdb.BucketsResourceType,
							},
							{
								ResourceID:   2,
								ResourceType: influxdb.BucketsResourceType,
							},
							{
								ResourceID:   3,
								ResourceType: influxdb.BucketsResourceType,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							// ID:    &idOne,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type:  influxdb.BucketsResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type:  influxdb.BucketsResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				urms: []*influxdb.UserResourceMapping{
					{
						ResourceID:   1,
						ResourceType: influxdb.BucketsResourceType,
					},
					{
						ResourceID:   2,
						ResourceType: influxdb.BucketsResourceType,
					},
					{
						ResourceID:   3,
						ResourceType: influxdb.BucketsResourceType,
					},
				},
			},
		},
		{
			name: "authorized to see all users by org auth",
			fields: fields{
				UserResourceMappingService: &mock.UserResourceMappingService{
					FindMappingsFn: func(ctx context.Context, filter influxdb.UserResourceMappingFilter) ([]*influxdb.UserResourceMapping, int, error) {
						return []*influxdb.UserResourceMapping{
							{
								ResourceID:   1,
								ResourceType: influxdb.BucketsResourceType,
							},
							{
								ResourceID:   2,
								ResourceType: influxdb.BucketsResourceType,
							},
							{
								ResourceID:   3,
								ResourceType: influxdb.BucketsResourceType,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type:  influxdb.BucketsResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: ErrNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewAuthedURMService(tt.fields.OrgService, tt.fields.UserResourceMappingService)
			orgID := influxdbtesting.IDPtr(10)
			ctx := context.WithValue(context.Background(), kithttp.CtxOrgKey, *orgID)
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))
			ctx, _ = feature.Annotate(ctx, feature.DefaultFlagger(), feature.MakeBoolFlag("Org Only Member list",
				"orgOnlyMemberList",
				"Compute Team",
				true,
				feature.Temporary,
				false,
			))

			urms, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(urms, tt.wants.urms); diff != "" {
				t.Errorf("urms are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestURMService_FindUserResourceMappingsBucketAuth(t *testing.T) {
	type fields struct {
		UserResourceMappingService influxdb.UserResourceMappingService
		OrgService                 influxdb.OrganizationService
	}
	type args struct {
		permissions []influxdb.Permission
	}
	type wants struct {
		err  error
		urms []*influxdb.UserResourceMapping
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all users by bucket auth",
			fields: fields{
				UserResourceMappingService: &mock.UserResourceMappingService{
					FindMappingsFn: func(ctx context.Context, filter influxdb.UserResourceMappingFilter) ([]*influxdb.UserResourceMapping, int, error) {
						return []*influxdb.UserResourceMapping{
							{
								ResourceID:   1,
								ResourceType: influxdb.BucketsResourceType,
							},
							{
								ResourceID:   2,
								ResourceType: influxdb.BucketsResourceType,
							},
							{
								ResourceID:   3,
								ResourceType: influxdb.BucketsResourceType,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   &idOne,
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   &idTwo,
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   &idThree,
						},
					},
				},
			},
			wants: wants{
				urms: []*influxdb.UserResourceMapping{
					{
						ResourceID:   1,
						ResourceType: influxdb.BucketsResourceType,
					},
					{
						ResourceID:   2,
						ResourceType: influxdb.BucketsResourceType,
					},
					{
						ResourceID:   3,
						ResourceType: influxdb.BucketsResourceType,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewAuthedURMService(tt.fields.OrgService, tt.fields.UserResourceMappingService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			urms, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(urms, tt.wants.urms); diff != "" {
				t.Errorf("urms are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestURMService_WriteUserResourceMapping(t *testing.T) {
	type fields struct {
		UserResourceMappingService influxdb.UserResourceMappingService
		OrgService                 influxdb.OrganizationService
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
			name: "authorized to write urm",
			fields: fields{
				UserResourceMappingService: &mock.UserResourceMappingService{
					CreateMappingFn: func(ctx context.Context, m *influxdb.UserResourceMapping) error {
						return nil
					},
					DeleteMappingFn: func(ctx context.Context, rid, uid platform.ID) error {
						return nil
					},
					FindMappingsFn: func(ctx context.Context, filter influxdb.UserResourceMappingFilter) ([]*influxdb.UserResourceMapping, int, error) {
						return []*influxdb.UserResourceMapping{
							{
								ResourceID:   1,
								ResourceType: influxdb.BucketsResourceType,
								UserID:       100,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.BucketsResourceType,
						ID:    &idOne,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to write urm",
			fields: fields{
				UserResourceMappingService: &mock.UserResourceMappingService{
					CreateMappingFn: func(ctx context.Context, m *influxdb.UserResourceMapping) error {
						return nil
					},
					DeleteMappingFn: func(ctx context.Context, rid, uid platform.ID) error {
						return nil
					},
					FindMappingsFn: func(ctx context.Context, filter influxdb.UserResourceMappingFilter) ([]*influxdb.UserResourceMapping, int, error) {
						return []*influxdb.UserResourceMapping{
							{
								ResourceID:   1,
								ResourceType: influxdb.BucketsResourceType,
								UserID:       100,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.BucketsResourceType,
						OrgID: influxdbtesting.IDPtr(11),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:buckets/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewAuthedURMService(tt.fields.OrgService, tt.fields.UserResourceMappingService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			t.Run("create urm", func(t *testing.T) {
				err := s.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{ResourceType: influxdb.BucketsResourceType, ResourceID: 1})
				influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
			})

			t.Run("delete urm", func(t *testing.T) {
				err := s.DeleteUserResourceMapping(ctx, 1, 100)
				influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
			})

		})
	}
}
