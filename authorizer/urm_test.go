package authorizer_test

import (
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

type OrgService struct {
	OrgID platform.ID
}

func (s *OrgService) FindResourceOrganizationID(ctx context.Context, rt influxdb.ResourceType, id platform.ID) (platform.ID, error) {
	return s.OrgID, nil
}

func TestURMService_FindUserResourceMappings(t *testing.T) {
	type fields struct {
		UserResourceMappingService influxdb.UserResourceMappingService
		OrgService                 authorizer.OrgIDResolver
	}
	type args struct {
		permission influxdb.Permission
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
				OrgService: &OrgService{OrgID: 10},
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
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.BucketsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
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
			name: "authorized to see all users",
			fields: fields{
				OrgService: &OrgService{OrgID: 10},
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
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.BucketsResourceType,
						OrgID: influxdbtesting.IDPtr(11),
					},
				},
			},
			wants: wants{
				urms: []*influxdb.UserResourceMapping{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewURMService(tt.fields.OrgService, tt.fields.UserResourceMappingService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

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
		OrgService                 authorizer.OrgIDResolver
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
				OrgService: &OrgService{OrgID: 10},
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
				OrgService: &OrgService{OrgID: 10},
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
					Msg:  "write:orgs/000000000000000a/buckets/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewURMService(tt.fields.OrgService, tt.fields.UserResourceMappingService)

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
