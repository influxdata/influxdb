package dbrp_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestAuth_FindByID(t *testing.T) {
	type fields struct {
		service influxdb.DBRPMappingServiceV2
	}
	type args struct {
		orgID      influxdb.ID
		id         influxdb.ID
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
			name: "authorized to access id by org id",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.DBRPResourceType,
						OrgID: influxdbtesting.IDPtr(1),
					},
				},
				id:    1,
				orgID: 1,
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "authorized to access id by id",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.DBRPResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
				id:    1,
				orgID: 1,
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to access id by org id",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.DBRPResourceType,
						OrgID: influxdbtesting.IDPtr(1),
					},
				},
				id:    1,
				orgID: 2,
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "read:orgs/0000000000000002/dbrp/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
		{
			name: "unauthorized to access id by id",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.DBRPResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id:    1,
				orgID: 2,
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "read:orgs/0000000000000002/dbrp/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := dbrp.NewAuthorizedService(tt.fields.service)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindByID(ctx, tt.args.orgID, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestAuth_FindMany(t *testing.T) {
	type fields struct {
		service influxdb.DBRPMappingServiceV2
	}
	type args struct {
		filter      influxdb.DBRPMappingFilterV2
		permissions []influxdb.Permission
	}
	type wants struct {
		err error
		ms  []*influxdb.DBRPMappingV2
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "no result",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{
					FindManyFn: func(ctx context.Context, dbrp influxdb.DBRPMappingFilterV2, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMappingV2, int, error) {
						return []*influxdb.DBRPMappingV2{
							{
								ID:             1,
								OrganizationID: 1,
								BucketID:       1,
							},
							{
								ID:             2,
								OrganizationID: 1,
								BucketID:       2,
							},
							{
								ID:             3,
								OrganizationID: 2,
								BucketID:       3,
							},
							{
								ID:             4,
								OrganizationID: 3,
								BucketID:       4,
							},
						}, 4, nil
					},
				},
			},
			args: args{
				permissions: []influxdb.Permission{{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.DBRPResourceType,
						OrgID: influxdbtesting.IDPtr(42),
					},
				}},
				filter: influxdb.DBRPMappingFilterV2{},
			},
			wants: wants{
				err: nil,
				ms:  []*influxdb.DBRPMappingV2{},
			},
		},
		{
			name: "partial",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{
					FindManyFn: func(ctx context.Context, dbrp influxdb.DBRPMappingFilterV2, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMappingV2, int, error) {
						return []*influxdb.DBRPMappingV2{
							{
								ID:             1,
								OrganizationID: 1,
								BucketID:       1,
							},
							{
								ID:             2,
								OrganizationID: 1,
								BucketID:       2,
							},
							{
								ID:             3,
								OrganizationID: 2,
								BucketID:       3,
							},
							{
								ID:             4,
								OrganizationID: 3,
								BucketID:       4,
							},
						}, 4, nil
					},
				},
			},
			args: args{
				permissions: []influxdb.Permission{{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.DBRPResourceType,
						OrgID: influxdbtesting.IDPtr(1),
					},
				}},
				filter: influxdb.DBRPMappingFilterV2{},
			},
			wants: wants{
				err: nil,
				ms: []*influxdb.DBRPMappingV2{
					{
						ID:             1,
						OrganizationID: 1,
						BucketID:       1,
					},
					{
						ID:             2,
						OrganizationID: 1,
						BucketID:       2,
					},
				},
			},
		},
		{
			name: "all",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{
					FindManyFn: func(ctx context.Context, dbrp influxdb.DBRPMappingFilterV2, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMappingV2, int, error) {
						return []*influxdb.DBRPMappingV2{
							{
								ID:             1,
								OrganizationID: 1,
								BucketID:       1,
							},
							{
								ID:             2,
								OrganizationID: 1,
								BucketID:       2,
							},
							{
								ID:             3,
								OrganizationID: 2,
								BucketID:       3,
							},
							{
								ID:             4,
								OrganizationID: 3,
								BucketID:       4,
							},
						}, 4, nil
					},
				},
			},
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type:  influxdb.DBRPResourceType,
							OrgID: influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type:  influxdb.DBRPResourceType,
							OrgID: influxdbtesting.IDPtr(2),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type:  influxdb.DBRPResourceType,
							OrgID: influxdbtesting.IDPtr(3),
						},
					},
				},
				filter: influxdb.DBRPMappingFilterV2{},
			},
			wants: wants{
				err: nil,
				ms: []*influxdb.DBRPMappingV2{
					{
						ID:             1,
						OrganizationID: 1,
						BucketID:       1,
					},
					{
						ID:             2,
						OrganizationID: 1,
						BucketID:       2,
					},
					{
						ID:             3,
						OrganizationID: 2,
						BucketID:       3,
					},
					{
						ID:             4,
						OrganizationID: 3,
						BucketID:       4,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := dbrp.NewAuthorizedService(tt.fields.service)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			gots, ngots, err := s.FindMany(ctx, tt.args.filter)
			if ngots != len(gots) {
				t.Errorf("got wrong number back")
			}
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
			if diff := cmp.Diff(tt.wants.ms, gots, influxdbtesting.DBRPMappingCmpOptionsV2...); diff != "" {
				t.Errorf("unexpected result -want/+got:\n\t%s", diff)
			}
		})
	}
}

func TestAuth_Create(t *testing.T) {
	type fields struct {
		service influxdb.DBRPMappingServiceV2
	}
	type args struct {
		m          influxdb.DBRPMappingV2
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
			name: "authorized",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{},
			},
			args: args{
				m: influxdb.DBRPMappingV2{
					ID:             1,
					OrganizationID: 1,
				},
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.DBRPResourceType,
						OrgID: influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{},
			},
			args: args{
				m: influxdb.DBRPMappingV2{
					ID:             1,
					OrganizationID: 1,
				},
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.DBRPResourceType,
						OrgID: influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/0000000000000001/dbrp is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := dbrp.NewAuthorizedService(tt.fields.service)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.Create(ctx, &tt.args.m)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestAuth_Update(t *testing.T) {
	type fields struct {
		service influxdb.DBRPMappingServiceV2
	}
	type args struct {
		orgID      influxdb.ID
		id         influxdb.ID
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
			name: "authorized",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.DBRPResourceType,
						OrgID: influxdbtesting.IDPtr(1),
					},
				},
				id:    1,
				orgID: 1,
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.DBRPResourceType,
						OrgID: influxdbtesting.IDPtr(1),
					},
				},
				id:    1,
				orgID: 1,
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/0000000000000001/dbrp/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := dbrp.NewAuthorizedService(tt.fields.service)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			// Does not matter how we update, we only need to check auth.
			err := s.Update(ctx, &influxdb.DBRPMappingV2{ID: tt.args.id, OrganizationID: tt.args.orgID, BucketID: 1})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestAuth_Delete(t *testing.T) {
	type fields struct {
		service influxdb.DBRPMappingServiceV2
	}
	type args struct {
		orgID      influxdb.ID
		id         influxdb.ID
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
			name: "authorized",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.DBRPResourceType,
						OrgID: influxdbtesting.IDPtr(1),
					},
				},
				id:    1,
				orgID: 1,
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized",
			fields: fields{
				service: &mock.DBRPMappingServiceV2{},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.DBRPResourceType,
						OrgID: influxdbtesting.IDPtr(1),
					},
				},
				id:    1,
				orgID: 1,
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/0000000000000001/dbrp/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := dbrp.NewAuthorizedService(tt.fields.service)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.Delete(ctx, tt.args.orgID, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
