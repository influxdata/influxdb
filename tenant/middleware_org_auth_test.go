package tenant_test

import (
	"bytes"
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

var orgCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Organization) []*influxdb.Organization {
		out := append([]*influxdb.Organization(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

func TestOrgService_FindOrganizationByID(t *testing.T) {
	type fields struct {
		OrgService influxdb.OrganizationService
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
				OrgService: &mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID: id,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
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
				OrgService: &mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID: id,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tenant.NewAuthedOrgService(tt.fields.OrgService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindOrganizationByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestOrgService_FindOrganization(t *testing.T) {
	type fields struct {
		OrgService influxdb.OrganizationService
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
			name: "authorized to access org",
			fields: fields{
				OrgService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID: 1,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to access org",
			fields: fields{
				OrgService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID: 1,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tenant.NewAuthedOrgService(tt.fields.OrgService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindOrganization(ctx, influxdb.OrganizationFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestOrgService_FindOrganizations(t *testing.T) {
	type fields struct {
		OrgService influxdb.OrganizationService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err  error
		orgs []*influxdb.Organization
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all orgs",
			fields: fields{
				OrgService: &mock.OrganizationService{
					FindOrganizationsF: func(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
						return []*influxdb.Organization{
							{
								ID: 1,
							},
							{
								ID: 2,
							},
							{
								ID: 3,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
					},
				},
			},
			wants: wants{
				orgs: []*influxdb.Organization{
					{
						ID: 1,
					},
					{
						ID: 2,
					},
					{
						ID: 3,
					},
				},
			},
		},
		{
			name: "authorized to access a single org",
			fields: fields{
				OrgService: &mock.OrganizationService{
					FindOrganizationsF: func(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
						return []*influxdb.Organization{
							{
								ID: 1,
							},
							{
								ID: 2,
							},
							{
								ID: 3,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
			},
			wants: wants{
				orgs: []*influxdb.Organization{
					{
						ID: 2,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tenant.NewAuthedOrgService(tt.fields.OrgService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			orgs, _, err := s.FindOrganizations(ctx, influxdb.OrganizationFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(orgs, tt.wants.orgs, orgCmpOptions...); diff != "" {
				t.Errorf("organizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestOrgService_UpdateOrganization(t *testing.T) {
	type fields struct {
		OrgService influxdb.OrganizationService
	}
	type args struct {
		id         platform.ID
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
			name: "authorized to update org",
			fields: fields{
				OrgService: &mock.OrganizationService{
					UpdateOrganizationF: func(ctx context.Context, id platform.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID: 1,
						}, nil
					},
				},
			},
			args: args{
				id: 1,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to update org",
			fields: fields{
				OrgService: &mock.OrganizationService{
					UpdateOrganizationF: func(ctx context.Context, id platform.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID: 1,
						}, nil
					},
				},
			},
			args: args{
				id: 1,
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tenant.NewAuthedOrgService(tt.fields.OrgService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.UpdateOrganization(ctx, tt.args.id, influxdb.OrganizationUpdate{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestOrgService_DeleteOrganization(t *testing.T) {
	type fields struct {
		OrgService influxdb.OrganizationService
	}
	type args struct {
		id         platform.ID
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
			name: "authorized to delete org",
			fields: fields{
				OrgService: &mock.OrganizationService{
					DeleteOrganizationF: func(ctx context.Context, id platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: 1,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to delete org",
			fields: fields{
				OrgService: &mock.OrganizationService{
					DeleteOrganizationF: func(ctx context.Context, id platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: 1,
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tenant.NewAuthedOrgService(tt.fields.OrgService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.DeleteOrganization(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestOrgService_CreateOrganization(t *testing.T) {
	type fields struct {
		OrgService influxdb.OrganizationService
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
			name: "authorized to create org",
			fields: fields{
				OrgService: &mock.OrganizationService{
					CreateOrganizationF: func(ctx context.Context, o *influxdb.Organization) error {
						return nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create org",
			fields: fields{
				OrgService: &mock.OrganizationService{
					CreateOrganizationF: func(ctx context.Context, o *influxdb.Organization) error {
						return nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tenant.NewAuthedOrgService(tt.fields.OrgService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.CreateOrganization(ctx, &influxdb.Organization{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
