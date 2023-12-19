package authorizer_test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

var dashboardCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Dashboard) []*influxdb.Dashboard {
		out := append([]*influxdb.Dashboard(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

func TestDashboardService_FindDashboardByID(t *testing.T) {
	type fields struct {
		DashboardService influxdb.DashboardService
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
				DashboardService: &mock.DashboardService{
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:             id,
							OrganizationID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.DashboardsResourceType,
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
				DashboardService: &mock.DashboardService{
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:             id,
							OrganizationID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.DashboardsResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/000000000000000a/dashboards/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewDashboardService(tt.fields.DashboardService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindDashboardByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestDashboardService_FindDashboards(t *testing.T) {
	type fields struct {
		DashboardService influxdb.DashboardService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err        error
		dashboards []*influxdb.Dashboard
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all dashboards",
			fields: fields{
				DashboardService: &mock.DashboardService{
					FindDashboardsF: func(ctx context.Context, filter influxdb.DashboardFilter, opt influxdb.FindOptions) ([]*influxdb.Dashboard, int, error) {
						return []*influxdb.Dashboard{
							{
								ID:             1,
								OrganizationID: 10,
							},
							{
								ID:             2,
								OrganizationID: 10,
							},
							{
								ID:             3,
								OrganizationID: 11,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.DashboardsResourceType,
					},
				},
			},
			wants: wants{
				dashboards: []*influxdb.Dashboard{
					{
						ID:             1,
						OrganizationID: 10,
					},
					{
						ID:             2,
						OrganizationID: 10,
					},
					{
						ID:             3,
						OrganizationID: 11,
					},
				},
			},
		},
		{
			name: "authorized to access a single orgs dashboards",
			fields: fields{
				DashboardService: &mock.DashboardService{

					FindDashboardsF: func(ctx context.Context, filter influxdb.DashboardFilter, opt influxdb.FindOptions) ([]*influxdb.Dashboard, int, error) {
						return []*influxdb.Dashboard{
							{
								ID:             1,
								OrganizationID: 10,
							},
							{
								ID:             2,
								OrganizationID: 10,
							},
							{
								ID:             3,
								OrganizationID: 11,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.DashboardsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				dashboards: []*influxdb.Dashboard{
					{
						ID:             1,
						OrganizationID: 10,
					},
					{
						ID:             2,
						OrganizationID: 10,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewDashboardService(tt.fields.DashboardService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			dashboards, _, err := s.FindDashboards(ctx, influxdb.DashboardFilter{}, influxdb.FindOptions{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(dashboards, tt.wants.dashboards, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboards are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestDashboardService_UpdateDashboard(t *testing.T) {
	type fields struct {
		DashboardService influxdb.DashboardService
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
			name: "authorized to update dashboard",
			fields: fields{
				DashboardService: &mock.DashboardService{
					FindDashboardByIDF: func(ctc context.Context, id platform.ID) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					UpdateDashboardF: func(ctx context.Context, id platform.ID, upd influxdb.DashboardUpdate) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:             1,
							OrganizationID: 10,
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
							Type: influxdb.DashboardsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.DashboardsResourceType,
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
			name: "unauthorized to update dashboard",
			fields: fields{
				DashboardService: &mock.DashboardService{
					FindDashboardByIDF: func(ctc context.Context, id platform.ID) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					UpdateDashboardF: func(ctx context.Context, id platform.ID, upd influxdb.DashboardUpdate) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:             1,
							OrganizationID: 10,
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
							Type: influxdb.DashboardsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/dashboards/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewDashboardService(tt.fields.DashboardService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			_, err := s.UpdateDashboard(ctx, tt.args.id, influxdb.DashboardUpdate{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestDashboardService_DeleteDashboard(t *testing.T) {
	type fields struct {
		DashboardService influxdb.DashboardService
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
			name: "authorized to delete dashboard",
			fields: fields{
				DashboardService: &mock.DashboardService{
					FindDashboardByIDF: func(ctc context.Context, id platform.ID) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					DeleteDashboardF: func(ctx context.Context, id platform.ID) error {
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
							Type: influxdb.DashboardsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.DashboardsResourceType,
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
			name: "unauthorized to delete dashboard",
			fields: fields{
				DashboardService: &mock.DashboardService{
					FindDashboardByIDF: func(ctc context.Context, id platform.ID) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					DeleteDashboardF: func(ctx context.Context, id platform.ID) error {
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
							Type: influxdb.DashboardsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/dashboards/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewDashboardService(tt.fields.DashboardService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.DeleteDashboard(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestDashboardService_CreateDashboard(t *testing.T) {
	type fields struct {
		DashboardService influxdb.DashboardService
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
			name: "authorized to create dashboard",
			fields: fields{
				DashboardService: &mock.DashboardService{
					CreateDashboardF: func(ctx context.Context, o *influxdb.Dashboard) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.DashboardsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create dashboard",
			fields: fields{
				DashboardService: &mock.DashboardService{
					CreateDashboardF: func(ctx context.Context, o *influxdb.Dashboard) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.DashboardsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/dashboards is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewDashboardService(tt.fields.DashboardService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.CreateDashboard(ctx, &influxdb.Dashboard{OrganizationID: tt.args.orgID})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestDashboardService_WriteDashboardCell(t *testing.T) {
	type fields struct {
		DashboardService influxdb.DashboardService
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
			name: "authorized to write dashboard cells/cell/view",
			fields: fields{
				DashboardService: &mock.DashboardService{
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:             id,
							OrganizationID: 10,
						}, nil
					},
					AddDashboardCellF: func(ctx context.Context, id platform.ID, c *influxdb.Cell, opts influxdb.AddDashboardCellOptions) error {
						return nil
					},
					RemoveDashboardCellF: func(ctx context.Context, id platform.ID, cid platform.ID) error {
						return nil
					},
					ReplaceDashboardCellsF: func(ctx context.Context, id platform.ID, cs []*influxdb.Cell) error {
						return nil
					},
					UpdateDashboardCellF: func(ctx context.Context, id platform.ID, cid platform.ID, upd influxdb.CellUpdate) (*influxdb.Cell, error) {
						return &influxdb.Cell{}, nil
					},
					UpdateDashboardCellViewF: func(ctx context.Context, id platform.ID, cid platform.ID, upd influxdb.ViewUpdate) (*influxdb.View, error) {
						return &influxdb.View{}, nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.DashboardsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to write dashboard cells/cell/view",
			fields: fields{
				DashboardService: &mock.DashboardService{
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:             id,
							OrganizationID: 10,
						}, nil
					},
					AddDashboardCellF: func(ctx context.Context, id platform.ID, c *influxdb.Cell, opts influxdb.AddDashboardCellOptions) error {
						return nil
					},
					ReplaceDashboardCellsF: func(ctx context.Context, id platform.ID, cs []*influxdb.Cell) error {
						return nil
					},
					UpdateDashboardCellF: func(ctx context.Context, id platform.ID, cid platform.ID, upd influxdb.CellUpdate) (*influxdb.Cell, error) {
						return &influxdb.Cell{}, nil
					},
					RemoveDashboardCellF: func(ctx context.Context, id platform.ID, cid platform.ID) error {
						return nil
					},
					UpdateDashboardCellViewF: func(ctx context.Context, id platform.ID, cid platform.ID, upd influxdb.ViewUpdate) (*influxdb.View, error) {
						return &influxdb.View{}, nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.DashboardsResourceType,
						ID:   influxdbtesting.IDPtr(100),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/dashboards/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewDashboardService(tt.fields.DashboardService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.AddDashboardCell(ctx, 1, &influxdb.Cell{}, influxdb.AddDashboardCellOptions{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			err = s.RemoveDashboardCell(ctx, 1, 2)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			_, err = s.UpdateDashboardCellView(ctx, 1, 2, influxdb.ViewUpdate{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			_, err = s.UpdateDashboardCell(ctx, 1, 2, influxdb.CellUpdate{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			err = s.ReplaceDashboardCells(ctx, 1, []*influxdb.Cell{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestDashboardService_FindDashboardCellView(t *testing.T) {
	type fields struct {
		DashboardService influxdb.DashboardService
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
			name: "authorized to read dashboard cells/cell/view",
			fields: fields{
				DashboardService: &mock.DashboardService{
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:             id,
							OrganizationID: 10,
						}, nil
					},
					GetDashboardCellViewF: func(ctx context.Context, id platform.ID, cid platform.ID) (*influxdb.View, error) {
						return &influxdb.View{}, nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.DashboardsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to read dashboard cells/cell/view",
			fields: fields{
				DashboardService: &mock.DashboardService{
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:             id,
							OrganizationID: 10,
						}, nil
					},
					GetDashboardCellViewF: func(ctx context.Context, id platform.ID, cid platform.ID) (*influxdb.View, error) {
						return &influxdb.View{}, nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.DashboardsResourceType,
						ID:   influxdbtesting.IDPtr(100),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/000000000000000a/dashboards/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewDashboardService(tt.fields.DashboardService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.GetDashboardCellView(ctx, 1, 1)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
