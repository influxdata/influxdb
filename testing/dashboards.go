package testing

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
)

const (
	dashOneID   = "020f755c3c082000"
	dashTwoID   = "020f755c3c082001"
	dashThreeID = "020f755c3c082002"
)

func int32Ptr(i int32) *int32 {
	return &i
}

var dashboardCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
}

// DashboardFields will include the IDGenerator, and dashboards
type DashboardFields struct {
	IDGenerator platform.IDGenerator
	NowFn       func() time.Time
	Dashboards  []*platform.Dashboard
	Views       []*platform.View
}

// DashboardService tests all the service functions.
func DashboardService(
	init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()),
			t *testing.T)
	}{
		{
			name: "FindDashboardByID",
			fn:   FindDashboardByID,
		},
		{
			name: "FindDashboards",
			fn:   FindDashboards,
		},
		{
			name: "CreateDashboard",
			fn:   CreateDashboard,
		},
		{
			name: "UpdateDashboard",
			fn:   UpdateDashboard,
		},
		{
			name: "DeleteDashboard",
			fn:   DeleteDashboard,
		},
		{
			name: "AddDashboardCell",
			fn:   AddDashboardCell,
		},
		{
			name: "RemoveDashboardCell",
			fn:   RemoveDashboardCell,
		},
		{
			name: "UpdateDashboardCell",
			fn:   UpdateDashboardCell,
		},
		{
			name: "ReplaceDashboardCells",
			fn:   ReplaceDashboardCells,
		},
		{
			name: "GetDashboardCellView",
			fn:   GetDashboardCellView,
		},
		{
			name: "UpdateDashboardCellView",
			fn:   UpdateDashboardCellView,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// CreateDashboard testing
func CreateDashboard(
	init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()),
	t *testing.T,
) {
	type args struct {
		dashboard *platform.Dashboard
	}
	type wants struct {
		err        error
		dashboards []*platform.Dashboard
	}

	tests := []struct {
		name   string
		fields DashboardFields
		args   args
		wants  wants
	}{
		{
			name: "basic create dashboard",
			fields: DashboardFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
				},
			},
			args: args{
				dashboard: &platform.Dashboard{
					ID:             MustIDBase16(dashTwoID),
					OrganizationID: 1,
					Name:           "dashboard2",
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "dashboard2",
						Meta: platform.DashboardMeta{
							CreatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
							UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
					},
				},
			},
		},
		{
			name: "create dashboard with missing id",
			fields: DashboardFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
				},
			},
			args: args{
				dashboard: &platform.Dashboard{
					OrganizationID: 1,
					Name:           "dashboard2",
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "dashboard2",
						Meta: platform.DashboardMeta{
							CreatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
							UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateDashboard(ctx, tt.args.dashboard)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			defer s.DeleteDashboard(ctx, tt.args.dashboard.ID)

			dashboards, _, err := s.FindDashboards(ctx, platform.DashboardFilter{}, platform.DefaultDashboardFindOptions)
			if err != nil {
				t.Fatalf("failed to retrieve dashboards: %v", err)
			}
			if diff := cmp.Diff(dashboards, tt.wants.dashboards, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboards are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// AddDashboardCell testing
func AddDashboardCell(
	init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()),
	t *testing.T,
) {
	type args struct {
		dashboardID platform.ID
		cell        *platform.Cell
	}
	type wants struct {
		err        error
		dashboards []*platform.Dashboard
	}

	tests := []struct {
		name   string
		fields DashboardFields
		args   args
		wants  wants
	}{
		{
			name: "basic add cell",
			fields: DashboardFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
				},
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID: MustIDBase16(dashTwoID),
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(dashOneID),
				cell: &platform.Cell{
					ID: MustIDBase16(dashTwoID),
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Meta: platform.DashboardMeta{
							UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
							},
						},
					},
				},
			},
		},
		{
			name: "add cell with no id",
			fields: DashboardFields{
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
				},
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID: MustIDBase16(dashTwoID),
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(dashOneID),
				cell:        &platform.Cell{},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Meta: platform.DashboardMeta{
							UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
							},
						},
					},
				},
			},
		},
		{
			name: "add cell with id not exist",
			fields: DashboardFields{
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
				},
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID: MustIDBase16(dashTwoID),
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(threeID),
				cell:        &platform.Cell{},
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpAddDashboardCell,
					Msg:  platform.ErrDashboardNotFound,
				},
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.AddDashboardCell(ctx, tt.args.dashboardID, tt.args.cell, platform.AddDashboardCellOptions{})
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			defer s.DeleteDashboard(ctx, tt.args.dashboardID)

			dashboards, _, err := s.FindDashboards(ctx, platform.DashboardFilter{}, platform.DefaultDashboardFindOptions)
			if err != nil {
				t.Fatalf("failed to retrieve dashboards: %v", err)
			}
			if diff := cmp.Diff(dashboards, tt.wants.dashboards, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboards are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindDashboardByID testing
func FindDashboardByID(
	init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()),
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err       error
		dashboard *platform.Dashboard
	}

	tests := []struct {
		name   string
		fields DashboardFields
		args   args
		wants  wants
	}{
		{
			name: "basic find dashboard by id",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "dashboard2",
					},
				},
			},
			args: args{
				id: MustIDBase16(dashTwoID),
			},
			wants: wants{
				dashboard: &platform.Dashboard{
					ID:             MustIDBase16(dashTwoID),
					OrganizationID: 1,
					Name:           "dashboard2",
				},
			},
		},
		{
			name: "find dashboard by id not exists",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "dashboard2",
					},
				},
			},
			args: args{
				id: MustIDBase16(threeID),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpFindDashboardByID,
					Msg:  platform.ErrDashboardNotFound,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			dashboard, err := s.FindDashboardByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(dashboard, tt.wants.dashboard, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboard is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindDashboards testing
func FindDashboards(
	init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()),
	t *testing.T,
) {
	type args struct {
		IDs            []*platform.ID
		organizationID *platform.ID
		findOptions    platform.FindOptions
	}

	type wants struct {
		dashboards []*platform.Dashboard
		err        error
	}

	tests := []struct {
		name   string
		fields DashboardFields
		args   args
		wants  wants
	}{
		{
			name: "find all dashboards",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
					},
				},
			},
			args: args{
				findOptions: platform.DefaultDashboardFindOptions,
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
					},
				},
			},
		},
		{
			name: "find all dashboards by offset and limit",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
					},
					{
						ID:             MustIDBase16(dashThreeID),
						OrganizationID: 1,
						Name:           "321",
					},
				},
			},
			args: args{
				findOptions: platform.FindOptions{
					Limit:  1,
					Offset: 1,
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
					},
				},
			},
		},
		{
			name: "find all dashboards with limit",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
					},
					{
						ID:             MustIDBase16(dashThreeID),
						OrganizationID: 2,
						Name:           "321",
					},
				},
			},
			args: args{
				findOptions: platform.FindOptions{
					Limit: 1,
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
					},
				},
			},
		},
		{
			name: "find all dashboards by descending",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
					},
					{
						ID:             MustIDBase16(dashThreeID),
						OrganizationID: 1,
						Name:           "321",
					},
				},
			},
			args: args{
				findOptions: platform.FindOptions{
					Descending: true,
					Offset:     1,
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
					},
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
					},
				},
			},
		},
		{
			name: "find all dashboards by org 10",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
					},
					{
						ID:             2,
						OrganizationID: 10,
						Name:           "hello",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
					},
					{
						ID:             3,
						OrganizationID: 10,
						Name:           "world",
					},
				},
			},
			args: args{
				findOptions:    platform.DefaultDashboardFindOptions,
				organizationID: idPtr(10),
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             2,
						OrganizationID: 10,
						Name:           "hello",
					},
					{
						ID:             3,
						OrganizationID: 10,
						Name:           "world",
					},
				},
			},
		},
		{
			name: "find all dashboards sorted by created at",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Meta: platform.DashboardMeta{
							CreatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
						Name: "abc",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Meta: platform.DashboardMeta{
							CreatedAt: time.Date(2004, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
						Name: "xyz",
					},
				},
			},
			args: args{
				findOptions: platform.FindOptions{
					SortBy: "CreatedAt",
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
						Meta: platform.DashboardMeta{
							CreatedAt: time.Date(2004, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
					},
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
						Meta: platform.DashboardMeta{
							CreatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
					},
				},
			},
		},
		{
			name: "find all dashboards sorted by updated at",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Meta: platform.DashboardMeta{
							UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
						Name: "abc",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Meta: platform.DashboardMeta{
							UpdatedAt: time.Date(2010, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
						Name: "xyz",
					},
				},
			},
			args: args{
				findOptions: platform.FindOptions{
					SortBy: "UpdatedAt",
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Meta: platform.DashboardMeta{
							UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
						Name: "abc",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Meta: platform.DashboardMeta{
							UpdatedAt: time.Date(2010, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
						Name: "xyz",
					},
				},
			},
		},
		{
			name: "find dashboard by id",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
					},
				},
			},
			args: args{
				IDs: []*platform.ID{
					idPtr(MustIDBase16(dashTwoID)),
				},
				findOptions: platform.DefaultDashboardFindOptions,
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
					},
				},
			},
		},
		{
			name: "find multiple dashboards by id",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
					},
				},
			},
			args: args{
				IDs: []*platform.ID{
					idPtr(MustIDBase16(dashOneID)),
					idPtr(MustIDBase16(dashTwoID)),
				},
				findOptions: platform.DefaultDashboardFindOptions,
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
					},
				},
			},
		},
		{
			name: "find multiple dashboards by id not exists",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "abc",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "xyz",
					},
				},
			},
			args: args{
				IDs: []*platform.ID{
					idPtr(MustIDBase16(threeID)),
				},
				findOptions: platform.DefaultDashboardFindOptions,
			},
			wants: wants{
				dashboards: []*platform.Dashboard{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			filter := platform.DashboardFilter{}
			if tt.args.IDs != nil {
				filter.IDs = tt.args.IDs
			}

			if tt.args.organizationID != nil {
				filter.OrganizationID = tt.args.organizationID
			}

			dashboards, _, err := s.FindDashboards(ctx, filter, tt.args.findOptions)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(dashboards, tt.wants.dashboards, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboards are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteDashboard testing
func DeleteDashboard(
	init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID platform.ID
	}
	type wants struct {
		err        error
		dashboards []*platform.Dashboard
	}

	tests := []struct {
		name   string
		fields DashboardFields
		args   args
		wants  wants
	}{
		{
			name: "delete dashboards using exist id",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						Name:           "A",
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
					},
					{
						Name:           "B",
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
					},
				},
			},
			args: args{
				ID: MustIDBase16(dashOneID),
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						Name:           "B",
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
					},
				},
			},
		},
		{
			name: "delete dashboards using id that does not exist",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						Name:           "A",
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
					},
					{
						Name:           "B",
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
					},
				},
			},
			args: args{
				ID: MustIDBase16(dashThreeID),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpDeleteDashboard,
					Msg:  platform.ErrDashboardNotFound,
				},
				dashboards: []*platform.Dashboard{
					{
						Name:           "A",
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
					},
					{
						Name:           "B",
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteDashboard(ctx, tt.args.ID)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			filter := platform.DashboardFilter{}
			dashboards, _, err := s.FindDashboards(ctx, filter, platform.DefaultDashboardFindOptions)
			if err != nil {
				t.Fatalf("failed to retrieve dashboards: %v", err)
			}
			if diff := cmp.Diff(dashboards, tt.wants.dashboards, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboards are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateDashboard testing
func UpdateDashboard(
	init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()),
	t *testing.T,
) {
	type args struct {
		name        string
		description string
		id          platform.ID
	}
	type wants struct {
		err       error
		dashboard *platform.Dashboard
	}

	tests := []struct {
		name   string
		fields DashboardFields
		args   args
		wants  wants
	}{
		{
			name: "update name",
			fields: DashboardFields{
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "dashboard2",
					},
				},
			},
			args: args{
				id:   MustIDBase16(dashOneID),
				name: "changed",
			},
			wants: wants{
				dashboard: &platform.Dashboard{
					ID:             MustIDBase16(dashOneID),
					OrganizationID: 1,
					Meta: platform.DashboardMeta{
						UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
					},
					Name: "changed",
				},
			},
		},
		{
			name: "update description",
			fields: DashboardFields{
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "dashboard2",
					},
				},
			},
			args: args{
				id:          MustIDBase16(dashOneID),
				description: "changed",
			},
			wants: wants{
				dashboard: &platform.Dashboard{
					ID:             MustIDBase16(dashOneID),
					OrganizationID: 1,
					Name:           "dashboard1",
					Description:    "changed",
					Meta: platform.DashboardMeta{
						UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "update description and name",
			fields: DashboardFields{
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "dashboard2",
					},
				},
			},
			args: args{
				id:          MustIDBase16(dashOneID),
				description: "changed",
				name:        "changed",
			},
			wants: wants{
				dashboard: &platform.Dashboard{
					ID:             MustIDBase16(dashOneID),
					OrganizationID: 1,
					Name:           "changed",
					Description:    "changed",
					Meta: platform.DashboardMeta{
						UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "update with id not exist",
			fields: DashboardFields{
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
					},
					{
						ID:             MustIDBase16(dashTwoID),
						OrganizationID: 1,
						Name:           "dashboard2",
					},
				},
			},
			args: args{
				id:          MustIDBase16(threeID),
				description: "changed",
				name:        "changed",
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpUpdateDashboard,
					Msg:  platform.ErrDashboardNotFound,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			upd := platform.DashboardUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}
			if tt.args.description != "" {
				upd.Description = &tt.args.description
			}

			dashboard, err := s.UpdateDashboard(ctx, tt.args.id, upd)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(dashboard, tt.wants.dashboard, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboard is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// RemoveDashboardCell testing
func RemoveDashboardCell(
	init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()),
	t *testing.T,
) {
	type args struct {
		dashboardID platform.ID
		cellID      platform.ID
	}
	type wants struct {
		err        error
		dashboards []*platform.Dashboard
	}

	tests := []struct {
		name   string
		fields DashboardFields
		args   args
		wants  wants
	}{
		{
			name: "basic remove cell",
			fields: DashboardFields{
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
							},
							{
								ID: MustIDBase16(dashOneID),
							},
						},
					},
				},
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID: MustIDBase16(dashTwoID),
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(dashOneID),
				cellID:      MustIDBase16(dashTwoID),
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Meta: platform.DashboardMeta{
							UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashOneID),
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.RemoveDashboardCell(ctx, tt.args.dashboardID, tt.args.cellID)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			defer s.DeleteDashboard(ctx, tt.args.dashboardID)

			dashboards, _, err := s.FindDashboards(ctx, platform.DashboardFilter{}, platform.DefaultDashboardFindOptions)
			if err != nil {
				t.Fatalf("failed to retrieve dashboards: %v", err)
			}
			if diff := cmp.Diff(dashboards, tt.wants.dashboards, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboards are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateDashboardCell testing
func UpdateDashboardCell(
	init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()),
	t *testing.T,
) {
	type args struct {
		dashboardID platform.ID
		cellID      platform.ID
		cellUpdate  platform.CellUpdate
	}
	type wants struct {
		err        error
		dashboards []*platform.Dashboard
	}

	tests := []struct {
		name   string
		fields DashboardFields
		args   args
		wants  wants
	}{
		{
			name: "basic update cell",
			fields: DashboardFields{
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
							},
							{
								ID: MustIDBase16(dashOneID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(dashOneID),
				cellID:      MustIDBase16(dashTwoID),
				cellUpdate: platform.CellUpdate{
					X: func(i int32) *int32 { return &i }(int32(10)),
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Meta: platform.DashboardMeta{
							UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
								X:  10,
							},
							{
								ID: MustIDBase16(dashOneID),
							},
						},
					},
				},
			},
		},
		{
			name: "invalid cell update without attribute",
			fields: DashboardFields{
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
							},
							{
								ID: MustIDBase16(dashOneID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(dashOneID),
				cellID:      MustIDBase16(dashTwoID),
				cellUpdate:  platform.CellUpdate{},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
							},
							{
								ID: MustIDBase16(dashOneID),
							},
						},
					},
				},
				err: &platform.Error{
					Code: platform.EInvalid,
					Op:   platform.OpUpdateDashboardCell,
					Msg:  "must update at least one attribute",
				},
			},
		},
		{
			name: "invalid cell update cell id not exist",
			fields: DashboardFields{
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
							},
							{
								ID: MustIDBase16(dashOneID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(dashOneID),
				cellID:      MustIDBase16(fourID),
				cellUpdate: platform.CellUpdate{
					X: int32Ptr(1),
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
							},
							{
								ID: MustIDBase16(dashOneID),
							},
						},
					},
				},
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpUpdateDashboardCell,
					Msg:  platform.ErrCellNotFound,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			_, err := s.UpdateDashboardCell(ctx, tt.args.dashboardID, tt.args.cellID, tt.args.cellUpdate)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			defer s.DeleteDashboard(ctx, tt.args.dashboardID)

			dashboards, _, err := s.FindDashboards(ctx, platform.DashboardFilter{}, platform.DefaultDashboardFindOptions)
			if err != nil {
				t.Fatalf("failed to retrieve dashboards: %v", err)
			}
			if diff := cmp.Diff(dashboards, tt.wants.dashboards, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboards are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// ReplaceDashboardCells testing
func ReplaceDashboardCells(
	init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()),
	t *testing.T,
) {
	type args struct {
		dashboardID platform.ID
		cells       []*platform.Cell
	}
	type wants struct {
		err        error
		dashboards []*platform.Dashboard
	}

	tests := []struct {
		name   string
		fields DashboardFields
		args   args
		wants  wants
	}{
		{
			name: "basic replace cells",
			fields: DashboardFields{
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID: MustIDBase16(dashTwoID),
						},
					},
					{
						ViewContents: platform.ViewContents{
							ID: MustIDBase16(dashOneID),
						},
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
							},
							{
								ID: MustIDBase16(dashOneID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(dashOneID),
				cells: []*platform.Cell{
					{
						ID: MustIDBase16(dashTwoID),
						X:  10,
					},
					{
						ID: MustIDBase16(dashOneID),
						Y:  11,
					},
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
						Meta: platform.DashboardMeta{
							UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
						},
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
								X:  10,
							},
							{
								ID: MustIDBase16(dashOneID),
								Y:  11,
							},
						},
					},
				},
			},
		},
		{
			name: "try to add a cell that didn't previously exist",
			fields: DashboardFields{
				NowFn: func() time.Time { return time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC) },
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID: MustIDBase16(dashTwoID),
						},
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(dashOneID),
				cells: []*platform.Cell{
					{
						ID: MustIDBase16(dashTwoID),
						X:  10,
					},
					{
						ID: MustIDBase16(dashOneID),
						Y:  11,
					},
				},
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.EConflict,
					Op:   platform.OpReplaceDashboardCells,
					Msg:  "cannot replace cells that were not already present",
				},
				dashboards: []*platform.Dashboard{
					{
						ID:             MustIDBase16(dashOneID),
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: MustIDBase16(dashTwoID),
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.ReplaceDashboardCells(ctx, tt.args.dashboardID, tt.args.cells)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			defer s.DeleteDashboard(ctx, tt.args.dashboardID)

			dashboards, _, err := s.FindDashboards(ctx, platform.DashboardFilter{}, platform.DefaultDashboardFindOptions)
			if err != nil {
				t.Fatalf("failed to retrieve dashboards: %v", err)
			}
			if diff := cmp.Diff(dashboards, tt.wants.dashboards, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboards are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// GetDashboardCellView is the conformance test for the retrieving a dashboard cell.
func GetDashboardCellView(
	init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()),
	t *testing.T,
) {
	type args struct {
		dashboardID platform.ID
		cellID      platform.ID
	}
	type wants struct {
		err  error
		view *platform.View
	}

	tests := []struct {
		name   string
		fields DashboardFields
		args   args
		wants  wants
	}{
		{
			name: "get view for cell that exists",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             1,
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: 100,
							},
						},
					},
				},
			},
			args: args{
				dashboardID: 1,
				cellID:      100,
			},
			wants: wants{
				view: &platform.View{
					ViewContents: platform.ViewContents{
						ID: 100,
					},
					Properties: platform.EmptyViewProperties{},
				},
			},
		},
		{
			name: "get view for cell that does not exist",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             1,
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: 100,
							},
						},
					},
				},
			},
			args: args{
				dashboardID: 1,
				cellID:      5,
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpGetDashboardCellView,
					Msg:  platform.ErrViewNotFound,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			view, err := s.GetDashboardCellView(ctx, tt.args.dashboardID, tt.args.cellID)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(view, tt.wants.view); diff != "" {
				t.Errorf("dashboard cell views are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateDashboardCellView is the conformance test for the updating a dashboard cell.
func UpdateDashboardCellView(
	init func(DashboardFields, *testing.T) (platform.DashboardService, string, func()),
	t *testing.T,
) {
	type args struct {
		dashboardID platform.ID
		cellID      platform.ID
		properties  platform.ViewProperties
		name        string
	}
	type wants struct {
		err  error
		view *platform.View
	}

	tests := []struct {
		name   string
		fields DashboardFields
		args   args
		wants  wants
	}{
		{
			name: "update view name",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             1,
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: 100,
							},
						},
					},
				},
			},
			args: args{
				dashboardID: 1,
				cellID:      100,
				name:        "hello",
			},
			wants: wants{
				view: &platform.View{
					ViewContents: platform.ViewContents{
						ID:   100,
						Name: "hello",
					},
					Properties: platform.EmptyViewProperties{},
				},
			},
		},
		{
			name: "update view type",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             1,
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: 100,
							},
						},
					},
				},
			},
			args: args{
				dashboardID: 1,
				cellID:      100,
				properties: platform.TableViewProperties{
					Type:       "table",
					TimeFormat: "rfc3339",
				},
			},
			wants: wants{
				view: &platform.View{
					ViewContents: platform.ViewContents{
						ID: 100,
					},
					Properties: platform.TableViewProperties{
						Type:       "table",
						TimeFormat: "rfc3339",
					},
				},
			},
		},
		{
			name: "update view type and name",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             1,
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: 100,
							},
						},
					},
				},
			},
			args: args{
				dashboardID: 1,
				cellID:      100,
				name:        "hello",
				properties: platform.TableViewProperties{
					Type:       "table",
					TimeFormat: "rfc3339",
				},
			},
			wants: wants{
				view: &platform.View{
					ViewContents: platform.ViewContents{
						ID:   100,
						Name: "hello",
					},
					Properties: platform.TableViewProperties{
						Type:       "table",
						TimeFormat: "rfc3339",
					},
				},
			},
		},
		{
			name: "update view for cell that does not exist",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:             1,
						OrganizationID: 1,
						Name:           "dashboard1",
						Cells: []*platform.Cell{
							{
								ID: 100,
							},
						},
					},
				},
			},
			args: args{
				dashboardID: 1,
				cellID:      5,
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpGetDashboardCellView,
					Msg:  platform.ErrViewNotFound,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			upd := platform.ViewUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}

			if tt.args.properties != nil {
				upd.Properties = tt.args.properties
			}

			view, err := s.UpdateDashboardCellView(ctx, tt.args.dashboardID, tt.args.cellID, upd)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(view, tt.wants.view); diff != "" {
				t.Errorf("dashboard cell views are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
