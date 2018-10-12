package testing

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
)

const (
	dashOneID   = "020f755c3c082000"
	dashTwoID   = "020f755c3c082001"
	dashThreeID = "020f755c3c082002"
)

func idPtr(id platform.ID) *platform.ID {
	return &id
}

var dashboardCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.Dashboard) []*platform.Dashboard {
		out := append([]*platform.Dashboard(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// DashboardFields will include the IDGenerator, and dashboards
type DashboardFields struct {
	IDGenerator platform.IDGenerator
	Dashboards  []*platform.Dashboard
	Views       []*platform.View
}

// DashboardService tests all the service functions.
func DashboardService(
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// CreateDashboard testing
func CreateDashboard(
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
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
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
					},
				},
			},
			args: args{
				dashboard: &platform.Dashboard{
					ID:   MustIDBase16(dashTwoID),
					Name: "dashboard2",
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
					},
					{
						ID:   MustIDBase16(dashTwoID),
						Name: "dashboard2",
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
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
					},
				},
			},
			args: args{
				dashboard: &platform.Dashboard{
					Name: "dashboard2",
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
					},
					{
						ID:   MustIDBase16(dashTwoID),
						Name: "dashboard2",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateDashboard(ctx, tt.args.dashboard)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			defer s.DeleteDashboard(ctx, tt.args.dashboard.ID)

			dashboards, _, err := s.FindDashboards(ctx, platform.DashboardFilter{})
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
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
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
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
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
					ID:     MustIDBase16(dashTwoID),
					ViewID: MustIDBase16(dashTwoID),
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDBase16(dashTwoID),
								ViewID: MustIDBase16(dashTwoID),
							},
						},
					},
				},
			},
		},
		{
			name: "add cell with no id",
			fields: DashboardFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
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
					ViewID: MustIDBase16(dashTwoID),
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDBase16(dashTwoID),
								ViewID: MustIDBase16(dashTwoID),
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.AddDashboardCell(ctx, tt.args.dashboardID, tt.args.cell, platform.AddDashboardCellOptions{})
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			defer s.DeleteDashboard(ctx, tt.args.dashboardID)

			dashboards, _, err := s.FindDashboards(ctx, platform.DashboardFilter{})
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
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
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
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
					},
					{
						ID:   MustIDBase16(dashTwoID),
						Name: "dashboard2",
					},
				},
			},
			args: args{
				id: MustIDBase16(dashTwoID),
			},
			wants: wants{
				dashboard: &platform.Dashboard{
					ID:   MustIDBase16(dashTwoID),
					Name: "dashboard2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			dashboard, err := s.FindDashboardByID(ctx, tt.args.id)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(dashboard, tt.wants.dashboard, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboard is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindDashboards testing
func FindDashboards(
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
	t *testing.T,
) {
	type args struct {
		IDs  []*platform.ID
		name string
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
						ID:   MustIDBase16(dashOneID),
						Name: "abc",
					},
					{
						ID:   MustIDBase16(dashTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "abc",
					},
					{
						ID:   MustIDBase16(dashTwoID),
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
						ID:   MustIDBase16(dashOneID),
						Name: "abc",
					},
					{
						ID:   MustIDBase16(dashTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				IDs: []*platform.ID{
					idPtr(MustIDBase16(dashTwoID)),
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashTwoID),
						Name: "xyz",
					},
				},
			},
		},
		{
			name: "find multiple dashboards by id",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "abc",
					},
					{
						ID:   MustIDBase16(dashTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				IDs: []*platform.ID{
					idPtr(MustIDBase16(dashOneID)),
					idPtr(MustIDBase16(dashTwoID)),
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "abc",
					},
					{
						ID:   MustIDBase16(dashTwoID),
						Name: "xyz",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			filter := platform.DashboardFilter{}
			if tt.args.IDs != nil {
				filter.IDs = tt.args.IDs
			}

			dashboards, _, err := s.FindDashboards(ctx, filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(dashboards, tt.wants.dashboards, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboards are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteDashboard testing
func DeleteDashboard(
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
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
						Name: "A",
						ID:   MustIDBase16(dashOneID),
					},
					{
						Name: "B",
						ID:   MustIDBase16(dashTwoID),
					},
				},
			},
			args: args{
				ID: MustIDBase16(dashOneID),
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						Name: "B",
						ID:   MustIDBase16(dashTwoID),
					},
				},
			},
		},
		{
			name: "delete dashboards using id that does not exist",
			fields: DashboardFields{
				Dashboards: []*platform.Dashboard{
					{
						Name: "A",
						ID:   MustIDBase16(dashOneID),
					},
					{
						Name: "B",
						ID:   MustIDBase16(dashTwoID),
					},
				},
			},
			args: args{
				ID: MustIDBase16(dashThreeID),
			},
			wants: wants{
				err: fmt.Errorf("dashboard not found"),
				dashboards: []*platform.Dashboard{
					{
						Name: "A",
						ID:   MustIDBase16(dashOneID),
					},
					{
						Name: "B",
						ID:   MustIDBase16(dashTwoID),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteDashboard(ctx, tt.args.ID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			filter := platform.DashboardFilter{}
			dashboards, _, err := s.FindDashboards(ctx, filter)
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
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
	t *testing.T,
) {
	type args struct {
		name      string
		id        platform.ID
		retention int
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
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
					},
					{
						ID:   MustIDBase16(dashTwoID),
						Name: "dashboard2",
					},
				},
			},
			args: args{
				id:   MustIDBase16(dashOneID),
				name: "changed",
			},
			wants: wants{
				dashboard: &platform.Dashboard{
					ID:   MustIDBase16(dashOneID),
					Name: "changed",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			upd := platform.DashboardUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}

			dashboard, err := s.UpdateDashboard(ctx, tt.args.id, upd)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(dashboard, tt.wants.dashboard, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboard is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// RemoveDashboardCell testing
func RemoveDashboardCell(
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
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
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDBase16(dashTwoID),
								ViewID: MustIDBase16(dashTwoID),
							},
							{
								ID:     MustIDBase16(dashOneID),
								ViewID: MustIDBase16(dashOneID),
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
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDBase16(dashOneID),
								ViewID: MustIDBase16(dashOneID),
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.RemoveDashboardCell(ctx, tt.args.dashboardID, tt.args.cellID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			defer s.DeleteDashboard(ctx, tt.args.dashboardID)

			dashboards, _, err := s.FindDashboards(ctx, platform.DashboardFilter{})
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
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
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
			name: "basic remove cell",
			fields: DashboardFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDBase16(dashTwoID),
								ViewID: MustIDBase16(dashTwoID),
							},
							{
								ID:     MustIDBase16(dashOneID),
								ViewID: MustIDBase16(dashOneID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(dashOneID),
				cellID:      MustIDBase16(dashTwoID),
				cellUpdate: platform.CellUpdate{
					X:      func(i int32) *int32 { return &i }(int32(10)),
					ViewID: MustIDBase16(dashTwoID),
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDBase16(dashTwoID),
								ViewID: MustIDBase16(dashTwoID),
								X:      10,
							},
							{
								ID:     MustIDBase16(dashOneID),
								ViewID: MustIDBase16(dashOneID),
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			_, err := s.UpdateDashboardCell(ctx, tt.args.dashboardID, tt.args.cellID, tt.args.cellUpdate)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			defer s.DeleteDashboard(ctx, tt.args.dashboardID)

			dashboards, _, err := s.FindDashboards(ctx, platform.DashboardFilter{})
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
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
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
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDBase16(dashTwoID),
								ViewID: MustIDBase16(dashTwoID),
							},
							{
								ID:     MustIDBase16(dashOneID),
								ViewID: MustIDBase16(dashOneID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(dashOneID),
				cells: []*platform.Cell{
					{
						ID:     MustIDBase16(dashTwoID),
						ViewID: MustIDBase16(dashTwoID),
						X:      10,
					},
					{
						ID:     MustIDBase16(dashOneID),
						ViewID: MustIDBase16(dashOneID),
						Y:      11,
					},
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDBase16(dashTwoID),
								ViewID: MustIDBase16(dashTwoID),
								X:      10,
							},
							{
								ID:     MustIDBase16(dashOneID),
								ViewID: MustIDBase16(dashOneID),
								Y:      11,
							},
						},
					},
				},
			},
		},
		{
			name: "try to add a cell that didn't previously exist",
			fields: DashboardFields{
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
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDBase16(dashTwoID),
								ViewID: MustIDBase16(dashTwoID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(dashOneID),
				cells: []*platform.Cell{
					{
						ID:     MustIDBase16(dashTwoID),
						ViewID: MustIDBase16(dashTwoID),
						X:      10,
					},
					{
						ID:     MustIDBase16(dashOneID),
						ViewID: MustIDBase16(dashOneID),
						Y:      11,
					},
				},
			},
			wants: wants{
				err: fmt.Errorf("cannot replace cells that were not already present"),
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDBase16(dashTwoID),
								ViewID: MustIDBase16(dashTwoID),
							},
						},
					},
				},
			},
		},
		{
			name: "try to update a view during a replace",
			fields: DashboardFields{
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
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDBase16(dashTwoID),
								ViewID: MustIDBase16(dashTwoID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDBase16(dashOneID),
				cells: []*platform.Cell{
					{
						ID:     MustIDBase16(dashTwoID),
						ViewID: MustIDBase16(dashOneID),
						X:      10,
					},
				},
			},
			wants: wants{
				err: fmt.Errorf("cannot update view id in replace"),
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDBase16(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDBase16(dashTwoID),
								ViewID: MustIDBase16(dashTwoID),
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.ReplaceDashboardCells(ctx, tt.args.dashboardID, tt.args.cells)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			defer s.DeleteDashboard(ctx, tt.args.dashboardID)

			dashboards, _, err := s.FindDashboards(ctx, platform.DashboardFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve dashboards: %v", err)
			}
			if diff := cmp.Diff(dashboards, tt.wants.dashboards, dashboardCmpOptions...); diff != "" {
				t.Errorf("dashboards are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
