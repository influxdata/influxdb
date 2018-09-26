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
						return MustIDFromString(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
					},
				},
			},
			args: args{
				dashboard: &platform.Dashboard{
					ID:   MustIDFromString(dashTwoID),
					Name: "dashboard2",
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
					},
					{
						ID:   MustIDFromString(dashTwoID),
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
						return MustIDFromString(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
					},
				},
			},
			args: args{
				dashboardID: MustIDFromString(dashOneID),
				cell:        &platform.Cell{},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDFromString(dashTwoID),
								ViewID: MustIDFromString(dashTwoID),
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
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
					},
					{
						ID:   MustIDFromString(dashTwoID),
						Name: "dashboard2",
					},
				},
			},
			args: args{
				id: MustIDFromString(dashTwoID),
			},
			wants: wants{
				dashboard: &platform.Dashboard{
					ID:   MustIDFromString(dashTwoID),
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
						ID:   MustIDFromString(dashOneID),
						Name: "abc",
					},
					{
						ID:   MustIDFromString(dashTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "abc",
					},
					{
						ID:   MustIDFromString(dashTwoID),
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
						ID:   MustIDFromString(dashOneID),
						Name: "abc",
					},
					{
						ID:   MustIDFromString(dashTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				IDs: []*platform.ID{
					idPtr(MustIDFromString(dashTwoID)),
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashTwoID),
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
						ID:   MustIDFromString(dashOneID),
						Name: "abc",
					},
					{
						ID:   MustIDFromString(dashTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				IDs: []*platform.ID{
					idPtr(MustIDFromString(dashOneID)),
					idPtr(MustIDFromString(dashTwoID)),
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "abc",
					},
					{
						ID:   MustIDFromString(dashTwoID),
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
						ID:   MustIDFromString(dashOneID),
					},
					{
						Name: "B",
						ID:   MustIDFromString(dashTwoID),
					},
				},
			},
			args: args{
				ID: MustIDFromString(dashOneID),
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						Name: "B",
						ID:   MustIDFromString(dashTwoID),
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
						ID:   MustIDFromString(dashOneID),
					},
					{
						Name: "B",
						ID:   MustIDFromString(dashTwoID),
					},
				},
			},
			args: args{
				ID: MustIDFromString(dashThreeID),
			},
			wants: wants{
				err: fmt.Errorf("dashboard not found"),
				dashboards: []*platform.Dashboard{
					{
						Name: "A",
						ID:   MustIDFromString(dashOneID),
					},
					{
						Name: "B",
						ID:   MustIDFromString(dashTwoID),
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
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
					},
					{
						ID:   MustIDFromString(dashTwoID),
						Name: "dashboard2",
					},
				},
			},
			args: args{
				id:   MustIDFromString(dashOneID),
				name: "changed",
			},
			wants: wants{
				dashboard: &platform.Dashboard{
					ID:   MustIDFromString(dashOneID),
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
						return MustIDFromString(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDFromString(dashTwoID),
								ViewID: MustIDFromString(dashTwoID),
							},
							{
								ID:     MustIDFromString(dashOneID),
								ViewID: MustIDFromString(dashOneID),
							},
						},
					},
				},
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID: MustIDFromString(dashTwoID),
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDFromString(dashOneID),
				cellID:      MustIDFromString(dashTwoID),
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDFromString(dashOneID),
								ViewID: MustIDFromString(dashOneID),
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
		x           int32
		y           int32
		w           int32
		h           int32
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
						return MustIDFromString(dashTwoID)
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDFromString(dashTwoID),
								ViewID: MustIDFromString(dashTwoID),
							},
							{
								ID:     MustIDFromString(dashOneID),
								ViewID: MustIDFromString(dashOneID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDFromString(dashOneID),
				cellID:      MustIDFromString(dashTwoID),
				x:           10,
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDFromString(dashTwoID),
								ViewID: MustIDFromString(dashTwoID),
								X:      10,
							},
							{
								ID:     MustIDFromString(dashOneID),
								ViewID: MustIDFromString(dashOneID),
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
			upd := platform.CellUpdate{}
			if tt.args.x != 0 {
				upd.X = &tt.args.x
			}
			if tt.args.y != 0 {
				upd.Y = &tt.args.y
			}
			if tt.args.w != 0 {
				upd.W = &tt.args.w
			}
			if tt.args.h != 0 {
				upd.H = &tt.args.h
			}
			_, err := s.UpdateDashboardCell(ctx, tt.args.dashboardID, tt.args.cellID, upd)
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
						return MustIDFromString(dashTwoID)
					},
				},
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID: MustIDFromString(dashTwoID),
						},
					},
					{
						ViewContents: platform.ViewContents{
							ID: MustIDFromString(dashOneID),
						},
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDFromString(dashTwoID),
								ViewID: MustIDFromString(dashTwoID),
							},
							{
								ID:     MustIDFromString(dashOneID),
								ViewID: MustIDFromString(dashOneID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDFromString(dashOneID),
				cells: []*platform.Cell{
					{
						ID:     MustIDFromString(dashTwoID),
						ViewID: MustIDFromString(dashTwoID),
						X:      10,
					},
					{
						ID:     MustIDFromString(dashOneID),
						ViewID: MustIDFromString(dashOneID),
						Y:      11,
					},
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDFromString(dashTwoID),
								ViewID: MustIDFromString(dashTwoID),
								X:      10,
							},
							{
								ID:     MustIDFromString(dashOneID),
								ViewID: MustIDFromString(dashOneID),
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
						return MustIDFromString(dashTwoID)
					},
				},
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID: MustIDFromString(dashTwoID),
						},
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDFromString(dashTwoID),
								ViewID: MustIDFromString(dashTwoID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDFromString(dashOneID),
				cells: []*platform.Cell{
					{
						ID:     MustIDFromString(dashTwoID),
						ViewID: MustIDFromString(dashTwoID),
						X:      10,
					},
					{
						ID:     MustIDFromString(dashOneID),
						ViewID: MustIDFromString(dashOneID),
						Y:      11,
					},
				},
			},
			wants: wants{
				err: fmt.Errorf("cannot replace cells that were not already present"),
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDFromString(dashTwoID),
								ViewID: MustIDFromString(dashTwoID),
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
						return MustIDFromString(dashTwoID)
					},
				},
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID: MustIDFromString(dashTwoID),
						},
					},
					{
						ViewContents: platform.ViewContents{
							ID: MustIDFromString(dashOneID),
						},
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDFromString(dashTwoID),
								ViewID: MustIDFromString(dashTwoID),
							},
						},
					},
				},
			},
			args: args{
				dashboardID: MustIDFromString(dashOneID),
				cells: []*platform.Cell{
					{
						ID:     MustIDFromString(dashTwoID),
						ViewID: MustIDFromString(dashOneID),
						X:      10,
					},
				},
			},
			wants: wants{
				err: fmt.Errorf("cannot update view id in replace"),
				dashboards: []*platform.Dashboard{
					{
						ID:   MustIDFromString(dashOneID),
						Name: "dashboard1",
						Cells: []*platform.Cell{
							{
								ID:     MustIDFromString(dashTwoID),
								ViewID: MustIDFromString(dashTwoID),
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
