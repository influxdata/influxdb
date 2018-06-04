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
	IDGenerator   platform.IDGenerator
	Dashboards    []*platform.Dashboard
	Organizations []*platform.Organization
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
			name: "create dashboards with empty set",
			fields: DashboardFields{
				IDGenerator: mock.NewIDGenerator("id1"),
				Dashboards:  []*platform.Dashboard{},
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
				},
			},
			args: args{
				dashboard: &platform.Dashboard{
					Name:           "name1",
					OrganizationID: platform.ID("org1"),
					Cells: []platform.DashboardCell{
						{
							DashboardCellContents: platform.DashboardCellContents{
								Name: "hello",
								X:    10,
								Y:    10,
								W:    100,
								H:    12,
							},
							Visualization: platform.CommonVisualization{
								Query: "SELECT * FROM foo",
							},
						},
					},
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						Name:           "name1",
						ID:             platform.ID("id1"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Cells: []platform.DashboardCell{
							{
								DashboardCellContents: platform.DashboardCellContents{
									ID:   platform.ID("id1"),
									Name: "hello",
									X:    10,
									Y:    10,
									W:    100,
									H:    12,
								},
								Visualization: platform.CommonVisualization{
									Query: "SELECT * FROM foo",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "basic create dashboard",
			fields: DashboardFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return platform.ID("2")
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("1"),
						Name:           "dashboard1",
						OrganizationID: platform.ID("org1"),
					},
				},
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
					{
						Name: "otherorg",
						ID:   platform.ID("org2"),
					},
				},
			},
			args: args{
				dashboard: &platform.Dashboard{
					Name:           "dashboard2",
					OrganizationID: platform.ID("org2"),
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("1"),
						Name:           "dashboard1",
						Organization:   "theorg",
						OrganizationID: platform.ID("org1"),
					},
					{
						ID:             platform.ID("2"),
						Name:           "dashboard2",
						Organization:   "otherorg",
						OrganizationID: platform.ID("org2"),
					},
				},
			},
		},
		{
			name: "basic create dashboard using org name",
			fields: DashboardFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return platform.ID("2")
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("1"),
						Name:           "dashboard1",
						OrganizationID: platform.ID("org1"),
					},
				},
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
					{
						Name: "otherorg",
						ID:   platform.ID("org2"),
					},
				},
			},
			args: args{
				dashboard: &platform.Dashboard{
					Name:         "dashboard2",
					Organization: "otherorg",
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("1"),
						Name:           "dashboard1",
						Organization:   "theorg",
						OrganizationID: platform.ID("org1"),
					},
					{
						ID:             platform.ID("2"),
						Name:           "dashboard2",
						Organization:   "otherorg",
						OrganizationID: platform.ID("org2"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
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
						ID:             platform.ID("1"),
						OrganizationID: platform.ID("org1"),
						Name:           "dashboard1",
					},
					{
						ID:             platform.ID("2"),
						OrganizationID: platform.ID("org1"),
						Name:           "dashboard2",
					},
				},
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
				},
			},
			args: args{
				id: platform.ID("2"),
			},
			wants: wants{
				dashboard: &platform.Dashboard{
					ID:             platform.ID("2"),
					OrganizationID: platform.ID("org1"),
					Organization:   "theorg",
					Name:           "dashboard2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

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

// FindDashboardsByOrganiztionName tests.
func FindDashboardsByOrganizationName(
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
	t *testing.T,
) {
	type args struct {
		organization string
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
			name: "find dashboards by organization name",
			fields: DashboardFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
					{
						Name: "otherorg",
						ID:   platform.ID("org2"),
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
					},
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org2"),
						Name:           "xyz",
					},
					{
						ID:             platform.ID("test3"),
						OrganizationID: platform.ID("org1"),
						Name:           "123",
					},
				},
			},
			args: args{
				organization: "theorg",
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "abc",
					},
					{
						ID:             platform.ID("test3"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "123",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			dashboards, _, err := s.FindDashboardsByOrganizationName(ctx, tt.args.organization)
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

// FindDashboardsByOrganiztionID tests.
func FindDashboardsByOrganizationID(
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
	t *testing.T,
) {
	type args struct {
		organizationID string
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
			name: "find dashboards by organization id",
			fields: DashboardFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
					{
						Name: "otherorg",
						ID:   platform.ID("org2"),
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
					},
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org2"),
						Name:           "xyz",
					},
					{
						ID:             platform.ID("test3"),
						OrganizationID: platform.ID("org1"),
						Name:           "123",
					},
				},
			},
			args: args{
				organizationID: "org1",
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "abc",
					},
					{
						ID:             platform.ID("test3"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "123",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			id := platform.ID(tt.args.organizationID)

			dashboards, _, err := s.FindDashboardsByOrganizationID(ctx, id)
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

// FindDashboards testing
func FindDashboards(
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
	t *testing.T,
) {
	type args struct {
		ID             string
		name           string
		organization   string
		organizationID string
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
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
					{
						Name: "otherorg",
						ID:   platform.ID("org2"),
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
					},
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org2"),
						Name:           "xyz",
					},
				},
			},
			args: args{},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "abc",
					},
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org2"),
						Organization:   "otherorg",
						Name:           "xyz",
					},
				},
			},
		},
		{
			name: "find dashboards by organization name",
			fields: DashboardFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
					{
						Name: "otherorg",
						ID:   platform.ID("org2"),
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
					},
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org2"),
						Name:           "xyz",
					},
					{
						ID:             platform.ID("test3"),
						OrganizationID: platform.ID("org1"),
						Name:           "123",
					},
				},
			},
			args: args{
				organization: "theorg",
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "abc",
					},
					{
						ID:             platform.ID("test3"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "123",
					},
				},
			},
		},
		{
			name: "find dashboards by organization id",
			fields: DashboardFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
					{
						Name: "otherorg",
						ID:   platform.ID("org2"),
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
					},
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org2"),
						Name:           "xyz",
					},
					{
						ID:             platform.ID("test3"),
						OrganizationID: platform.ID("org1"),
						Name:           "123",
					},
				},
			},
			args: args{
				organizationID: "org1",
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "abc",
					},
					{
						ID:             platform.ID("test3"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "123",
					},
				},
			},
		},
		{
			name: "find dashboard by id",
			fields: DashboardFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
					},
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org1"),
						Name:           "xyz",
					},
				},
			},
			args: args{
				ID: "test2",
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "xyz",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			filter := platform.DashboardFilter{}
			if tt.args.ID != "" {
				id := platform.ID(tt.args.ID)
				filter.ID = &id
			}
			if tt.args.organizationID != "" {
				id := platform.ID(tt.args.organizationID)
				filter.OrganizationID = &id
			}
			if tt.args.organization != "" {
				filter.Organization = &tt.args.organization
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
		ID string
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
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						Name:           "A",
						ID:             platform.ID("abc"),
						OrganizationID: platform.ID("org1"),
					},
					{
						Name:           "B",
						ID:             platform.ID("xyz"),
						OrganizationID: platform.ID("org1"),
					},
				},
			},
			args: args{
				ID: "abc",
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						Name:           "B",
						ID:             platform.ID("xyz"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
					},
				},
			},
		},
		{
			name: "delete dashboards using id that does not exist",
			fields: DashboardFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						Name:           "A",
						ID:             platform.ID("abc"),
						OrganizationID: platform.ID("org1"),
					},
					{
						Name:           "B",
						ID:             platform.ID("xyz"),
						OrganizationID: platform.ID("org1"),
					},
				},
			},
			args: args{
				ID: "123",
			},
			wants: wants{
				err: fmt.Errorf("dashboard not found"),
				dashboards: []*platform.Dashboard{
					{
						Name:           "A",
						ID:             platform.ID("abc"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
					},
					{
						Name:           "B",
						ID:             platform.ID("xyz"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			err := s.DeleteDashboard(ctx, platform.ID(tt.args.ID))
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
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("1"),
						OrganizationID: platform.ID("org1"),
						Name:           "dashboard1",
					},
					{
						ID:             platform.ID("2"),
						OrganizationID: platform.ID("org1"),
						Name:           "dashboard2",
					},
				},
			},
			args: args{
				id:   platform.ID("1"),
				name: "changed",
			},
			wants: wants{
				dashboard: &platform.Dashboard{
					ID:             platform.ID("1"),
					OrganizationID: platform.ID("org1"),
					Organization:   "theorg",
					Name:           "changed",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

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

// AddDashboardCell tests.
func AddDashboardCell(
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
	t *testing.T,
) {
	type args struct {
		dashboardID platform.ID
		cell        *platform.DashboardCell
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
			name: "add dashboard cell",
			fields: DashboardFields{
				IDGenerator: mock.NewIDGenerator("id1"),
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
					},
				},
			},
			args: args{
				dashboardID: platform.ID("test1"),
				cell: &platform.DashboardCell{
					DashboardCellContents: platform.DashboardCellContents{
						Name: "hello",
						X:    10,
						Y:    10,
						W:    100,
						H:    12,
					},
					Visualization: platform.CommonVisualization{
						Query: "SELECT * FROM foo",
					},
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "abc",
						Cells: []platform.DashboardCell{
							{
								DashboardCellContents: platform.DashboardCellContents{
									ID:   platform.ID("id1"),
									Name: "hello",
									X:    10,
									Y:    10,
									W:    100,
									H:    12,
								},
								Visualization: platform.CommonVisualization{
									Query: "SELECT * FROM foo",
								},
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
			ctx := context.TODO()
			err := s.AddDashboardCell(ctx, tt.args.dashboardID, tt.args.cell)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

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

// RemoveDashboardCell tests.
func RemoveDashboardCell(
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
	t *testing.T,
) {
	type args struct {
		dashboardID platform.ID
		cellID      platform.ID
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
			name: "remove dashboard cell",
			fields: DashboardFields{
				IDGenerator: mock.NewIDGenerator("id1"),
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
						Cells: []platform.DashboardCell{
							{
								DashboardCellContents: platform.DashboardCellContents{
									ID:   platform.ID("id1"),
									Name: "hello",
									X:    10,
									Y:    10,
									W:    100,
									H:    12,
								},
								Visualization: platform.CommonVisualization{
									Query: "SELECT * FROM foo",
								},
							},
							{
								DashboardCellContents: platform.DashboardCellContents{
									ID:   platform.ID("id2"),
									Name: "world",
									X:    10,
									Y:    10,
									W:    100,
									H:    12,
								},
								Visualization: platform.CommonVisualization{
									Query: "SELECT * FROM mem",
								},
							},
							{
								DashboardCellContents: platform.DashboardCellContents{
									ID:   platform.ID("id3"),
									Name: "bar",
									X:    10,
									Y:    10,
									W:    101,
									H:    12,
								},
								Visualization: platform.CommonVisualization{
									Query: "SELECT thing FROM foo",
								},
							},
						},
					},
				},
			},
			args: args{
				dashboardID: platform.ID("test1"),
				cellID:      platform.ID("id2"),
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "abc",
						Cells: []platform.DashboardCell{
							{
								DashboardCellContents: platform.DashboardCellContents{
									ID:   platform.ID("id1"),
									Name: "hello",
									X:    10,
									Y:    10,
									W:    100,
									H:    12,
								},
								Visualization: platform.CommonVisualization{
									Query: "SELECT * FROM foo",
								},
							},
							{
								DashboardCellContents: platform.DashboardCellContents{
									ID:   platform.ID("id3"),
									Name: "bar",
									X:    10,
									Y:    10,
									W:    101,
									H:    12,
								},
								Visualization: platform.CommonVisualization{
									Query: "SELECT thing FROM foo",
								},
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
			ctx := context.TODO()
			err := s.RemoveDashboardCell(ctx, tt.args.dashboardID, tt.args.cellID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

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

// ReplaceDashboardCell tests.
func ReplaceDashboardCell(
	init func(DashboardFields, *testing.T) (platform.DashboardService, func()),
	t *testing.T,
) {
	type args struct {
		dashboardID platform.ID
		cell        *platform.DashboardCell
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
			name: "add dashboard cell",
			fields: DashboardFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   platform.ID("org1"),
					},
				},
				Dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
						Cells: []platform.DashboardCell{
							{
								DashboardCellContents: platform.DashboardCellContents{
									ID:   platform.ID("id1"),
									Name: "hello",
									X:    10,
									Y:    10,
									W:    100,
									H:    12,
								},
								Visualization: platform.CommonVisualization{
									Query: "SELECT * FROM foo",
								},
							},
						},
					},
				},
			},
			args: args{
				dashboardID: platform.ID("test1"),
				cell: &platform.DashboardCell{
					DashboardCellContents: platform.DashboardCellContents{
						ID:   platform.ID("id1"),
						Name: "what",
						X:    101,
						Y:    102,
						W:    100,
						H:    12,
					},
					Visualization: platform.CommonVisualization{
						Query: "SELECT * FROM thing",
					},
				},
			},
			wants: wants{
				dashboards: []*platform.Dashboard{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Organization:   "theorg",
						Name:           "abc",
						Cells: []platform.DashboardCell{
							{
								DashboardCellContents: platform.DashboardCellContents{
									ID:   platform.ID("id1"),
									Name: "what",
									X:    101,
									Y:    102,
									W:    100,
									H:    12,
								},
								Visualization: platform.CommonVisualization{
									Query: "SELECT * FROM thing",
								},
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
			ctx := context.TODO()
			err := s.ReplaceDashboardCell(ctx, tt.args.dashboardID, tt.args.cell)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

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
