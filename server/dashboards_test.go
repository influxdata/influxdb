package server

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
)

func TestCorrectWidthHeight(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cell chronograf.DashboardCell
		want chronograf.DashboardCell
	}{
		{
			name: "updates width",
			cell: chronograf.DashboardCell{
				W: 0,
				H: 4,
			},
			want: chronograf.DashboardCell{
				W: 4,
				H: 4,
			},
		},
		{
			name: "updates height",
			cell: chronograf.DashboardCell{
				W: 4,
				H: 0,
			},
			want: chronograf.DashboardCell{
				W: 4,
				H: 4,
			},
		},
		{
			name: "updates both",
			cell: chronograf.DashboardCell{
				W: 0,
				H: 0,
			},
			want: chronograf.DashboardCell{
				W: 4,
				H: 4,
			},
		},
		{
			name: "updates neither",
			cell: chronograf.DashboardCell{
				W: 4,
				H: 4,
			},
			want: chronograf.DashboardCell{
				W: 4,
				H: 4,
			},
		},
	}
	for _, tt := range tests {
		if CorrectWidthHeight(&tt.cell); !reflect.DeepEqual(tt.cell, tt.want) {
			t.Errorf("%q. CorrectWidthHeight() = %v, want %v", tt.name, tt.cell, tt.want)
		}
	}
}

func TestDashboardDefaults(t *testing.T) {
	tests := []struct {
		name string
		d    chronograf.Dashboard
		want chronograf.Dashboard
	}{
		{
			name: "Updates all cell widths/heights",
			d: chronograf.Dashboard{
				Cells: []chronograf.DashboardCell{
					{
						W: 0,
						H: 0,
					},
					{
						W: 2,
						H: 2,
					},
				},
			},
			want: chronograf.Dashboard{
				Cells: []chronograf.DashboardCell{
					{
						W: 4,
						H: 4,
					},
					{
						W: 2,
						H: 2,
					},
				},
			},
		},
		{
			name: "Updates no cell",
			d: chronograf.Dashboard{
				Cells: []chronograf.DashboardCell{
					{
						W: 4,
						H: 4,
					}, {
						W: 2,
						H: 2,
					},
				},
			},
			want: chronograf.Dashboard{
				Cells: []chronograf.DashboardCell{
					{
						W: 4,
						H: 4,
					},
					{
						W: 2,
						H: 2,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		if actual := DashboardDefaults(tt.d); !reflect.DeepEqual(actual, tt.want) {
			t.Errorf("%q. DashboardDefaults() = %v, want %v", tt.name, tt.d, tt.want)
		}
	}
}

func TestValidDashboardRequest(t *testing.T) {
	tests := []struct {
		name    string
		d       chronograf.Dashboard
		want    chronograf.Dashboard
		wantErr bool
	}{
		{
			name: "Updates all cell widths/heights",
			d: chronograf.Dashboard{
				Cells: []chronograf.DashboardCell{
					{
						W: 0,
						H: 0,
						Queries: []chronograf.DashboardQuery{
							{
								Command: "SELECT donors from hill_valley_preservation_society where time > 1985-10-25T08:00:00",
							},
						},
					},
					{
						W: 2,
						H: 2,
						Queries: []chronograf.DashboardQuery{
							{
								Command: "SELECT winning_horses from grays_sports_alamanc where time > 1955-11-1T00:00:00",
							},
						},
					},
				},
			},
			want: chronograf.Dashboard{
				Cells: []chronograf.DashboardCell{
					{
						W: 4,
						H: 4,
						Queries: []chronograf.DashboardQuery{
							{
								Command: "SELECT donors from hill_valley_preservation_society where time > 1985-10-25T08:00:00",
							},
						},
					},
					{
						W: 2,
						H: 2,
						Queries: []chronograf.DashboardQuery{
							{
								Command: "SELECT winning_horses from grays_sports_alamanc where time > 1955-11-1T00:00:00",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		err := ValidDashboardRequest(&tt.d)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. ValidDashboardRequest() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(tt.d, tt.want) {
			t.Errorf("%q. ValidDashboardRequest() = %v, want %v", tt.name, tt.d, tt.want)
		}
	}
}

func Test_newDashboardResponse(t *testing.T) {
	tests := []struct {
		name string
		d    chronograf.Dashboard
		want *dashboardResponse
	}{
		{
			name: "creates a dashboard response",
			d: chronograf.Dashboard{
				Cells: []chronograf.DashboardCell{
					{
						ID: "a",
						W:  0,
						H:  0,
						Queries: []chronograf.DashboardQuery{
							{
								Command: "SELECT donors from hill_valley_preservation_society where time > '1985-10-25 08:00:00'",
							},
						},
						Axes: map[string]chronograf.Axis{
							"x": chronograf.Axis{
								Bounds: []string{"0", "100"},
							},
							"y": chronograf.Axis{
								Bounds: []string{"2", "95"},
							},
						},
					},
					{
						ID: "b",
						W:  0,
						H:  0,
						Queries: []chronograf.DashboardQuery{
							{
								Command: "SELECT winning_horses from grays_sports_alamanc where time > now() - 15m",
							},
						},
					},
				},
			},
			want: &dashboardResponse{
				Templates: []templateResponse{},
				Cells: []dashboardCellResponse{
					dashboardCellResponse{
						Links: dashboardCellLinks{
							Self: "/chronograf/v1/dashboards/0/cells/a",
						},
						DashboardCell: chronograf.DashboardCell{
							ID: "a",
							W:  4,
							H:  4,
							Queries: []chronograf.DashboardQuery{
								{
									Command: "SELECT donors from hill_valley_preservation_society where time > '1985-10-25 08:00:00'",
									QueryConfig: chronograf.QueryConfig{
										RawText: &[]string{"SELECT donors from hill_valley_preservation_society where time > '1985-10-25 08:00:00'"}[0],
										Fields:  []chronograf.Field{},
										GroupBy: chronograf.GroupBy{
											Tags: []string{},
										},
										Tags:            make(map[string][]string, 0),
										AreTagsAccepted: false,
									},
								},
							},
							Axes: map[string]chronograf.Axis{
								"x": chronograf.Axis{
									Bounds: []string{"0", "100"},
								},
								"y": chronograf.Axis{
									Bounds: []string{"2", "95"},
								},
								"y2": chronograf.Axis{
									Bounds: []string{},
								},
							},
						},
					},
					dashboardCellResponse{
						Links: dashboardCellLinks{
							Self: "/chronograf/v1/dashboards/0/cells/b",
						},
						DashboardCell: chronograf.DashboardCell{
							ID: "b",
							W:  4,
							H:  4,
							Axes: map[string]chronograf.Axis{
								"x": chronograf.Axis{
									Bounds: []string{},
								},
								"y": chronograf.Axis{
									Bounds: []string{},
								},
								"y2": chronograf.Axis{
									Bounds: []string{},
								},
							},
							Queries: []chronograf.DashboardQuery{
								{
									Command: "SELECT winning_horses from grays_sports_alamanc where time > now() - 15m",
									QueryConfig: chronograf.QueryConfig{
										Measurement: "grays_sports_alamanc",
										Fields: []chronograf.Field{
											{
												Field: "winning_horses",
												Funcs: []string{},
											},
										},
										GroupBy: chronograf.GroupBy{
											Tags: []string{},
										},
										Tags:            make(map[string][]string, 0),
										AreTagsAccepted: false,
										Range: &chronograf.DurationRange{
											Lower: "now() - 15m",
										},
									},
								},
							},
						},
					},
				},
				Links: dashboardLinks{
					Self:      "/chronograf/v1/dashboards/0",
					Cells:     "/chronograf/v1/dashboards/0/cells",
					Templates: "/chronograf/v1/dashboards/0/templates",
				},
			},
		},
	}
	for _, tt := range tests {
		if got := newDashboardResponse(tt.d); !cmp.Equal(got, tt.want) {
			t.Errorf("%q. newDashboardResponse() = diff:\n%s", tt.name, cmp.Diff(got, tt.want))
		}
	}
}
