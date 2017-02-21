package server

import (
	"reflect"
	"testing"

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
		if DashboardDefaults(&tt.d); !reflect.DeepEqual(tt.d, tt.want) {
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
						Queries: []chronograf.Query{
							{
								Command: "SELECT donors from hill_valley_preservation_society where time > 1985-10-25T08:00:00",
							},
						},
					},
					{
						W: 2,
						H: 2,
						Queries: []chronograf.Query{
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
						Queries: []chronograf.Query{
							{
								Command: "SELECT donors from hill_valley_preservation_society where time > 1985-10-25T08:00:00",
							},
						},
					},
					{
						W: 2,
						H: 2,
						Queries: []chronograf.Query{
							{
								Command: "SELECT winning_horses from grays_sports_alamanc where time > 1955-11-1T00:00:00",
							},
						},
					},
				},
			},
		},
		{
			name: "No queries",
			d: chronograf.Dashboard{
				Cells: []chronograf.DashboardCell{
					{
						W:       2,
						H:       2,
						Queries: []chronograf.Query{},
					},
				},
			},
			want: chronograf.Dashboard{
				Cells: []chronograf.DashboardCell{
					{
						W:       2,
						H:       2,
						Queries: []chronograf.Query{},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Empty Cells",
			d: chronograf.Dashboard{
				Cells: []chronograf.DashboardCell{},
			},
			want: chronograf.Dashboard{
				Cells: []chronograf.DashboardCell{},
			},
			wantErr: true,
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
		want dashboardResponse
	}{
		{
			name: "Updates all cell widths/heights",
			d: chronograf.Dashboard{
				Cells: []chronograf.DashboardCell{
					{
						W: 0,
						H: 0,
						Queries: []chronograf.Query{
							{
								Command: "SELECT donors from hill_valley_preservation_society where time > 1985-10-25T08:00:00",
							},
						},
					},
					{
						W: 0,
						H: 0,
						Queries: []chronograf.Query{
							{
								Command: "SELECT winning_horses from grays_sports_alamanc where time > 1955-11-1T00:00:00",
							},
						},
					},
				},
			},
			want: dashboardResponse{
				Dashboard: chronograf.Dashboard{
					Cells: []chronograf.DashboardCell{
						{
							W: 4,
							H: 4,
							Queries: []chronograf.Query{
								{
									Command: "SELECT donors from hill_valley_preservation_society where time > 1985-10-25T08:00:00",
								},
							},
						},
						{
							W: 4,
							H: 4,
							Queries: []chronograf.Query{
								{
									Command: "SELECT winning_horses from grays_sports_alamanc where time > 1955-11-1T00:00:00",
								},
							},
						},
					},
				},
				Links: dashboardLinks{
					Self: "/chronograf/v1/dashboards/0",
				},
			},
		},
	}
	for _, tt := range tests {
		if got := newDashboardResponse(tt.d); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. newDashboardResponse() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
