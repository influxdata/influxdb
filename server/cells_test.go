package server_test

import (
	"testing"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/server"
)

func Test_Cells_CorrectAxis(t *testing.T) {
	t.Parallel()

	axisTests := []struct {
		name       string
		cell       *chronograf.DashboardCell
		shouldFail bool
	}{
		{
			"correct axes",
			&chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"x": chronograf.Axis{
						Bounds: []string{"0", "100"},
					},
					"y": chronograf.Axis{
						Bounds: []string{"0", "100"},
					},
					"y2": chronograf.Axis{
						Bounds: []string{"0", "100"},
					},
				},
			},
			false,
		},
		{
			"invalid axes present",
			&chronograf.DashboardCell{
				Axes: map[string]chronograf.Axis{
					"axis of evil": chronograf.Axis{
						Bounds: []string{"666", "666"},
					},
					"axis of awesome": chronograf.Axis{
						Bounds: []string{"1337", "31337"},
					},
				},
			},
			true,
		},
	}

	for _, test := range axisTests {
		t.Run(test.name, func(tt *testing.T) {
			if err := server.HasCorrectAxes(test.cell); err != nil && !test.shouldFail {
				t.Errorf("%q: Unexpected error: err: %s", test.name, err)
			} else if err == nil && test.shouldFail {
				t.Errorf("%q: Expected error and received none", test.name)
			}
		})
	}
}
