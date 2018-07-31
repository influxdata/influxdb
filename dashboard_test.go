package platform_test

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func TestDashboardCell_MarshalJSON(t *testing.T) {
	type args struct {
		cell platform.DashboardCell
	}
	type wants struct {
		json string
	}
	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			args: args{
				cell: platform.DashboardCell{
					DashboardCellContents: platform.DashboardCellContents{
						ID:   platformtesting.MustIDFromString("f01dab1ef005ba11"),
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
				json: `
{
  "id": "f01dab1ef005ba11",
  "name": "hello",
  "x": 10,
  "y": 10,
  "w": 100,
  "h": 12,
  "visualization": {
    "type": "common",
    "query": "SELECT * FROM foo"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.MarshalIndent(tt.args.cell, "", "  ")
			if err != nil {
				t.Fatalf("error marshalling json")
			}

			eq, err := jsonEqual(string(b), tt.wants.json)
			if err != nil {
				t.Fatalf("error marshalling json")
			}
			if !eq {
				t.Errorf("JSON did not match\nexpected:%s\ngot:\n%s\n", tt.wants.json, string(b))
			}
		})
	}
}

func jsonEqual(s1, s2 string) (eq bool, err error) {
	var o1, o2 interface{}

	if err = json.Unmarshal([]byte(s1), &o1); err != nil {
		return
	}
	if err = json.Unmarshal([]byte(s2), &o2); err != nil {
		return
	}

	return cmp.Equal(o1, o2), nil
}
