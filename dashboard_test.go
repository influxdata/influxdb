package influxdb_test

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func TestView_MarshalJSON(t *testing.T) {
	type args struct {
		view platform.View
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
				view: platform.View{
					ViewContents: platform.ViewContents{
						ID:   platformtesting.MustIDBase16("f01dab1ef005ba11"),
						Name: "hello",
					},
					Properties: platform.XYViewProperties{
						Type: "xy",
					},
				},
			},
			wants: wants{
				json: `
{
  "id": "f01dab1ef005ba11",
  "name": "hello",
  "properties": {
    "shape": "chronograf-v2",
    "queries": null,
    "axes": null,
    "type": "xy",
    "colors": null,
    "legend": {},
	"geom": "",
    "note": "",
    "showNoteWhenEmpty": false
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.MarshalIndent(tt.args.view, "", "  ")
			if err != nil {
				t.Fatalf("error marshalling json")
			}

			eq, err := jsonEqual(string(b), tt.wants.json)
			if err != nil {
				t.Fatalf("error marshalling json %v", err)
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
