package influxdb_test

import (
	"encoding/json"
	"reflect"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

var (
	macroTestID    = "debac1e0deadbeef"
	macroTestOrgID = "deadbeefdeadbeef"
)

func TestMacro_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		json string
		want platform.Macro
	}{
		{
			name: "with organization",
			json: `
{ 
  "id": "debac1e0deadbeef",
  "org_id": "deadbeefdeadbeef",
  "name": "howdy",
  "selected": [],
  "arguments": {
    "type": "constant",
    "values": ["a", "b", "c", "d"]
  }
}
`,
			want: platform.Macro{
				ID:             platformtesting.MustIDBase16(macroTestID),
				OrganizationID: platformtesting.MustIDBase16(macroTestOrgID),
				Name:           "howdy",
				Selected:       make([]string, 0),
				Arguments: &platform.MacroArguments{
					Type:   "constant",
					Values: platform.MacroConstantValues{"a", "b", "c", "d"},
				},
			},
		},
		{
			name: "with constant arguments",
			json: `
{ 
  "id": "debac1e0deadbeef",
  "name": "howdy",
  "selected": [],
  "arguments": {
    "type": "constant",
    "values": ["a", "b", "c"]
  }
}
`,
			want: platform.Macro{
				ID:       platformtesting.MustIDBase16(macroTestID),
				Name:     "howdy",
				Selected: make([]string, 0),
				Arguments: &platform.MacroArguments{
					Type:   "constant",
					Values: platform.MacroConstantValues{"a", "b", "c"},
				},
			},
		},
		{
			name: "with map arguments",
			json: `
{ 
  "id": "debac1e0deadbeef",
  "name": "howdy",
  "selected": [],
  "arguments": {
    "type": "map",
    "values": {
      "a": "A",
      "b": "B"
    }
  }
}
`,
			want: platform.Macro{
				ID:       platformtesting.MustIDBase16(macroTestID),
				Name:     "howdy",
				Selected: make([]string, 0),
				Arguments: &platform.MacroArguments{
					Type:   "map",
					Values: platform.MacroMapValues{"a": "A", "b": "B"},
				},
			},
		},
		{
			name: "with query arguments",
			json: `
{ 
  "id": "debac1e0deadbeef",
  "name": "howdy",
  "selected": [],
  "arguments": {
    "type": "query",
    "values": {
      "query": "howdy",
      "language": "flux"
    }
  }
}
`,
			want: platform.Macro{
				ID:       platformtesting.MustIDBase16(macroTestID),
				Name:     "howdy",
				Selected: make([]string, 0),
				Arguments: &platform.MacroArguments{
					Type: "query",
					Values: platform.MacroQueryValues{
						Query:    "howdy",
						Language: "flux",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m platform.Macro

			err := json.Unmarshal([]byte(tt.json), &m)
			if err != nil {
				t.Fatalf("error unmarshalling json: %v", err)
			}

			if !reflect.DeepEqual(m, tt.want) {
				t.Errorf("%q. got = %+v, want %+v", tt.name, m, tt.want)
			}
		})
	}
}
