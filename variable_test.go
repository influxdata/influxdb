package influxdb_test

import (
	"encoding/json"
	"reflect"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
)

var (
	variableTestID    = "debac1e0deadbeef"
	variableTestOrgID = "deadbeefdeadbeef"
)

func TestVariable_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		json string
		want platform.Variable
	}{
		{
			name: "with organization",
			json: `
{ 
  "id": "debac1e0deadbeef",
  "orgID": "deadbeefdeadbeef",
  "name": "howdy",
  "selected": [],
  "arguments": {
    "type": "constant",
    "values": ["a", "b", "c", "d"]
  }
}
`,
			want: platform.Variable{
				ID:             platformtesting.MustIDBase16(variableTestID),
				OrganizationID: platformtesting.MustIDBase16(variableTestOrgID),
				Name:           "howdy",
				Selected:       make([]string, 0),
				Arguments: &platform.VariableArguments{
					Type:   "constant",
					Values: platform.VariableConstantValues{"a", "b", "c", "d"},
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
			want: platform.Variable{
				ID:       platformtesting.MustIDBase16(variableTestID),
				Name:     "howdy",
				Selected: make([]string, 0),
				Arguments: &platform.VariableArguments{
					Type:   "constant",
					Values: platform.VariableConstantValues{"a", "b", "c"},
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
			want: platform.Variable{
				ID:       platformtesting.MustIDBase16(variableTestID),
				Name:     "howdy",
				Selected: make([]string, 0),
				Arguments: &platform.VariableArguments{
					Type:   "map",
					Values: platform.VariableMapValues{"a": "A", "b": "B"},
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
			want: platform.Variable{
				ID:       platformtesting.MustIDBase16(variableTestID),
				Name:     "howdy",
				Selected: make([]string, 0),
				Arguments: &platform.VariableArguments{
					Type: "query",
					Values: platform.VariableQueryValues{
						Query:    "howdy",
						Language: "flux",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m platform.Variable

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
