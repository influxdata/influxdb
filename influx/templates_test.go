package influx

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/influxdata/chronograf"
)

func TestTemplateReplace(t *testing.T) {
	tests := []struct {
		name  string
		query string
		vars  chronograf.TemplateVars
		want  string
	}{
		{
			name:  "select with parameters",
			query: "$METHOD field1, $field FROM $measurement WHERE temperature > $temperature",
			vars: chronograf.TemplateVars{
				chronograf.BasicTemplateVar{
					Var: "$temperature",
					Values: []chronograf.BasicTemplateValue{
						{
							Type:  "csv",
							Value: "10",
						},
					},
				},
				chronograf.BasicTemplateVar{
					Var: "$field",
					Values: []chronograf.BasicTemplateValue{
						{
							Type:  "fieldKey",
							Value: "field2",
						},
					},
				},
				chronograf.BasicTemplateVar{
					Var: "$METHOD",
					Values: []chronograf.BasicTemplateValue{
						{
							Type:  "csv",
							Value: "SELECT",
						},
					},
				},
				chronograf.BasicTemplateVar{
					Var: "$measurement",
					Values: []chronograf.BasicTemplateValue{
						{
							Type:  "csv",
							Value: `"cpu"`,
						},
					},
				},
			},
			want: `SELECT field1, "field2" FROM "cpu" WHERE temperature > 10`,
		},
		{
			name:  "select with parameters and aggregates",
			query: `SELECT mean($field) FROM "cpu" WHERE $tag = $value GROUP BY $tag`,
			vars: chronograf.TemplateVars{
				chronograf.BasicTemplateVar{
					Var: "$value",
					Values: []chronograf.BasicTemplateValue{
						{
							Type:  "tagValue",
							Value: "howdy.com",
						},
					},
				},
				chronograf.BasicTemplateVar{
					Var: "$tag",
					Values: []chronograf.BasicTemplateValue{
						{
							Type:  "tagKey",
							Value: "host",
						},
					},
				},
				chronograf.BasicTemplateVar{
					Var: "$field",
					Values: []chronograf.BasicTemplateValue{
						{
							Type:  "fieldKey",
							Value: "field",
						},
					},
				},
			},
			want: `SELECT mean("field") FROM "cpu" WHERE "host" = 'howdy.com' GROUP BY "host"`,
		},
		{
			name:  "Non-existant parameters",
			query: `SELECT $field FROM "cpu"`,
			want:  `SELECT $field FROM "cpu"`,
		},
		{
			name:  "var without a value",
			query: `SELECT $field FROM "cpu"`,
			vars: chronograf.TemplateVars{
				chronograf.BasicTemplateVar{
					Var: "$field",
				},
			},
			want: `SELECT $field FROM "cpu"`,
		},
		{
			name:  "var with unknown type",
			query: `SELECT $field FROM "cpu"`,
			vars: chronograf.TemplateVars{
				chronograf.BasicTemplateVar{
					Var: "$field",
					Values: []chronograf.BasicTemplateValue{
						{
							Type:  "who knows?",
							Value: "field",
						},
					},
				},
			},
			want: `SELECT $field FROM "cpu"`,
		},
		{
			name:  "auto group by",
			query: `SELECT mean(usage_idle) from "cpu" where time > now() - 4320h :autoGroupBy:`,
			vars: chronograf.TemplateVars{
				&chronograf.GroupByVar{
					Var:               ":autoGroupBy:",
					Duration:          180 * 24 * time.Hour,
					Resolution:        1000,
					ReportingInterval: 10 * time.Second,
				},
			},
			want: `SELECT mean(usage_idle) from "cpu" where time > now() - 4320h group by time(1555s)`,
		},
		{
			name:  "auto group by without duration",
			query: `SELECT mean(usage_idle) from "cpu" WHERE time > now() - 4320h :autoGroupBy:`,
			vars: chronograf.TemplateVars{
				&chronograf.GroupByVar{
					Var:               ":autoGroupBy:",
					Duration:          0 * time.Minute,
					Resolution:        1000,
					ReportingInterval: 10 * time.Second,
				},
			},
			want: `SELECT mean(usage_idle) from "cpu" WHERE time > now() - 4320h group by time(1555s)`,
		},
		{
			name:  "auto group by with :dashboardTime:",
			query: `SELECT mean(usage_idle) from "cpu" WHERE time > :dashboardTime: :autoGroupBy:`,
			vars: chronograf.TemplateVars{
				&chronograf.GroupByVar{
					Var:               ":autoGroupBy:",
					Duration:          0 * time.Minute,
					Resolution:        1000,
					ReportingInterval: 10 * time.Second,
				},
				&chronograf.BasicTemplateVar{
					Var: ":dashboardTime:",
					Values: []chronograf.BasicTemplateValue{
						{
							Type:  "constant",
							Value: "now() - 4320h",
						},
					},
				},
			},
			want: `SELECT mean(usage_idle) from "cpu" WHERE time > now() - 4320h group by time(1555s)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TemplateReplace(tt.query, tt.vars)
			if got != tt.want {
				t.Errorf("TestParse %s =\n%s\nwant\n%s", tt.name, got, tt.want)
			}
		})
	}
}

func Test_TemplateVarsUnmarshalling(t *testing.T) {
	req := `[
	{
		"tempVar": ":autoGroupBy:",
		"duration": 15552000,
		"resolution": 1000,
		"reportingInterval": 10
	},
	{
		"tempVar": "cpu",
		"values": [
			{
				"type": "tagValue",
				"value": "cpu-total",
				"selected": false
			}
		]
	}
	]`

	expected := []string{
		"group by time(1555s)",
		"'cpu-total'",
	}

	var tvars chronograf.TemplateVars
	err := json.Unmarshal([]byte(req), &tvars)
	if err != nil {
		t.Fatal("Err unmarshaling:", err)
	}

	if len(tvars) != len(expected) {
		t.Fatal("Expected", len(expected), "vars but found", len(tvars))
	}

	for idx, tvar := range tvars {
		if actual := tvar.String(); expected[idx] != actual {
			t.Error("Unexpected tvar. Want:", expected[idx], "Got:", actual)
		}
	}
}
