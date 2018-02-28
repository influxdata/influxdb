package influx

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/chronograf"
)

func TestTemplateReplace(t *testing.T) {
	tests := []struct {
		name  string
		query string
		vars  []chronograf.TemplateVar
		want  string
	}{
		{
			name:  "select with parameters",
			query: ":method: field1, :field: FROM :measurement: WHERE temperature > :temperature:",
			vars: []chronograf.TemplateVar{
				chronograf.TemplateVar{
					Var: ":temperature:",
					Values: []chronograf.TemplateValue{
						{
							Type:  "csv",
							Value: "10",
						},
					},
				},
				chronograf.TemplateVar{
					Var: ":field:",
					Values: []chronograf.TemplateValue{
						{
							Type:  "fieldKey",
							Value: "field2",
						},
					},
				},
				chronograf.TemplateVar{
					Var: ":method:",
					Values: []chronograf.TemplateValue{
						{
							Type:  "csv",
							Value: "SELECT",
						},
					},
				},
				chronograf.TemplateVar{
					Var: ":measurement:",
					Values: []chronograf.TemplateValue{
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
			query: `SELECT mean(:field:) FROM "cpu" WHERE :tag: = :value: GROUP BY :tag:`,
			vars: []chronograf.TemplateVar{
				chronograf.TemplateVar{
					Var: ":value:",
					Values: []chronograf.TemplateValue{
						{
							Type:  "tagValue",
							Value: "howdy.com",
						},
					},
				},
				chronograf.TemplateVar{
					Var: ":tag:",
					Values: []chronograf.TemplateValue{
						{
							Type:  "tagKey",
							Value: "host",
						},
					},
				},
				chronograf.TemplateVar{
					Var: ":field:",
					Values: []chronograf.TemplateValue{
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
			query: `SELECT :field: FROM "cpu"`,
			want:  `SELECT :field: FROM "cpu"`,
		},
		{
			name:  "var without a value",
			query: `SELECT :field: FROM "cpu"`,
			vars: []chronograf.TemplateVar{
				chronograf.TemplateVar{
					Var: ":field:",
				},
			},
			want: `SELECT :field: FROM "cpu"`,
		},
		{
			name:  "var with unknown type",
			query: `SELECT :field: FROM "cpu"`,
			vars: []chronograf.TemplateVar{
				chronograf.TemplateVar{
					Var: ":field:",
					Values: []chronograf.TemplateValue{
						{
							Type:  "who knows?",
							Value: "field",
						},
					},
				},
			},
			want: `SELECT :field: FROM "cpu"`,
		},
		{
			name:  "auto interval",
			query: `SELECT mean(usage_idle) from "cpu" where time > now() - 4320h group by time(:interval:)`,
			vars: []chronograf.TemplateVar{
				{
					Var: ":interval:",
					Values: []chronograf.TemplateValue{
						{
							Value: "333",
							Type:  "points",
						},
					},
				},
			},
			want: `SELECT mean(usage_idle) from "cpu" where time > now() - 4320h group by time(46702s)`,
		},
		{
			name:  "auto interval",
			query: `SELECT derivative(mean(usage_idle),:interval:) from "cpu" where time > now() - 4320h group by time(:interval:)`,
			vars: []chronograf.TemplateVar{
				{
					Var: ":interval:",
					Values: []chronograf.TemplateValue{
						{
							Value: "333",
							Type:  "points",
						},
					},
				},
			},
			want: `SELECT derivative(mean(usage_idle),46702s) from "cpu" where time > now() - 4320h group by time(46702s)`,
		},
		{
			name:  "auto group by",
			query: `SELECT mean(usage_idle) from "cpu" where time > now() - 4320h group by :interval:`,
			vars: []chronograf.TemplateVar{
				{
					Var: ":interval:",
					Values: []chronograf.TemplateValue{
						{
							Value: "999",
							Type:  "resolution",
						},
						{
							Value: "3",
							Type:  "pointsPerPixel",
						},
					},
				},
			},
			want: `SELECT mean(usage_idle) from "cpu" where time > now() - 4320h group by time(46702s)`,
		},
		{
			name:  "auto group by without duration",
			query: `SELECT mean(usage_idle) from "cpu" WHERE time > now() - 4320h group by :interval:`,
			vars: []chronograf.TemplateVar{
				{
					Var: ":interval:",
					Values: []chronograf.TemplateValue{
						{
							Value: "999",
							Type:  "resolution",
						},
						{
							Value: "3",
							Type:  "pointsPerPixel",
						},
					},
				},
			},
			want: `SELECT mean(usage_idle) from "cpu" WHERE time > now() - 4320h group by time(46702s)`,
		},
		{
			name:  "auto group by with :dashboardTime:",
			query: `SELECT mean(usage_idle) from "cpu" WHERE time > :dashboardTime: group by :interval:`,
			vars: []chronograf.TemplateVar{
				{
					Var: ":interval:",
					Values: []chronograf.TemplateValue{
						{
							Value: "1000",
							Type:  "resolution",
						},
						{
							Value: "3",
							Type:  "pointsPerPixel",
						},
					},
				},
				{
					Var: ":dashboardTime:",
					Values: []chronograf.TemplateValue{
						{
							Type:  "constant",
							Value: "now() - 4320h",
						},
					},
				},
			},
			want: `SELECT mean(usage_idle) from "cpu" WHERE time > now() - 4320h group by time(46655s)`,
		},
		{
			name:  "auto group by failing condition",
			query: `SELECT mean(usage_idle) FROM "cpu" WHERE time > :dashboardTime: GROUP BY :interval:`,
			vars: []chronograf.TemplateVar{
				{
					Var: ":interval:",
					Values: []chronograf.TemplateValue{
						{
							Value: "115",
							Type:  "resolution",
						},
						{
							Value: "3",
							Type:  "pointsPerPixel",
						},
					},
				},
				{
					Var: ":dashboardTime:",
					Values: []chronograf.TemplateValue{
						{
							Value:    "now() - 1h",
							Type:     "constant",
							Selected: true,
						},
					},
				},
			},
			want: `SELECT mean(usage_idle) FROM "cpu" WHERE time > now() - 1h GROUP BY time(93s)`,
		},
		{
			name:  "no template variables specified",
			query: `SELECT mean(usage_idle) FROM "cpu" WHERE time > :dashboardTime: GROUP BY :interval:`,
			want:  `SELECT mean(usage_idle) FROM "cpu" WHERE time > :dashboardTime: GROUP BY :interval:`,
		},
		{
			name:  "auto group by failing condition",
			query: `SELECT mean(usage_idle) FROM "cpu" WHERE time > :dashboardTime: GROUP BY :interval:`,
			vars: []chronograf.TemplateVar{
				{
					Var: ":interval:",
					Values: []chronograf.TemplateValue{
						{
							Value: "115",
							Type:  "resolution",
						},
						{
							Value: "3",
							Type:  "pointsPerPixel",
						},
					},
				},
				{
					Var: ":dashboardTime:",
					Values: []chronograf.TemplateValue{
						{
							Value:    "now() - 1h",
							Type:     "constant",
							Selected: true,
						},
					},
				},
			},
			want: `SELECT mean(usage_idle) FROM "cpu" WHERE time > now() - 1h GROUP BY time(93s)`,
		},
		{
			name:  "query with no template variables contained should return query",
			query: `SHOW DATABASES`,
			vars: []chronograf.TemplateVar{
				{
					Var: ":interval:",
					Values: []chronograf.TemplateValue{
						{
							Value: "115",
							Type:  "resolution",
						},
						{
							Value: "3",
							Type:  "pointsPerPixel",
						},
					},
				},
				{
					Var: ":dashboardTime:",
					Values: []chronograf.TemplateValue{
						{
							Value:    "now() - 1h",
							Type:     "constant",
							Selected: true,
						},
					},
				},
			},
			want: `SHOW DATABASES`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now, err := time.Parse(time.RFC3339, "1985-10-25T00:01:00Z")
			if err != nil {
				t.Fatal(err)
			}
			got, err := TemplateReplace(tt.query, tt.vars, now)
			if err != nil {
				t.Fatalf("TestParse unexpected TemplateReplace error: %v", err)
			}
			if got != tt.want {
				t.Errorf("TestParse %s =\n%s\nwant\n%s", tt.name, got, tt.want)
			}
		})
	}
}

func Test_TemplateVarsUnmarshalling(t *testing.T) {
	req := `[
	{
		"tempVar": ":interval:",
		"values": [
			{
				"value": "1000",
				"type":  "resolution"
			},
			{
				"value": "3",
				"type":  "pointsPerPixel"
			},
			{
				"value": "10",
				"type":  "reportingInterval"
			}
		]
	},
	{
		"tempVar": ":cpu:",
		"values": [
			{
				"type": "tagValue",
				"value": "cpu-total",
				"selected": false
			}
		]
	}
	]`

	want := []chronograf.TemplateVar{
		{
			Var: ":interval:",
			Values: []chronograf.TemplateValue{
				{
					Value: "1000",
					Type:  "resolution",
				},
				{
					Value: "3",
					Type:  "pointsPerPixel",
				},
				{
					Value: "10",
					Type:  "reportingInterval",
				},
			},
		},
		{
			Var: ":cpu:",
			Values: []chronograf.TemplateValue{
				{
					Value:    "cpu-total",
					Type:     "tagValue",
					Selected: false,
				},
			},
		},
	}

	var got []chronograf.TemplateVar
	err := json.Unmarshal([]byte(req), &got)
	if err != nil {
		t.Fatal("Err unmarshaling:", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("UnmarshalJSON() = \n%#v\n want \n%#v\n", got, want)
	}
}

func TestAutoGroupBy(t *testing.T) {
	tests := []struct {
		name           string
		resolution     int64
		pixelsPerPoint int64
		duration       time.Duration
		want           string
	}{
		{
			name:           "String() calculates the GROUP BY interval",
			resolution:     700,
			pixelsPerPoint: 3,
			duration:       24 * time.Hour,
			want:           "time(370s)",
		},
		{
			name:           "String() milliseconds if less than one second intervals",
			resolution:     100000,
			pixelsPerPoint: 3,
			duration:       time.Hour,
			want:           "time(107ms)",
		},
		{
			name:           "String() milliseconds if less than one millisecond",
			resolution:     100000,
			pixelsPerPoint: 3,
			duration:       time.Second,
			want:           "time(1ms)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AutoGroupBy(tt.resolution, tt.pixelsPerPoint, tt.duration)
			if got != tt.want {
				t.Errorf("TestAutoGroupBy %s =\n%s\nwant\n%s", tt.name, got, tt.want)
			}
		})
	}
}

func Test_RenderTemplate(t *testing.T) {
	gbvTests := []struct {
		name       string
		query      string
		want       string
		resolution uint // the screen resolution to render queries into
	}{
		{
			name:       "relative time only lower bound with one day of duration",
			query:      "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1d GROUP BY :interval:",
			resolution: 1000,
			want:       "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1d GROUP BY time(259s)",
		},
		{
			name:       "relative time offset by week",
			query:      "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1d - 7d AND time < now() - 7d GROUP BY :interval:",
			resolution: 1000,
			want:       "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1d - 7d AND time < now() - 7d GROUP BY time(259s)",
		},
		{
			name:       "relative time with relative upper bound with one minute of duration",
			query:      "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 3m  AND time < now() - 2m GROUP BY :interval:",
			resolution: 1000,
			want:       "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 3m  AND time < now() - 2m GROUP BY time(179ms)",
		},
		{
			name:       "relative time with relative lower bound and now upper with one day of duration",
			query:      "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1d  AND time < now() GROUP BY :interval:",
			resolution: 1000,
			want:       "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1d  AND time < now() GROUP BY time(259s)",
		},
		{
			name:       "absolute time with one minute of duration",
			query:      "SELECT mean(usage_idle) FROM cpu WHERE time > '1985-10-25T00:01:00Z' and time < '1985-10-25T00:02:00Z' GROUP BY :interval:",
			resolution: 1000,
			want:       "SELECT mean(usage_idle) FROM cpu WHERE time > '1985-10-25T00:01:00Z' and time < '1985-10-25T00:02:00Z' GROUP BY time(179ms)",
		},
		{
			name:       "absolute time with nano seconds and zero duration",
			query:      "SELECT mean(usage_idle) FROM cpu WHERE time > '2017-07-24T15:33:42.994Z' and time < '2017-07-24T15:33:42.994Z' GROUP BY :interval:",
			resolution: 1000,
			want:       "SELECT mean(usage_idle) FROM cpu WHERE time > '2017-07-24T15:33:42.994Z' and time < '2017-07-24T15:33:42.994Z' GROUP BY time(1ms)",
		},
		{
			name:  "query should be returned if there are no template variables",
			query: "SHOW DATABASES",
			want:  "SHOW DATABASES",
		},
	}

	for _, tt := range gbvTests {
		t.Run(tt.name, func(t *testing.T) {
			now, err := time.Parse(time.RFC3339, "1985-10-25T00:01:00Z")
			if err != nil {
				t.Fatal(err)
			}
			tvar := chronograf.TemplateVar{
				Var: ":interval:",
				Values: []chronograf.TemplateValue{
					{
						Value: fmt.Sprintf("%d", tt.resolution),
						Type:  "resolution",
					},
				},
			}

			got, err := RenderTemplate(tt.query, tvar, now)
			if err != nil {
				t.Fatalf("unexpected error rendering template %v", err)
			}

			if got != tt.want {
				t.Fatalf("%q - durations not equal! Want: %s, Got: %s", tt.name, tt.want, got)
			}
		})
	}
}
