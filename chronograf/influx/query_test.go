package influx

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/chronograf"
)

func TestConvert(t *testing.T) {
	tests := []struct {
		name     string
		influxQL string
		RawText  string
		want     chronograf.QueryConfig
		wantErr  bool
	}{
		{
			name:     "Test field order",
			influxQL: `SELECT "usage_idle", "usage_guest_nice", "usage_system", "usage_guest" FROM "telegraf"."autogen"."cpu" WHERE time > :dashboardTime:`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "usage_idle",
						Type:  "field",
					},
					chronograf.Field{
						Value: "usage_guest_nice",
						Type:  "field",
					},
					chronograf.Field{
						Value: "usage_system",
						Type:  "field",
					},
					chronograf.Field{
						Value: "usage_guest",
						Type:  "field",
					},
				},
				Tags: map[string][]string{},
				GroupBy: chronograf.GroupBy{
					Tags: []string{},
				},
			},
		},
		{
			name:     "Test field function order",
			influxQL: `SELECT mean("usage_idle"), median("usage_idle"), count("usage_guest_nice"), mean("usage_guest_nice") FROM "telegraf"."autogen"."cpu" WHERE time > :dashboardTime:`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "mean",
						Type:  "func",
						Args: []chronograf.Field{
							{
								Value: "usage_idle",
								Type:  "field",
							},
						},
					},
					chronograf.Field{
						Value: "median",
						Type:  "func",
						Args: []chronograf.Field{
							{
								Value: "usage_idle",
								Type:  "field",
							},
						},
					},
					chronograf.Field{
						Value: "count",
						Type:  "func",
						Args: []chronograf.Field{
							{
								Value: "usage_guest_nice",
								Type:  "field",
							},
						},
					},
					chronograf.Field{
						Value: "mean",
						Type:  "func",
						Args: []chronograf.Field{
							{
								Value: "usage_guest_nice",
								Type:  "field",
							},
						},
					},
				},
				Tags: map[string][]string{},
				GroupBy: chronograf.GroupBy{
					Tags: []string{},
				},
			},
		},
		{
			name:     "Test named count field",
			influxQL: `SELECT moving_average(mean("count"),14) FROM "usage_computed"."autogen".unique_clusters_by_day WHERE time > now() - 90d AND product = 'influxdb' group by time(1d)`,
			RawText:  `SELECT moving_average(mean("count"),14) FROM "usage_computed"."autogen".unique_clusters_by_day WHERE time > now() - 90d AND product = 'influxdb' group by time(1d)`,
			want: chronograf.QueryConfig{
				Fields: []chronograf.Field{},
				Tags:   map[string][]string{},
				GroupBy: chronograf.GroupBy{
					Tags: []string{},
				},
			},
		},
		{
			name:     "Test math",
			influxQL: `SELECT count("event_id")/3 as "event_count_id" from discource.autogen.discourse_events where time > now() - 7d group by time(1d), "event_type"`,
			RawText:  `SELECT count("event_id")/3 as "event_count_id" from discource.autogen.discourse_events where time > now() - 7d group by time(1d), "event_type"`,
			want: chronograf.QueryConfig{
				Fields: []chronograf.Field{},
				Tags:   map[string][]string{},
				GroupBy: chronograf.GroupBy{
					Tags: []string{},
				},
			},
		},
		{
			name:     "Test range",
			influxQL: `SELECT usage_user from telegraf.autogen.cpu where "host" != 'myhost' and time > now() - 15m`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "usage_user",
						Type:  "field",
					},
				},
				Tags: map[string][]string{"host": []string{"myhost"}},
				GroupBy: chronograf.GroupBy{
					Time: "",
					Tags: []string{},
				},
				AreTagsAccepted: false,
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
				},
			},
		},
		{
			name:     "Test invalid range",
			influxQL: `SELECT usage_user from telegraf.autogen.cpu where "host" != 'myhost' and time > now() - 15`,
			RawText:  `SELECT usage_user from telegraf.autogen.cpu where "host" != 'myhost' and time > now() - 15`,
			want: chronograf.QueryConfig{
				Fields: []chronograf.Field{},
				Tags:   map[string][]string{},
				GroupBy: chronograf.GroupBy{
					Tags: []string{},
				},
			},
		},
		{
			name:     "Test range with no duration",
			influxQL: `SELECT usage_user from telegraf.autogen.cpu where "host" != 'myhost' and time > now()`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "usage_user",
						Type:  "field",
					},
				},
				Tags: map[string][]string{"host": []string{"myhost"}},
				GroupBy: chronograf.GroupBy{
					Time: "",
					Tags: []string{},
				},
				AreTagsAccepted: false,
				Range: &chronograf.DurationRange{
					Lower: "now() - 0s",
				},
			},
		},
		{
			name:     "Test range with no tags",
			influxQL: `SELECT usage_user from telegraf.autogen.cpu where time > now() - 15m`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Tags:            map[string][]string{},
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "usage_user",
						Type:  "field",
					},
				},
				GroupBy: chronograf.GroupBy{
					Time: "",
					Tags: []string{},
				},
				AreTagsAccepted: false,
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
				},
			},
		},
		{
			name:     "Test range with no tags nor duration",
			influxQL: `SELECT usage_user from telegraf.autogen.cpu where time`,
			RawText:  `SELECT usage_user from telegraf.autogen.cpu where time`,
			want: chronograf.QueryConfig{
				Fields: []chronograf.Field{},
				Tags:   map[string][]string{},
				GroupBy: chronograf.GroupBy{
					Tags: []string{},
				},
			},
		},
		{
			name:     "Test with no time range",
			influxQL: `SELECT usage_user from telegraf.autogen.cpu where "host" != 'myhost' and time`,
			RawText:  `SELECT usage_user from telegraf.autogen.cpu where "host" != 'myhost' and time`,
			want: chronograf.QueryConfig{
				Fields: []chronograf.Field{},
				Tags:   map[string][]string{},
				GroupBy: chronograf.GroupBy{
					Tags: []string{},
				},
			},
		},
		{
			name:     "Test with no where clauses",
			influxQL: `SELECT usage_user from telegraf.autogen.cpu`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "usage_user",
						Type:  "field",
					},
				},
				Tags: map[string][]string{},
				GroupBy: chronograf.GroupBy{
					Time: "",
					Tags: []string{},
				},
			},
		},
		{
			name:     "Test tags accepted",
			influxQL: `SELECT usage_user from telegraf.autogen.cpu where "host" = 'myhost' and time > now() - 15m`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "usage_user",
						Type:  "field",
					},
				},
				Tags: map[string][]string{"host": []string{"myhost"}},
				GroupBy: chronograf.GroupBy{
					Time: "",
					Tags: []string{},
				},
				AreTagsAccepted: true,
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
					Upper: "",
				},
			},
		},
		{
			name:     "Test multible tags not accepted",
			influxQL: `SELECT usage_user from telegraf.autogen.cpu where time > now() - 15m and "host" != 'myhost' and "cpu" != 'cpu-total'`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "usage_user",
						Type:  "field",
					},
				},
				Tags: map[string][]string{
					"host": []string{
						"myhost",
					},
					"cpu": []string{
						"cpu-total",
					},
				},
				GroupBy: chronograf.GroupBy{
					Time: "",
					Tags: []string{},
				},
				AreTagsAccepted: false,
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
					Upper: "",
				},
			},
		},
		{
			name:     "Test mixed tag logic",
			influxQL: `SELECT usage_user from telegraf.autogen.cpu where ("host" = 'myhost' or "this" = 'those') and ("howdy" != 'doody') and time > now() - 15m`,
			RawText:  `SELECT usage_user from telegraf.autogen.cpu where ("host" = 'myhost' or "this" = 'those') and ("howdy" != 'doody') and time > now() - 15m`,
			want: chronograf.QueryConfig{
				Fields: []chronograf.Field{},
				Tags:   map[string][]string{},
				GroupBy: chronograf.GroupBy{
					Tags: []string{},
				},
			},
		},
		{
			name:     "Test tags accepted",
			influxQL: `SELECT usage_user from telegraf.autogen.cpu where ("host" = 'myhost' OR "host" = 'yourhost') and ("these" = 'those') and time > now() - 15m`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "usage_user",
						Type:  "field",
					},
				},
				Tags: map[string][]string{
					"host":  []string{"myhost", "yourhost"},
					"these": []string{"those"},
				},
				GroupBy: chronograf.GroupBy{
					Time: "",
					Tags: []string{},
				},
				AreTagsAccepted: true,
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
				},
			},
		},
		{
			name:     "Complex Logic with tags not accepted",
			influxQL: `SELECT "usage_idle", "usage_guest_nice", "usage_system", "usage_guest" FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m AND ("cpu"!='cpu-total' OR "cpu"!='cpu0') AND ("host"!='dev-052978d6-us-east-2-meta-0' OR "host"!='dev-052978d6-us-east-2-data-5' OR "host"!='dev-052978d6-us-east-2-data-4' OR "host"!='dev-052978d6-us-east-2-data-3')`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "usage_idle",
						Type:  "field",
					},
					chronograf.Field{
						Value: "usage_guest_nice",
						Type:  "field",
					},
					chronograf.Field{
						Value: "usage_system",
						Type:  "field",
					},
					chronograf.Field{
						Value: "usage_guest",
						Type:  "field",
					},
				},
				Tags: map[string][]string{
					"host": []string{
						"dev-052978d6-us-east-2-meta-0",
						"dev-052978d6-us-east-2-data-5",
						"dev-052978d6-us-east-2-data-4",
						"dev-052978d6-us-east-2-data-3",
					},
					"cpu": []string{
						"cpu-total",
						"cpu0",
					},
				},
				GroupBy: chronograf.GroupBy{
					Time: "",
					Tags: []string{},
				},
				AreTagsAccepted: false,
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
				},
			},
		},
		{
			name:     "Complex Logic with tags accepted",
			influxQL: `SELECT "usage_idle", "usage_guest_nice", "usage_system", "usage_guest" FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m AND ("cpu" = 'cpu-total' OR "cpu" = 'cpu0') AND ("host" = 'dev-052978d6-us-east-2-meta-0' OR "host" = 'dev-052978d6-us-east-2-data-5' OR "host" = 'dev-052978d6-us-east-2-data-4' OR "host" = 'dev-052978d6-us-east-2-data-3')`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "usage_idle",
						Type:  "field",
					},
					chronograf.Field{
						Value: "usage_guest_nice",
						Type:  "field",
					},
					chronograf.Field{
						Value: "usage_system",
						Type:  "field",
					},
					chronograf.Field{
						Value: "usage_guest",
						Type:  "field",
					},
				},
				Tags: map[string][]string{
					"host": []string{
						"dev-052978d6-us-east-2-meta-0",
						"dev-052978d6-us-east-2-data-5",
						"dev-052978d6-us-east-2-data-4",
						"dev-052978d6-us-east-2-data-3",
					},
					"cpu": []string{
						"cpu-total",
						"cpu0",
					},
				},
				GroupBy: chronograf.GroupBy{
					Time: "",
					Tags: []string{},
				},
				AreTagsAccepted: true,
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
				},
			},
		},
		{
			name:     "Test explicit non-null fill accepted",
			influxQL: `SELECT mean("usage_idle") FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m GROUP BY time(1m) FILL(linear)`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "mean",
						Type:  "func",
						Args: []chronograf.Field{
							{
								Value: "usage_idle",
								Type:  "field",
							},
						},
					},
				},
				GroupBy: chronograf.GroupBy{
					Time: "1m",
					Tags: []string{},
				},
				Tags:            map[string][]string{},
				AreTagsAccepted: false,
				Fill:            "linear",
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
				},
			},
		},
		{
			name:     "Test explicit null fill accepted",
			influxQL: `SELECT mean("usage_idle") FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m GROUP BY time(1m) FILL(null)`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "mean",
						Type:  "func",
						Args: []chronograf.Field{
							{
								Value: "usage_idle",
								Type:  "field",
							},
						},
					},
				},
				GroupBy: chronograf.GroupBy{
					Time: "1m",
					Tags: []string{},
				},
				Tags:            map[string][]string{},
				AreTagsAccepted: false,
				Fill:            "null",
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
				},
			},
		},
		{
			name:     "Test implicit null fill accepted and made explicit",
			influxQL: `SELECT mean("usage_idle") as "mean_usage_idle" FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m GROUP BY time(1m)`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "mean",
						Type:  "func",
						Alias: "mean_usage_idle",
						Args: []chronograf.Field{
							{
								Value: "usage_idle",
								Type:  "field",
							},
						},
					},
				},
				GroupBy: chronograf.GroupBy{
					Time: "1m",
					Tags: []string{},
				},
				Tags:            map[string][]string{},
				AreTagsAccepted: false,
				Fill:            "null",
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
				},
			},
		},
		{
			name:     "Test percentile with a number parameter",
			influxQL: `SELECT percentile("usage_idle", 3.14) as "mean_usage_idle" FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m GROUP BY time(1m)`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "percentile",
						Type:  "func",
						Alias: "mean_usage_idle",
						Args: []chronograf.Field{
							{
								Value: "usage_idle",
								Type:  "field",
							},
							chronograf.Field{
								Value: "3.14",
								Type:  "number",
							},
						},
					},
				},
				GroupBy: chronograf.GroupBy{
					Time: "1m",
					Tags: []string{},
				},
				Tags:            map[string][]string{},
				AreTagsAccepted: false,
				Fill:            "null",
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
				},
			},
		},
		{
			name:     "Test top with 2 arguments",
			influxQL: `SELECT TOP("water_level","location",2) FROM "h2o_feet"`,
			want: chronograf.QueryConfig{
				Measurement: "h2o_feet",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "top",
						Type:  "func",
						Args: []chronograf.Field{
							{
								Value: "water_level",
								Type:  "field",
							},
							chronograf.Field{
								Value: "location",
								Type:  "field",
							},
							chronograf.Field{
								Value: "2",
								Type:  "integer",
							},
						},
					},
				},
				GroupBy: chronograf.GroupBy{
					Tags: []string{},
				},
				Tags:            map[string][]string{},
				AreTagsAccepted: false,
			},
		},
		{
			name:     "count of a regex",
			influxQL: ` SELECT COUNT(/water/) FROM "h2o_feet"`,
			want: chronograf.QueryConfig{
				Measurement: "h2o_feet",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "count",
						Type:  "func",
						Args: []chronograf.Field{
							{
								Value: "water",
								Type:  "regex",
							},
						},
					},
				},
				GroupBy: chronograf.GroupBy{
					Tags: []string{},
				},
				Tags:            map[string][]string{},
				AreTagsAccepted: false,
			},
		},
		{
			name:     "count with aggregate",
			influxQL: `SELECT COUNT(water) as "count_water" FROM "h2o_feet"`,
			want: chronograf.QueryConfig{
				Measurement: "h2o_feet",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "count",
						Type:  "func",
						Alias: "count_water",
						Args: []chronograf.Field{
							{
								Value: "water",
								Type:  "field",
							},
						},
					},
				},
				GroupBy: chronograf.GroupBy{
					Tags: []string{},
				},
				Tags:            map[string][]string{},
				AreTagsAccepted: false,
			},
		},
		{
			name:     "count of a wildcard",
			influxQL: ` SELECT COUNT(*) FROM "h2o_feet"`,
			want: chronograf.QueryConfig{
				Measurement: "h2o_feet",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "count",
						Type:  "func",
						Args: []chronograf.Field{
							{
								Value: "*",
								Type:  "wildcard",
							},
						},
					},
				},
				GroupBy: chronograf.GroupBy{
					Tags: []string{},
				},
				Tags:            map[string][]string{},
				AreTagsAccepted: false,
			},
		},
		{
			name:     "Test fill number (int) accepted",
			influxQL: `SELECT mean("usage_idle") FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m GROUP BY time(1m) FILL(1337)`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "mean",
						Type:  "func",
						Args: []chronograf.Field{
							{
								Value: "usage_idle",
								Type:  "field",
							},
						},
					},
				},
				GroupBy: chronograf.GroupBy{
					Time: "1m",
					Tags: []string{},
				},
				Tags:            map[string][]string{},
				AreTagsAccepted: false,
				Fill:            "1337",
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
				},
			},
		},
		{
			name:     "Test fill number (float) accepted",
			influxQL: `SELECT mean("usage_idle") FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m GROUP BY time(1m) FILL(1.337)`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Value: "mean",
						Type:  "func",
						Args: []chronograf.Field{
							{
								Value: "usage_idle",
								Type:  "field",
							},
						},
					},
				},
				GroupBy: chronograf.GroupBy{
					Time: "1m",
					Tags: []string{},
				},
				Tags:            map[string][]string{},
				AreTagsAccepted: false,
				Fill:            "1.337",
				Range: &chronograf.DurationRange{
					Lower: "now() - 15m",
				},
			},
		},
		{
			name:     "Test invalid fill rejected",
			influxQL: `SELECT mean("usage_idle") FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m GROUP BY time(1m) FILL(LINEAR)`,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Convert(tt.influxQL)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.RawText != "" {
				tt.want.RawText = &tt.RawText
				if got.RawText == nil {
					t.Errorf("Convert() = nil, want %s", tt.RawText)
				} else if *got.RawText != tt.RawText {
					t.Errorf("Convert() = %s, want %s", *got.RawText, tt.RawText)
				}
			}
			if !cmp.Equal(got, tt.want) {
				t.Errorf("Convert() = %s", cmp.Diff(got, tt.want))
			}
		})
	}
}

func TestParseTime(t *testing.T) {
	tests := []struct {
		name     string
		influxQL string
		now      string
		want     time.Duration
		wantErr  bool
	}{
		{
			name:     "time equal",
			now:      "2000-01-01T00:00:00Z",
			influxQL: `SELECT mean("numSeries") AS "mean_numSeries" FROM "_internal"."monitor"."database" WHERE time > now() - 1h and time < now() - 1h GROUP BY :interval: FILL(null);`,
			want:     0,
		},
		{
			name:     "time shifted by one hour",
			now:      "2000-01-01T00:00:00Z",
			influxQL: `SELECT mean("numSeries") AS "mean_numSeries" FROM "_internal"."monitor"."database" WHERE time > now() - 1h - 1h and time < now() - 1h GROUP BY :interval: FILL(null);`,
			want:     3599999999998,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now, err := time.Parse(time.RFC3339, tt.now)
			if err != nil {
				t.Fatalf("%v", err)
			}
			got, err := ParseTime(tt.influxQL, now)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Logf("%d", got)
				t.Errorf("ParseTime() = %v, want %v", got, tt.want)
			}
		})
	}
}
