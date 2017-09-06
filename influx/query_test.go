package influx

import (
	"reflect"
	"testing"

	"github.com/influxdata/chronograf"
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
						Field: "usage_idle",
						Funcs: []string{},
					},
					chronograf.Field{
						Field: "usage_guest_nice",
						Funcs: []string{},
					},
					chronograf.Field{
						Field: "usage_system",
						Funcs: []string{},
					},
					chronograf.Field{
						Field: "usage_guest",
						Funcs: []string{},
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
						Field: "usage_idle",
						Funcs: []string{
							"mean",
							"median",
						},
					},
					chronograf.Field{
						Field: "usage_guest_nice",
						Funcs: []string{
							"count",
							"mean",
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
						Field: "usage_user",
						Funcs: []string{},
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
						Field: "usage_user",
						Funcs: []string{},
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
						Field: "usage_user",
						Funcs: []string{},
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
						Field: "usage_user",
						Funcs: []string{},
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
						Field: "usage_user",
						Funcs: []string{},
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
						Field: "usage_user",
						Funcs: []string{},
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
						Field: "usage_user",
						Funcs: []string{},
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
						Field: "usage_idle",
						Funcs: []string{},
					},
					chronograf.Field{
						Field: "usage_guest_nice",
						Funcs: []string{},
					},
					chronograf.Field{
						Field: "usage_system",
						Funcs: []string{},
					},
					chronograf.Field{
						Field: "usage_guest",
						Funcs: []string{},
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
						Field: "usage_idle",
						Funcs: []string{},
					},
					chronograf.Field{
						Field: "usage_guest_nice",
						Funcs: []string{},
					},
					chronograf.Field{
						Field: "usage_system",
						Funcs: []string{},
					},
					chronograf.Field{
						Field: "usage_guest",
						Funcs: []string{},
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
						Field: "usage_idle",
						Funcs: []string{
							"mean",
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
						Field: "usage_idle",
						Funcs: []string{
							"mean",
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
			influxQL: `SELECT mean("usage_idle") FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m GROUP BY time(1m)`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Field: "usage_idle",
						Funcs: []string{
							"mean",
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
			name:     "Test fill number (int) accepted",
			influxQL: `SELECT mean("usage_idle") FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m GROUP BY time(1m) FILL(1337)`,
			want: chronograf.QueryConfig{
				Database:        "telegraf",
				Measurement:     "cpu",
				RetentionPolicy: "autogen",
				Fields: []chronograf.Field{
					chronograf.Field{
						Field: "usage_idle",
						Funcs: []string{
							"mean",
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
						Field: "usage_idle",
						Funcs: []string{
							"mean",
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
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Convert() = \n%#v\n want \n%#v\n", got, tt.want)
			}
		})
	}
}
