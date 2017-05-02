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
					Lower: "15m",
					Upper: "now",
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
					Lower: "0s",
					Upper: "now",
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
					Lower: "15m",
					Upper: "now",
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
					Lower: "15m",
					Upper: "now",
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
					Lower: "15m",
					Upper: "now",
				},
			},
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
				t.Errorf("Convert() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
