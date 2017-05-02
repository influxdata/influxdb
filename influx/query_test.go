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
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Convert() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
