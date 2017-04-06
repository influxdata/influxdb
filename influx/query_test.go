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
		want     chronograf.QueryConfig
		wantErr  bool
	}{
		{
			name:     "Test named count field",
			influxQL: `SELECT moving_average(mean("count"),14) FROM "usage_computed"."autogen".unique_clusters_by_day WHERE time > now() - 90d AND product = 'influxdb' group by time(1d)`,
			want: chronograf.QueryConfig{
				RawText: `SELECT moving_average(mean("count"),14) FROM "usage_computed"."autogen".unique_clusters_by_day WHERE time > now() - 90d AND product = 'influxdb' group by time(1d)`,
				Fields:  []chronograf.Field{},
				Tags:    map[string][]string{},
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
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Convert() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
