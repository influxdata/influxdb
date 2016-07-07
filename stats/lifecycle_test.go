package stats_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/stats"
)

func TestStatisticsFirst(t *testing.T) {
	stat := stats.Root.
		NewBuilder("key", "n", map[string]string{"tag": "T"}).
		MustBuild().
		Open()
	defer stat.Close()

	observed := stats.Collect(stats.Root.Open(), true)

	expected := stats.Collection{stat}
	if !reflect.DeepEqual(expected, observed) {
		t.Fatalf("did not observe existing statistic. got: %+v, expected: %+v", observed, expected)
	}
}

func TestMonitorFirst(t *testing.T) {
	view := stats.Root.Open()
	defer view.Close()

	stat := stats.Root.
		NewBuilder("key", "n", map[string]string{"tag": "T"}).
		MustBuild().
		Open()
	defer stat.Close()

	observed := stats.Collect(view, false)
	expected := stats.Collection{stat}
	if !reflect.DeepEqual(expected, observed) {
		t.Fatalf("did not observe new statistic. got: %+v, expected: %+v", observed, expected)
	}
}
