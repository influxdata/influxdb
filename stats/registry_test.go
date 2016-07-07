package stats_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/stats"
)

func TestEmptyStatistics(t *testing.T) {
	found := stats.Collect(stats.Root.Open(), true)

	if length := len(found); length != 0 {
		t.Fatalf("non empty initial state. got %d, expected: %d", length, 0)
	}
}

// Test that we can create one statistic and that it disappears after it is deleted twice.
func TestOneStatistic(t *testing.T) {
	go func() {
		foo := stats.Root.
			NewBuilder("foo", "m", map[string]string{"tag": "T"}).
			MustBuild().
			Open()
		defer foo.Close()

		found := stats.Collect(stats.Root.Open(), true)
		expected := stats.Collection{foo}

		if !reflect.DeepEqual(found, expected) {
			t.Fatalf("should find statistic when there is an open recorder. got: %v, expected: %v", found, expected)
		}
	}()

	// foo is now closed - it shouldn't be open in any view opened from now on.

	found := stats.Collect(stats.Root.Open(), true)
	expected := stats.Collection{}
	if !reflect.DeepEqual(found, expected) {
		t.Fatalf("should not find statistic when view is opened after recorder is closed. got: %v, expected: %v", found, expected)
	}
}
