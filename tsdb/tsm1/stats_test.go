package tsm1_test

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/tsdb/tsm1"
)

func TestMeasurementStats_WriteTo(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		stats, other := tsm1.NewMeasurementStats(), tsm1.NewMeasurementStats()
		var buf bytes.Buffer
		if wn, err := stats.WriteTo(&buf); err != nil {
			t.Fatal(err)
		} else if rn, err := other.ReadFrom(&buf); err != nil {
			t.Fatal(err)
		} else if wn != rn {
			t.Fatalf("byte count mismatch: w=%d r=%d", wn, rn)
		} else if diff := cmp.Diff(stats, other); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("WithData", func(t *testing.T) {
		stats, other := tsm1.NewMeasurementStats(), tsm1.NewMeasurementStats()
		stats["cpu"] = 100
		stats["mem"] = 2000

		var buf bytes.Buffer
		if wn, err := stats.WriteTo(&buf); err != nil {
			t.Fatal(err)
		} else if rn, err := other.ReadFrom(&buf); err != nil {
			t.Fatal(err)
		} else if wn != rn {
			t.Fatalf("byte count mismatch: w=%d r=%d", wn, rn)
		} else if diff := cmp.Diff(stats, other); diff != "" {
			t.Fatal(diff)
		}
	})
}
