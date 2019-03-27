package gen

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestTimeSequenceSpec_ForTimeRange(t *testing.T) {
	secs := func(sec int64) time.Time {
		return time.Unix(sec, 0).UTC()
	}

	tests := []struct {
		name string
		ts   TimeSequenceSpec
		tr   TimeRange
		exp  TimeSequenceSpec
	}{
		{
			// this test verifies Count is reduced
			// as the range has fewer intervals than Count * Delta
			name: "delta/range_fewer",
			ts: TimeSequenceSpec{
				Count: 100,
				Delta: 10 * time.Second,
			},
			tr: TimeRange{
				Start: secs(0),
				End:   secs(100),
			},
			exp: TimeSequenceSpec{
				Count: 10,
				Start: secs(0),
				Delta: 10 * time.Second,
			},
		},
		{
			// this test verifies Count is not adjusted
			// as the range equals Count * Delta
			name: "delta/range_equal",
			ts: TimeSequenceSpec{
				Count: 100,
				Delta: 10 * time.Second,
			},
			tr: TimeRange{
				Start: secs(0),
				End:   secs(1000),
			},
			exp: TimeSequenceSpec{
				Count: 100,
				Start: secs(0),
				Delta: 10 * time.Second,
			},
		},
		{
			// this test verifies the Start is adjusted to
			// limit the number of intervals to Count
			name: "delta/range_greater",
			ts: TimeSequenceSpec{
				Count: 100,
				Delta: 10 * time.Second,
			},
			tr: TimeRange{
				Start: secs(0),
				End:   secs(2000),
			},
			exp: TimeSequenceSpec{
				Count: 100,
				Start: secs(1000),
				Delta: 10 * time.Second,
			},
		},

		{
			// this test verifies Count is reduced
			// as the time range has fewer intervals than Count * Precision
			name: "precision/range_fewer",
			ts: TimeSequenceSpec{
				Count:     100,
				Precision: 10 * time.Second,
			},
			tr: TimeRange{
				Start: secs(0),
				End:   secs(100),
			},
			exp: TimeSequenceSpec{
				Count: 10,
				Start: secs(0),
				Delta: 10 * time.Second,
			},
		},

		{
			// this test verifies Count is unchanged and Delta is a multiple
			// of Precision, given the time range has more intervals
			// than Count * Precision
			name: "precision/range_greater",
			ts: TimeSequenceSpec{
				Count:     100,
				Precision: 10 * time.Second,
			},
			tr: TimeRange{
				Start: secs(0),
				End:   secs(2000),
			},
			exp: TimeSequenceSpec{
				Count: 100,
				Start: secs(0),
				Delta: 20 * time.Second,
			},
		},

		{
			// this test verifies Count is unchanged and Delta is equal
			// to Precision, given the time range has an equal number of
			// intervals as Count * Precision
			name: "precision/range_equal",
			ts: TimeSequenceSpec{
				Count:     100,
				Precision: 10 * time.Second,
			},
			tr: TimeRange{
				Start: secs(0),
				End:   secs(1000),
			},
			exp: TimeSequenceSpec{
				Count: 100,
				Start: secs(0),
				Delta: 10 * time.Second,
			},
		},

		{
			// this test verifies Count is reduced
			// as the range has fewer intervals than Count * Delta
			name: "start/rounding",
			ts: TimeSequenceSpec{
				Count: 100,
				Delta: 10 * time.Second,
			},
			tr: TimeRange{
				Start: secs(13),
				End:   secs(110),
			},
			exp: TimeSequenceSpec{
				Count: 10,
				Start: secs(10),
				Delta: 10 * time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ts.ForTimeRange(tt.tr); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected value -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}
