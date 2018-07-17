package execute_test

import (
	"testing"

	"github.com/influxdata/platform/query/execute"
)

// Written to verify symmetrical behavior of execute.(Bounds).Overlaps
// Given two execute.Bounds a and b, if a.Overlaps(b) then b.Overlaps(a).

// Cases:
// given two ranges [a1, a2), [b1, b2)
// a1 <= b1 <= a2 <= b2 -> true
// b1 <= a1 <= b2 <= a2 -> true
// a1 <= b1 <= b2 <= a2 -> true
// b2 <= a1 <= a2 <= b2 -> true
// a1 <= a2 <= b1 <= b2 -> false
// b1 <= b2 <= a1 <= a2 -> false

func TestBounds_Overlaps(t *testing.T) {
	tests := []struct {
		name string
		a, b execute.Bounds
		want bool
	}{
		{
			name: "edge overlap",
			a: execute.Bounds{
				Start: execute.Time(0),
				Stop:  execute.Time(10),
			},
			b: execute.Bounds{
				Start: execute.Time(10),
				Stop:  execute.Time(20),
			},

			want: false,
		},
		{
			name: "edge overlap sym",
			a: execute.Bounds{
				Start: execute.Time(10),
				Stop:  execute.Time(20),
			},
			b: execute.Bounds{
				Start: execute.Time(0),
				Stop:  execute.Time(10),
			},
			want: false,
		},
		{
			name: "single overlap",
			a: execute.Bounds{
				Start: execute.Time(0),
				Stop:  execute.Time(10),
			},
			b: execute.Bounds{
				Start: execute.Time(5),
				Stop:  execute.Time(15),
			},
			want: true,
		},
		{
			name: "no overlap sym",
			a: execute.Bounds{
				Start: execute.Time(0),
				Stop:  execute.Time(10),
			},
			b: execute.Bounds{
				Start: execute.Time(5),
				Stop:  execute.Time(15),
			},
			want: true,
		},
		{
			name: "double overlap (bounds contained)",
			a: execute.Bounds{
				Start: execute.Time(10),
				Stop:  execute.Time(20),
			},
			b: execute.Bounds{
				Start: execute.Time(14),
				Stop:  execute.Time(15),
			},
			want: true,
		},
		{
			name: "double overlap (bounds contained) sym",
			a: execute.Bounds{
				Start: execute.Time(14),
				Stop:  execute.Time(15),
			},
			b: execute.Bounds{
				Start: execute.Time(10),
				Stop:  execute.Time(20),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Overlaps(tt.b); got != tt.want {
				t.Errorf("Bounds.Overlaps() = %v, want %v", got, tt.want)
			}
		})
	}
}
