package plan_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/plan/plantest"
)

func TestBoundsIntersect(t *testing.T) {
	tests := []struct {
		name string
		now  time.Time
		a, b plan.BoundsSpec
		want plan.BoundsSpec
	}{
		{
			name: "contained",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Now,
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -30 * time.Minute,
				},
				Stop: query.Now,
			},
			want: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -30 * time.Minute,
				},
				Stop: query.Now,
			},
		},
		{
			name: "contained sym",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -30 * time.Minute,
				},
				Stop: query.Now,
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Now,
			},
			want: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -30 * time.Minute,
				},
				Stop: query.Now,
			},
		},
		{
			name: "no overlap",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Now,
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -3 * time.Hour,
				},
				Stop: query.Time{
					IsRelative: true,
					Relative:   -2 * time.Hour,
				},
			},
			want: plan.EmptyBoundsSpec,
		},
		{
			name: "no overlap sym",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -3 * time.Hour,
				},
				Stop: query.Time{
					IsRelative: true,
					Relative:   -2 * time.Hour,
				},
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Now,
			},
			want: plan.EmptyBoundsSpec,
		},
		{
			name: "overlap",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Now,
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -2 * time.Hour,
				},
				Stop: query.Time{
					IsRelative: true,
					Relative:   -30 * time.Minute,
				},
			},
			want: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Time{
					IsRelative: true,
					Relative:   -30 * time.Minute,
				},
			},
		},
		{
			name: "overlap sym",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -2 * time.Hour,
				},
				Stop: query.Time{
					IsRelative: true,
					Relative:   -30 * time.Minute,
				},
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Now,
			},
			want: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Time{
					IsRelative: true,
					Relative:   -30 * time.Minute,
				},
			},
		},
		{
			name: "both start zero",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Stop: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
			},
			b: plan.BoundsSpec{
				Stop: query.Time{
					IsRelative: true,
					Relative:   -20 * time.Minute,
				},
			},
			want: plan.BoundsSpec{},
		},
		{
			name: "both start zero sym",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Stop: query.Time{
					IsRelative: true,
					Relative:   -20 * time.Minute,
				},
			},
			b: plan.BoundsSpec{
				Stop: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
			},
			want: plan.BoundsSpec{},
		},
		{
			name: "absolute times",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 1, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 3, 0, 0, time.UTC),
				},
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 4, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 5, 0, 0, time.UTC),
				},
			},
			want: plan.EmptyBoundsSpec,
		},
		{
			name: "absolute times sym",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 4, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 5, 0, 0, time.UTC),
				},
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 1, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 3, 0, 0, time.UTC),
				},
			},
			want: plan.EmptyBoundsSpec,
		},
		{
			name: "relative bounds future",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Time{
					IsRelative: true,
					Relative:   5 * time.Hour,
				},
			},
			b: plan.BoundsSpec{
				Start: query.Now,
				Stop: query.Time{
					IsRelative: true,
					Relative:   3 * time.Hour,
				},
			},
			want: plan.BoundsSpec{
				Start: query.Now,
				Stop: query.Time{
					IsRelative: true,
					Relative:   3 * time.Hour,
				},
			},
		},
		{
			name: "relative bounds 2",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -3 * time.Hour,
				},
				Stop: query.Now,
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -2 * time.Hour,
				},
				Stop: query.Time{
					IsRelative: true,
					Relative:   2 * time.Hour,
				},
			},
			want: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -2 * time.Hour,
				},
				Stop: query.Now,
			},
		},
		{
			name: "start stop zero",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a:    plan.BoundsSpec{},
			b: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 1, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 3, 0, 0, time.UTC),
				},
			},
			want: plan.BoundsSpec{},
		},
		{
			name: "one stops zero",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -3 * time.Hour,
				},
				Stop: query.Now,
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -2 * time.Hour,
				},
			},
			want: plan.BoundsSpec{},
		},
		{
			name: "relative/absolute",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC),
				},
				Stop: query.Now,
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 10, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.August, 15, 11, 0, 0, 0, time.UTC),
				},
			},
			want: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC),
				},
				Stop: query.Now,
			},
		},
		{
			name: "intersect with empty returns empty",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC),
				},
				Stop: query.Now,
			},
			b:    plan.EmptyBoundsSpec,
			want: plan.EmptyBoundsSpec,
		},
		{
			name: "intersect with empty returns empty sym",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a:    plan.EmptyBoundsSpec,
			b: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC),
				},
				Stop: query.Now,
			},
			want: plan.EmptyBoundsSpec,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Intersect(tt.b, tt.now)
			if !cmp.Equal(got, tt.want) {
				t.Errorf("unexpected bounds -want/+got:\n%s", cmp.Diff(tt.want, got, plantest.CmpOptions...))
			}
		})
	}
}

func TestBounds_Union(t *testing.T) {
	tests := []struct {
		name string
		now  time.Time
		a, b plan.BoundsSpec
		want plan.BoundsSpec
	}{
		{
			name: "basic case",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 1, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 3, 0, 0, time.UTC),
				},
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 2, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 4, 0, 0, time.UTC),
				},
			},
			want: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 1, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 4, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "basic case relative",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Now,
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -30 * time.Minute,
				},
				Stop: query.Now,
			},
			want: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Now,
			},
		},
		{
			name: "bounds in future",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -2 * time.Hour,
				},
				Stop: query.Now,
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Time{
					IsRelative: true,
					Relative:   2 * time.Hour,
				},
			},
			want: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -2 * time.Hour,
				},
				Stop: query.Time{
					IsRelative: true,
					Relative:   2 * time.Hour,
				},
			},
		},
		{
			name: "one zero, one not",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -2 * time.Hour,
				},
				Stop: query.Now,
			},
			b:    plan.BoundsSpec{},
			want: plan.BoundsSpec{},
		},
		{
			name: "relative/absolute",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Now,
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 10, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 20, 0, 0, time.UTC),
				},
			},
			want: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 10, 0, 0, time.UTC),
				},
				Stop: query.Now,
			},
		},
		{
			name: "union with empty returns empty",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC),
				},
				Stop: query.Now,
			},
			b:    plan.EmptyBoundsSpec,
			want: plan.EmptyBoundsSpec,
		},
		{
			name: "union with empty returns empty sym",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a:    plan.EmptyBoundsSpec,
			b: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC),
				},
				Stop: query.Now,
			},
			want: plan.EmptyBoundsSpec,
		},
		{
			name: "no overlap",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 20, 0, 0, time.UTC),
				},
			},
			b: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 45, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 50, 0, 0, time.UTC),
				},
			},
			want: plan.BoundsSpec{
				Start: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC),
				},
				Stop: query.Time{
					Absolute: time.Date(2018, time.January, 1, 0, 50, 0, 0, time.UTC),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Union(tt.b, tt.now)
			if !cmp.Equal(got, tt.want) {
				t.Errorf("unexpected bounds -want/+got:\n%s", cmp.Diff(tt.want, got, plantest.CmpOptions...))
			}
		})
	}
}

func TestBounds_HasZero(t *testing.T) {
	tests := []struct {
		name   string
		now    time.Time
		bounds plan.BoundsSpec
		want   bool
	}{
		{
			name: "single zero",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Time{},
			},
			want: true,
		},
		{
			name: "both zero",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: plan.BoundsSpec{
				Start: query.Time{},
				Stop:  query.Time{},
			},
			want: true,
		},
		{
			name: "both non-zero",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Now,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.bounds.HasZero()
			if got != tt.want {
				t.Errorf("unexpected result for bounds.HasZero(): got %t, want %t", got, tt.want)
			}
		})
	}
}

func TestBounds_IsEmpty(t *testing.T) {
	tests := []struct {
		name   string
		now    time.Time
		bounds plan.BoundsSpec
		want   bool
	}{
		{
			name: "empty bounds / start == stop",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: plan.BoundsSpec{
				Start: query.Now,
				Stop:  query.Now,
			},
			want: true,
		},
		{
			name: "empty bounds / absolute now == relative now",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: plan.BoundsSpec{
				Start: query.Now,
				Stop: query.Time{
					Absolute: time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
				},
			},
			want: true,
		},
		{
			name: "start > stop",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   time.Hour,
				},
				Stop: query.Now,
			},
			want: true,
		},
		{
			name: "start < stop",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: plan.BoundsSpec{
				Start: query.Time{
					IsRelative: true,
					Relative:   -1 * time.Hour,
				},
				Stop: query.Now,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.bounds.IsEmpty(tt.now)
			if got != tt.want {
				t.Errorf("unexpected result for bounds.IsEmpty(): got %t, want %t", got, tt.want)
			}
		})
	}
}
