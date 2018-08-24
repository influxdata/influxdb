package plan_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/plan/plantest"
	"github.com/influxdata/platform/query/values"
)

func TestBoundsIntersect(t *testing.T) {
	now := time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		now  time.Time
		a, b *plan.BoundsSpec
		want *plan.BoundsSpec
	}{
		{
			name: "contained",

			a: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(-1 * time.Hour)),
				Stop:  values.ConvertTime(now),
			},
			b: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(-30 * time.Minute)),
				Stop:  values.ConvertTime(now),
			},
			want: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(-30 * time.Minute)),
				Stop:  values.ConvertTime(now),
			},
		},
		{
			name: "contained sym",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(-30 * time.Minute)),
				Stop:  values.ConvertTime(now),
			},
			b: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(-1 * time.Hour)),
				Stop:  values.ConvertTime(now),
			},
			want: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(-30 * time.Minute)),
				Stop:  values.ConvertTime(now),
			},
		},
		{
			name: "no overlap",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(-1 * time.Hour)),
				Stop:  values.ConvertTime(now),
			},
			b: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(-3 * time.Hour)),
				Stop:  values.ConvertTime(now.Add(-2 * time.Hour)),
			},
			want: plan.EmptyBoundsSpec,
		},
		{
			name: "overlap",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(-1 * time.Hour)),
				Stop:  values.ConvertTime(now),
			},
			b: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(-2 * time.Hour)),
				Stop:  values.ConvertTime(now.Add(-30 * time.Minute)),
			},
			want: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(-1 * time.Hour)),
				Stop:  values.ConvertTime(now.Add(-30 * time.Minute)),
			},
		},
		{
			name: "absolute times",
			a: &plan.BoundsSpec{
				Start: values.ConvertTime(time.Date(2018, time.January, 1, 0, 1, 0, 0, time.UTC)),
				Stop:  values.ConvertTime(time.Date(2018, time.January, 1, 0, 3, 0, 0, time.UTC)),
			},
			b: &plan.BoundsSpec{
				Start: values.ConvertTime(time.Date(2018, time.January, 1, 0, 4, 0, 0, time.UTC)),
				Stop:  values.ConvertTime(time.Date(2018, time.January, 1, 0, 5, 0, 0, time.UTC)),
			},
			want: plan.EmptyBoundsSpec,
		},
		{
			name: "intersect with empty returns empty",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: &plan.BoundsSpec{
				Start: values.ConvertTime(time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC)),
				Stop:  values.ConvertTime(now),
			},
			b:    plan.EmptyBoundsSpec,
			want: plan.EmptyBoundsSpec,
		},
		{
			name: "intersect with empty returns empty sym",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a:    plan.EmptyBoundsSpec,
			b: &plan.BoundsSpec{
				Start: values.ConvertTime(time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC)),
				Stop:  values.ConvertTime(now),
			},
			want: plan.EmptyBoundsSpec,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Intersect(tt.b)
			if !cmp.Equal(got, tt.want) {
				t.Errorf("unexpected bounds -want/+got:\n%s", cmp.Diff(tt.want, got, plantest.CmpOptions...))
			}
		})
	}
}

func TestBounds_Union(t *testing.T) {
	now := time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		now  time.Time
		a, b *plan.BoundsSpec
		want *plan.BoundsSpec
	}{
		{
			name: "basic case",
			a: &plan.BoundsSpec{
				Start: values.ConvertTime(time.Date(2018, time.January, 1, 0, 1, 0, 0, time.UTC)),
				Stop:  values.ConvertTime(time.Date(2018, time.January, 1, 0, 3, 0, 0, time.UTC)),
			},
			b: &plan.BoundsSpec{
				Start: values.ConvertTime(time.Date(2018, time.January, 1, 0, 2, 0, 0, time.UTC)),
				Stop:  values.ConvertTime(time.Date(2018, time.January, 1, 0, 4, 0, 0, time.UTC)),
			},
			want: &plan.BoundsSpec{
				Start: values.ConvertTime(time.Date(2018, time.January, 1, 0, 1, 0, 0, time.UTC)),
				Stop:  values.ConvertTime(time.Date(2018, time.January, 1, 0, 4, 0, 0, time.UTC)),
			},
		},
		{
			name: "union with empty returns empty",
			a: &plan.BoundsSpec{
				Start: values.ConvertTime(time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC)),
				Stop:  values.ConvertTime(now),
			},
			b:    plan.EmptyBoundsSpec,
			want: plan.EmptyBoundsSpec,
		},
		{
			name: "union with empty returns empty sym",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a:    plan.EmptyBoundsSpec,
			b: &plan.BoundsSpec{
				Start: values.ConvertTime(time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC)),
				Stop:  values.ConvertTime(now),
			},
			want: plan.EmptyBoundsSpec,
		},
		{
			name: "no overlap",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			a: &plan.BoundsSpec{
				Start: values.ConvertTime(time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC)),
				Stop:  values.ConvertTime(time.Date(2018, time.January, 1, 0, 20, 0, 0, time.UTC)),
			},
			b: &plan.BoundsSpec{
				Start: values.ConvertTime(time.Date(2018, time.January, 1, 0, 45, 0, 0, time.UTC)),
				Stop:  values.ConvertTime(time.Date(2018, time.January, 1, 0, 50, 0, 0, time.UTC)),
			},
			want: &plan.BoundsSpec{
				Start: values.ConvertTime(time.Date(2018, time.January, 1, 0, 15, 0, 0, time.UTC)),
				Stop:  values.ConvertTime(time.Date(2018, time.January, 1, 0, 50, 0, 0, time.UTC)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Union(tt.b)
			if !cmp.Equal(got, tt.want) {
				t.Errorf("unexpected bounds -want/+got:\n%s", cmp.Diff(tt.want, got, plantest.CmpOptions...))
			}
		})
	}
}

func TestBounds_IsEmpty(t *testing.T) {
	now := time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		now    time.Time
		bounds *plan.BoundsSpec
		want   bool
	}{
		{
			name: "empty bounds / start == stop",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: &plan.BoundsSpec{
				Start: values.ConvertTime(now),
				Stop:  values.ConvertTime(now),
			},
			want: true,
		},
		{
			name: "empty bounds / absolute now == relative now",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: &plan.BoundsSpec{
				Start: values.ConvertTime(now),
				Stop:  values.ConvertTime(time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC)),
			},
			want: true,
		},
		{
			name: "start > stop",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(time.Hour)),
				Stop:  values.ConvertTime(now),
			},
			want: true,
		},
		{
			name: "start < stop",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: &plan.BoundsSpec{
				Start: values.ConvertTime(now.Add(-1 * time.Hour)),
				Stop:  values.ConvertTime(now),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.bounds.IsEmpty()
			if got != tt.want {
				t.Errorf("unexpected result for bounds.IsEmpty(): got %t, want %t", got, tt.want)
			}
		})
	}
}

/*
func TestPlanner_ResolveBounds(t *testing.T) {
	tests := []struct {
		name   string
		now time.Time
		bounds query.Bounds
		want   plan.BoundsSpec
	}{
		{
			name: "relative bounds",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: query.Bounds{
				Start: query.Time{
					Relative: true,
					Start: -1 * time.Hour,
				},
				Stop: values.ConvertTime(now),
			},
			want: &plan.BoundsSpec{
				Start: values.Time(time.Unix())
			}
		}
	}

}
*/
