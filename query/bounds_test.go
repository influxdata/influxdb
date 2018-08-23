package query_test

import (
	"testing"
	"time"

	"github.com/influxdata/platform/query"
)

var EmptyBounds = query.Bounds{
	Start: query.Now,
	Stop:  query.Now,
}

func TestBounds_HasZero(t *testing.T) {
	tests := []struct {
		name   string
		now    time.Time
		bounds query.Bounds
		want   bool
	}{
		{
			name: "single zero",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: query.Bounds{
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
			bounds: query.Bounds{
				Start: query.Time{},
				Stop:  query.Time{},
			},
			want: true,
		},
		{
			name: "both non-zero",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: query.Bounds{
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
		bounds query.Bounds
		want   bool
	}{
		{
			name: "empty bounds / start == stop",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: query.Bounds{
				Start: query.Now,
				Stop:  query.Now,
			},
			want: true,
		},
		{
			name: "empty bounds / absolute now == relative now",
			now:  time.Date(2018, time.August, 14, 11, 0, 0, 0, time.UTC),
			bounds: query.Bounds{
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
			bounds: query.Bounds{
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
			bounds: query.Bounds{
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
