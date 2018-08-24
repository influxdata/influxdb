package query

import "time"

type Bounds struct {
	Start Time
	Stop  Time
}

// IsEmpty reports whether the given bounds
// are empty, i.e., if start >= stop.
func (b Bounds) IsEmpty(now time.Time) bool {
	return b.Start.Time(now).Equal(b.Stop.Time(now)) || b.Start.Time(now).After(b.Stop.Time(now))
}

// HasZero returns true if the given bounds contain a Go zero time value as either Start or Stop.
func (b Bounds) HasZero() bool {
	return b.Start.IsZero() || b.Stop.IsZero()
}
