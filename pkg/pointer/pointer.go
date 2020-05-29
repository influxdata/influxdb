// Package pointer provides utilities for pointer handling that aren't available in go.
// Feel free to add more pointerification functions for more types as you need them.
package pointer

import (
	"time"
)

// Duration returns a pointer to its argument.
func Duration(d time.Duration) *time.Duration {
	return &d
}

// Int returns a pointer to its argument.
func Int(i int) *int {
	return &i
}

// Int64 returns a pointer to its argument.
func Int64(i int64) *int64 {
	return &i
}

// String returns a pointer to its argument.
func String(s string) *string {
	return &s
}

// Time returns a pointer to its argument.
func Time(t time.Time) *time.Time {
	return &t
}
