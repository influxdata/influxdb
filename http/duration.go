package http

import (
	"fmt"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

// ErrInvalidDuration is returned when parsing a malformatted duration.
var ErrInvalidDuration = &errors.Error{
	Code: errors.EInvalid,
	Msg:  "invalid duration",
}

// ParseDuration parses a time duration from a string.
// This is needed instead of time.ParseDuration because this will support
// the full syntax that InfluxQL supports for specifying durations
// including weeks and days.
func ParseDuration(s string) (time.Duration, error) {
	// Return an error if the string is blank or one character
	if len(s) < 2 {
		return 0, ErrInvalidDuration
	}

	// Split string into individual runes.
	a := split(s)

	// Start with a zero duration.
	var d time.Duration
	i := 0

	// Check for a negative.
	isNegative := false
	if a[i] == '-' {
		isNegative = true
		i++
	}

	var measure int64
	var unit string

	// Parsing loop.
	for i < len(a) {
		// Find the number portion.
		start := i
		for ; i < len(a) && isDigit(a[i]); i++ {
			// Scan for the digits.
		}

		// Check if we reached the end of the string prematurely.
		if i >= len(a) || i == start {
			return 0, ErrInvalidDuration
		}

		// Parse the numeric part.
		n, err := strconv.ParseInt(string(a[start:i]), 10, 64)
		if err != nil {
			return 0, ErrInvalidDuration
		}
		measure = n

		// Extract the unit of measure.
		unit = string(a[i])
		switch a[i] {
		case 'n':
			d += time.Duration(n) * time.Nanosecond
			if i+1 < len(a) && a[i+1] == 's' {
				unit = string(a[i : i+2])
				i += 2
				continue
			}
		case 'u', 'µ':
			d += time.Duration(n) * time.Microsecond
			if i+1 < len(a) && a[i+1] == 's' {
				unit = string(a[i : i+2])
				i += 2
				continue
			}
		case 'm':
			// Check for `mo` and `ms`; month and millisecond, respectively.
			if i+1 < len(a) {
				switch a[i+1] {
				case 's': // ms == milliseconds
					unit = string(a[i : i+2])
					d += time.Duration(n) * time.Millisecond
					i += 2
					continue
				case 'o': // mo == month
					// TODO(goller): use real duration values:
					// https://github.com/influxdata/platform/issues/657
					unit = string(a[i : i+2])
					d += time.Duration(n) * 30 * 24 * time.Hour
					i += 2
					continue
				}
			}
			d += time.Duration(n) * time.Minute
		case 's':
			d += time.Duration(n) * time.Second
		case 'h':
			d += time.Duration(n) * time.Hour
		case 'd':
			d += time.Duration(n) * 24 * time.Hour
		case 'w':
			// TODO(goller): use real duration values:
			// https://github.com/influxdata/platform/issues/657
			d += time.Duration(n) * 7 * 24 * time.Hour
		case 'y':
			// TODO(goller): use real duration values:
			// https://github.com/influxdata/platform/issues/657
			d += time.Duration(n) * 365 * 24 * time.Hour
		default:
			return 0, ErrInvalidDuration
		}
		i++
	}

	// Check to see if we overflowed a duration
	if d < 0 && !isNegative {
		return 0, fmt.Errorf("overflowed duration %d%s: choose a smaller duration or INF", measure, unit)
	}

	if isNegative {
		d = -d
	}
	return d, nil
}

// FormatDuration formats a duration to a string.
func FormatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	} else if d%(365*24*time.Hour) == 0 {
		return fmt.Sprintf("%dy", d/(365*24*time.Hour))
	} else if d%(30*24*time.Hour) == 0 {
		return fmt.Sprintf("%dmo", d/(30*24*time.Hour))
	} else if d%(7*24*time.Hour) == 0 {
		return fmt.Sprintf("%dw", d/(7*24*time.Hour))
	} else if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	} else if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", d/time.Hour)
	} else if d%time.Minute == 0 {
		return fmt.Sprintf("%dm", d/time.Minute)
	} else if d%time.Second == 0 {
		return fmt.Sprintf("%ds", d/time.Second)
	} else if d%time.Millisecond == 0 {
		return fmt.Sprintf("%dms", d/time.Millisecond)
	} else if d%time.Microsecond == 0 {
		return fmt.Sprintf("%dus", d/time.Microsecond)
	}
	return fmt.Sprintf("%dns", d/time.Nanosecond)
}

// split splits a string into a slice of runes.
func split(s string) (a []rune) {
	for _, ch := range s {
		a = append(a, ch)
	}
	return
}

// isDigit returns true if the rune is a digit.
func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }
