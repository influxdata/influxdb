// Package toml adds support to marshal and unmarshal types not in the official TOML spec.
package toml // import "github.com/influxdata/influxdb/toml"

import (
	"fmt"
	"math"
	"strconv"
	"time"
	"unicode"
)

// Duration is a TOML wrapper type for time.Duration.
type Duration time.Duration

// String returns the string representation of the duration.
func (d Duration) String() string {
	return time.Duration(d).String()
}

// UnmarshalText parses a TOML value into a duration value.
func (d *Duration) UnmarshalText(text []byte) error {
	// Ignore if there is no value set.
	if len(text) == 0 {
		return nil
	}

	// Otherwise parse as a duration formatted string.
	duration, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}

	// Set duration and return.
	*d = Duration(duration)
	return nil
}

// MarshalText converts a duration to a string for decoding toml
func (d Duration) MarshalText() (text []byte, err error) {
	return []byte(d.String()), nil
}

// Size represents a TOML parseable file size.
// Users can specify size using "k" or "K" for kibibytes, "m" or "M" for mebibytes,
// and "g" or "G" for gibibytes. If a size suffix isn't specified then bytes are assumed.
type Size uint64

// UnmarshalText parses a byte size from text.
func (s *Size) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		return fmt.Errorf("size was empty")
	}

	// The multiplier defaults to 1 in case the size has
	// no suffix (and is then just raw bytes)
	mult := uint64(1)

	// Preserve the original text for error messages
	sizeText := text

	// Parse unit of measure
	suffix := text[len(sizeText)-1]
	if !unicode.IsDigit(rune(suffix)) {
		switch suffix {
		case 'k', 'K':
			mult = 1 << 10 // KiB
		case 'm', 'M':
			mult = 1 << 20 // MiB
		case 'g', 'G':
			mult = 1 << 30 // GiB
		default:
			return fmt.Errorf("unknown size suffix: %c (expected k, m, or g)", suffix)
		}
		sizeText = sizeText[:len(sizeText)-1]
	}

	// Parse numeric portion of value.
	size, err := strconv.ParseUint(string(sizeText), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid size: %s", string(text))
	}

	if math.MaxUint64/mult < size {
		return fmt.Errorf("size would overflow the max size (%d) of a uint: %s", uint64(math.MaxUint64), string(text))
	}

	size *= mult

	*s = Size(size)
	return nil
}
