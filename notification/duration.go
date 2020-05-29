package notification

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/codes"
)

// Duration is a custom type used for generating flux compatible durations.
type Duration ast.DurationLiteral

// TimeDuration convert notification.Duration to time.Duration.
func (d Duration) TimeDuration() time.Duration {
	dl := ast.DurationLiteral(d)
	dd, _ := ast.DurationFrom(&dl, time.Time{})
	return dd
}

// MarshalJSON turns a Duration into a JSON-ified string.
func (d Duration) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('"')
	for _, d := range d.Values {
		b.WriteString(strconv.Itoa(int(d.Magnitude)))
		b.WriteString(d.Unit)
	}
	b.WriteByte('"')

	return b.Bytes(), nil
}

// UnmarshalJSON turns a flux duration literal into a Duration.
func (d *Duration) UnmarshalJSON(b []byte) error {
	dur, err := parseDuration(string(b[1 : len(b)-1]))
	if err != nil {
		return err
	}

	*d = Duration{Values: dur}

	return nil
}

// FromTimeDuration converts a time.Duration to a notification.Duration type.
func FromTimeDuration(d time.Duration) (Duration, error) {
	dur, err := parseDuration(d.String())
	if err != nil {
		return Duration{}, err
	}
	return Duration{Values: dur}, nil
}

// TODO(jsternberg): This file copies over code from an internal package
// because we need them from an internal package and the only way they
// are exposed is through a package that depends on the core flux parser.
// We want to avoid a dependency on the core parser so we copy these
// implementations.
//
// In the future, we should consider exposing these functions from flux
// in a non-internal package outside of the parser package.

// parseDuration will convert a string into components of the duration.
func parseDuration(lit string) ([]ast.Duration, error) {
	var values []ast.Duration
	for len(lit) > 0 {
		n := 0
		for n < len(lit) {
			ch, size := utf8.DecodeRuneInString(lit[n:])
			if size == 0 {
				panic("invalid rune in duration")
			}

			if !unicode.IsDigit(ch) {
				break
			}
			n += size
		}

		if n == 0 {
			return nil, &flux.Error{
				Code: codes.Invalid,
				Msg:  fmt.Sprintf("invalid duration %s", lit),
			}
		}

		magnitude, err := strconv.ParseInt(lit[:n], 10, 64)
		if err != nil {
			return nil, err
		}
		lit = lit[n:]

		n = 0
		for n < len(lit) {
			ch, size := utf8.DecodeRuneInString(lit[n:])
			if size == 0 {
				panic("invalid rune in duration")
			}

			if !unicode.IsLetter(ch) {
				break
			}
			n += size
		}

		if n == 0 {
			return nil, &flux.Error{
				Code: codes.Invalid,
				Msg:  fmt.Sprintf("duration is missing a unit: %s", lit),
			}
		}

		unit := lit[:n]
		if unit == "µs" {
			unit = "us"
		}
		values = append(values, ast.Duration{
			Magnitude: magnitude,
			Unit:      unit,
		})
		lit = lit[n:]
	}
	return values, nil
}
