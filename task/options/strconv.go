package options

import (
	"fmt"
	"strconv"
	"unicode"
	"unicode/utf8"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/codes"
)

// TODO(jsternberg): This file copies over code from an internal package
// because we need them from an internal package and the only way they
// are exposed is through a package that depends on the core flux parser.
// We want to avoid a dependency on the core parser so we copy these
// implementations.
//
// In the future, we should consider exposing these functions from flux
// in a non-internal package outside of the parser package.

// ParseSignedDuration is a helper wrapper around parser.ParseSignedDuration.
// We use it because we need to clear the basenode, but flux does not.
func ParseSignedDuration(text string) (*ast.DurationLiteral, error) {
	// TODO(jsternberg): This is copied from an internal package in flux to break a dependency
	// on the parser package where this method is exposed.
	// Consider exposing this properly in flux.
	if r, s := utf8.DecodeRuneInString(text); r == '-' {
		d, err := parseDuration(text[s:])
		if err != nil {
			return nil, err
		}
		for i := range d {
			d[i].Magnitude = -d[i].Magnitude
		}
		return &ast.DurationLiteral{Values: d}, nil
	}

	d, err := parseDuration(text)
	if err != nil {
		return nil, err
	}
	return &ast.DurationLiteral{Values: d}, nil
}

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
