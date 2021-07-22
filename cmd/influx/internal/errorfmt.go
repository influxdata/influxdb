package internal

import (
	"errors"
	"strings"
	"unicode"
)

// ErrorFmt formats errors presented to the user such that the first letter in the error
// is capitalized and ends with an appropriate punctuation.
func ErrorFmt(err error) error {
	if err == nil {
		return nil
	}

	s := err.Error()

	s = strings.Trim(s, "\n .!?")

	count := 0
	s = strings.Map(
		func(r rune) rune {
			defer func() { count++ }()
			if count == 0 {
				return unicode.ToUpper(r)
			}
			return r
		},
		s,
	)

	s = s + "."

	return errors.New(s)
}
