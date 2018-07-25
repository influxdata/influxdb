package platform

import (
	"strings"
	"unicode"
)

// Name represents a DB or a RP name.
type Name string

// Valid tells whether it is a accepted name or not.
func (n Name) Valid() bool {
	for _, r := range n {
		if !unicode.IsPrint(r) {
			return false
		}
	}

	return n != "" &&
		n != "." &&
		n != ".." &&
		!strings.ContainsAny(string(n), `/\`)
}
