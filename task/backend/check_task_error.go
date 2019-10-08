package backend

import (
	"strings"
)

// IsUnrecoverable takes in an error and determines if it is permanent (requiring user intervention to fix)
func IsUnrecoverable(err error) bool {
	if err == nil {
		return false
	}

	errString := err.Error()

	// missing bucket requires user intervention to resolve
	if strings.Contains(errString, "could not find bucket") {
		return true
	}

	// unparseable Flux requires user intervention to resolve
	if strings.Contains(errString, "could not parse Flux script") {
		return true
	}

	return false
}
