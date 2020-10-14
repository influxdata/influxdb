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

	// unparsable Flux requires user intervention to resolve
	if strings.Contains(errString, "could not parse Flux script") {
		return true
	}

	// Flux script uses an API that attempts to read the filesystem
	if strings.Contains(errString, "filesystem service uninitialized") {
		return true
	}

	return false
}
