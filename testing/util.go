package testing

import (
	"testing"

	"github.com/influxdata/platform"
)

// MustIDFromString is an helper to ensure a correct ID is built during testing.
func MustIDFromString(t *testing.T, s string) platform.ID {
	id, err := platform.IDFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	return *id
}
