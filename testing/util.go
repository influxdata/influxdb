package testing

import (
	"github.com/influxdata/platform"
)

// MustIDFromString is an helper to ensure a correct ID is built during testing.
func MustIDFromString(s string) platform.ID {
	id, err := platform.IDFromString(s)
	if err != nil {
		panic(err)
	}
	return *id
}
