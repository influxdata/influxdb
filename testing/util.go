package testing

import (
	"testing"

	"github.com/influxdata/platform"
)

func idFromString(t *testing.T, s string) platform.ID {
	id, err := platform.IDFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	return *id
}
