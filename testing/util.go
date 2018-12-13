package testing

import (
	"testing"

	"github.com/influxdata/platform"
)

func diffPlatformErrors(name string, actual, expected error, opPrefix string, t *testing.T) {
	if expected == nil && actual == nil {
		return
	}

	if expected == nil && actual != nil {
		t.Fatalf("%s failed, unexpected error %s", name, actual.Error())
	}

	if expected != nil && actual == nil {
		t.Fatalf("%s failed, expected error %s but received nil", name, expected.Error())
	}

	if platform.ErrorCode(expected) != platform.ErrorCode(actual) {
		t.Fatalf("%s failed, expected error %q but received error code %q", name, platform.ErrorCode(expected), platform.ErrorCode(actual))
	}

	if opPrefix+platform.ErrorOp(expected) != platform.ErrorOp(actual) {
		t.Fatalf("%s failed, expected error %q but received error op %q", name, opPrefix+platform.ErrorOp(expected), platform.ErrorOp(actual))
	}

	if platform.ErrorMessage(expected) != platform.ErrorMessage(actual) {
		t.Fatalf("%s failed, expected error %q but received error message %q", name, platform.ErrorMessage(expected), platform.ErrorMessage(actual))
	}
}

// MustIDBase16 is an helper to ensure a correct ID is built during testing.
func MustIDBase16(s string) platform.ID {
	id, err := platform.IDFromString(s)
	if err != nil {
		panic(err)
	}
	return *id
}
