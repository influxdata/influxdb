package testing

import (
	"testing"

	"github.com/influxdata/platform"
)

func diffErrors(actual, expected error, t *testing.T) {
	if expected == nil && actual != nil {
		t.Fatalf("unexpected error %q", actual.Error())
	}

	if expected != nil && actual == nil {
		t.Fatalf("expected error %q but received nil", expected.Error())
	}

	if expected != nil && actual != nil && expected.Error() != actual.Error() {
		t.Fatalf("expected error %q but received error %q", expected.Error(), actual.Error())
	}
}

func diffPlatformErrors(name string, actual, expected error, opPrefix string, t *testing.T) {
	if expected == nil && actual != nil {
		t.Fatalf("%s failed, unexpected error %s", name, actual.Error())
	}

	if expected != nil && actual == nil {
		t.Fatalf("%s failed, expected error %s but received nil", name, expected.Error())
	}

	if expected != nil && actual != nil && platform.ErrorCode(expected) != platform.ErrorCode(actual) {
		t.Fatalf("%s failed, expected error %q but received error code %q", name, platform.ErrorCode(expected), platform.ErrorCode(actual))
	}

	if expected != nil && actual != nil && opPrefix+platform.ErrorOp(expected) != platform.ErrorOp(actual) {
		t.Fatalf("%s failed, expected error %q but received error op %q", name, opPrefix+platform.ErrorOp(expected), platform.ErrorOp(actual))
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
