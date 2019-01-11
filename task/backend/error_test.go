package backend_test

import (
	"testing"

	"github.com/influxdata/influxdb/task/backend"
)

func TestParseRequestStillQueuedError(t *testing.T) {
	e := backend.RequestStillQueuedError{Start: 1000, End: 2000}
	validMsg := e.Error()

	if err := backend.ParseRequestStillQueuedError(validMsg); err == nil || *err != e {
		t.Fatalf("%q should have parsed to %v, but got %v", validMsg, e, err)
	}
}
