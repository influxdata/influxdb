package backend_test

import (
	"testing"

	"github.com/influxdata/platform/task/backend"
)

func TestParseRetryAlreadyQueuedError(t *testing.T) {
	e := backend.RetryAlreadyQueuedError{Start: 1000, End: 2000}
	validMsg := e.Error()

	if err := backend.ParseRetryAlreadyQueuedError(validMsg); err == nil || *err != e {
		t.Fatalf("%q should have parsed to %v, but got %v", validMsg, e, err)
	}
}
