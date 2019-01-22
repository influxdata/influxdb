package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/mock"
)

func TestID(t *testing.T) {
	c, closeFn, err := newTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	defer closeFn()

	c.IDGenerator = mock.NewIDGenerator(testIDStr, t)

	if err := c.Open(context.Background()); err != nil {
		t.Fatalf("failed to open bolt client: %v", err)
	}

	if got, want := c.ID(), testID; got != want {
		t.Errorf("Client.ID() = %v, want %v", got, want)
	}
}
