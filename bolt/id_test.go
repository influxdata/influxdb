package bolt_test

import (
	"context"
	"testing"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
)

func TestID(t *testing.T) {
	c, closeFn, err := newTestClient(t)
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	defer closeFn()

	testID := platform2.ID(70000)
	c.IDGenerator = mock.NewIDGenerator(testID.String(), t)

	if err := c.Open(context.Background()); err != nil {
		t.Fatalf("failed to open bolt client: %v", err)
	}

	if got, want := c.ID(), testID; got != want {
		t.Errorf("Client.ID() = %v, want %v", got, want)
	}
}
