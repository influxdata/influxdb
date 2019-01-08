package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initKeyValueLog(f platformtesting.KeyValueLogFields, t *testing.T) (platform.KeyValueLog, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	ctx := context.Background()
	for _, e := range f.LogEntries {
		if err := c.AddLogEntry(ctx, e.Key, e.Value, e.Time); err != nil {
			t.Fatalf("failed to populate log entries")
		}
	}
	return c, func() {
		closeFn()
	}
}

// TestKeyValueLog runs the conformance test for a keyvalue log
func TestKeyValueLog(t *testing.T) {
	platformtesting.KeyValueLog(initKeyValueLog, t)
}
