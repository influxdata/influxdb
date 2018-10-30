package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
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
