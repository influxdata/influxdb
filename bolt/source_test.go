package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initSourceService(f platformtesting.SourceFields, t *testing.T) (platform.SourceService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	ctx := context.TODO()
	for _, b := range f.Sources {
		if err := c.PutSource(ctx, b); err != nil {
			t.Fatalf("failed to populate buckets")
		}
	}
	return c, func() {
		defer closeFn()
		for _, b := range f.Sources {
			if err := c.DeleteSource(ctx, b.ID); err != nil {
				t.Logf("failed to remove bucket: %v", err)
			}
		}
	}
}

func TestSourceService_CreateSource(t *testing.T) {
	platformtesting.CreateSource(initSourceService, t)
}
func TestSourceService_FindSourceByID(t *testing.T) {
	platformtesting.FindSourceByID(initSourceService, t)
}

func TestSourceService_FindSources(t *testing.T) {
	platformtesting.FindSources(initSourceService, t)
}

func TestSourceService_DeleteSource(t *testing.T) {
	platformtesting.DeleteSource(initSourceService, t)
}
