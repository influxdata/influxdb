package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initSourceService(f platformtesting.SourceFields, t *testing.T) (platform.SourceService, string, func()) {
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
	return c, bolt.OpPrefix, func() {
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
