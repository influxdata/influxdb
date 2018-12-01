package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initLabelService(f platformtesting.LabelFields, t *testing.T) (platform.LabelService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	ctx := context.Background()
	for _, l := range f.Labels {
		if err := c.CreateLabel(ctx, l); err != nil {
			t.Fatalf("failed to populate labels")
		}
	}

	return c, func() {
		defer closeFn()
		for _, l := range f.Labels {
			if err := c.DeleteLabel(ctx, *l); err != nil {
				t.Logf("failed to remove label: %v", err)
			}
		}
	}
}

func TestLabelService_LabelService(t *testing.T) {
	platformtesting.LabelService(initLabelService, t)
}
