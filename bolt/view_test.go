package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initViewService(f platformtesting.ViewFields, t *testing.T) (platform.ViewService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	ctx := context.TODO()
	for _, b := range f.Views {
		if err := c.PutView(ctx, b); err != nil {
			t.Fatalf("failed to populate cells")
		}
	}
	return c, func() {
		defer closeFn()
		for _, b := range f.Views {
			if err := c.DeleteView(ctx, b.ID); err != nil {
				t.Logf("failed to remove cell: %v", err)
			}
		}
	}
}

func TestViewService_CreateView(t *testing.T) {
	platformtesting.CreateView(initViewService, t)
}

func TestViewService_FindViewByID(t *testing.T) {
	platformtesting.FindViewByID(initViewService, t)
}

func TestViewService_FindViews(t *testing.T) {
	platformtesting.FindViews(initViewService, t)
}

func TestViewService_DeleteView(t *testing.T) {
	platformtesting.DeleteView(initViewService, t)
}

func TestViewService_UpdateView(t *testing.T) {
	platformtesting.UpdateView(initViewService, t)
}
