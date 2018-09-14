package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initViewService(f platformtesting.ViewFields, t *testing.T) (platform.ViewService, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.TODO()
	for _, b := range f.Views {
		if err := s.PutView(ctx, b); err != nil {
			t.Fatalf("failed to populate Views")
		}
	}
	return s, func() {}
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
