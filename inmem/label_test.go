package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initLabelService(f platformtesting.LabelFields, t *testing.T) (platform.LabelService, func()) {
	s := NewService()
	ctx := context.TODO()
	for _, m := range f.Labels {
		if err := s.CreateLabel(ctx, m); err != nil {
			t.Fatalf("failed to populate labels")
		}
	}

	return s, func() {}
}

func TestLabelService(t *testing.T) {
	platformtesting.LabelService(initLabelService, t)
}
