package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initLabelService(f platformtesting.LabelFields, t *testing.T) (platform.LabelService, string, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, l := range f.Labels {
		if err := s.PutLabel(ctx, l); err != nil {
			t.Fatalf("failed to populate labels")
		}
	}

	return s, OpPrefix, func() {}
}

func TestLabelService(t *testing.T) {
	t.Parallel()
	platformtesting.LabelService(initLabelService, t)
}
