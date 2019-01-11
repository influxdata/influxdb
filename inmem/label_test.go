package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initLabelService(f platformtesting.LabelFields, t *testing.T) (platform.LabelService, string, func()) {
	s := NewService()
	ctx := context.TODO()
	for _, m := range f.Labels {
		if err := s.CreateLabel(ctx, m); err != nil {
			t.Fatalf("failed to populate labels")
		}
	}

	return s, OpPrefix, func() {}
}

func TestLabelService(t *testing.T) {
	platformtesting.LabelService(initLabelService, t)
}
