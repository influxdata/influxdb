package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initVariableService(f platformtesting.VariableFields, t *testing.T) (platform.VariableService, string, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator

	ctx := context.TODO()
	for _, variable := range f.Variables {
		if err := s.ReplaceVariable(ctx, variable); err != nil {
			t.Fatalf("failed to populate variables")
		}
	}

	done := func() {
		for _, variable := range f.Variables {
			if err := s.DeleteVariable(ctx, variable.ID); err != nil {
				t.Fatalf("failed to clean up variables bolt test: %v", err)
			}
		}
	}
	return s, OpPrefix, done
}

func TestVariableService(t *testing.T) {
	platformtesting.VariableService(initVariableService, t)
}
