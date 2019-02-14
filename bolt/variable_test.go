package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initVariableService(f platformtesting.VariableFields, t *testing.T) (platform.VariableService, string, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt test client: %v", err)
	}

	c.IDGenerator = f.IDGenerator
	ctx := context.TODO()

	for _, variable := range f.Variables {
		if err := c.ReplaceVariable(ctx, variable); err != nil {
			t.Fatalf("failed to populate test variables: %v", err)
		}
	}

	done := func() {
		defer closeFn()

		for _, variable := range f.Variables {
			if err := c.DeleteVariable(ctx, variable.ID); err != nil {
				t.Fatalf("failed to clean up variables bolt test: %v", err)
			}
		}
	}

	return c, bolt.OpPrefix, done
}

func TestVariableService(t *testing.T) {
	t.Parallel()
	platformtesting.VariableService(initVariableService, t)
}
