package bolt_test

import (
	"context"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/inmem"
	platformtesting "github.com/influxdata/influxdb/testing"
	"testing"
)

func initVariableService(f platformtesting.VariableFields, t *testing.T) (platform.VariableService, string, func()) {
	c := inmem.NewService()

	if f.TimeGenerator == nil {
		c.TimeGenerator = platform.RealTimeGenerator{}
	}
	c.IDGenerator = f.IDGenerator
	c.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		c.TimeGenerator = platform.RealTimeGenerator{}
	}

	ctx := context.Background()
	for _, variable := range f.Variables {
		if err := c.ReplaceVariable(ctx, variable); err != nil {
			t.Fatalf("failed to populate test variables: %v", err)
		}
	}

	done := func() {
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
