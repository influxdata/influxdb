package bolt_test

import (
	"context"
	"github.com/influxdata/platform/bolt"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initMacroService(f platformtesting.MacroFields, t *testing.T) (platform.MacroService, string, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt test client: %v", err)
	}

	c.IDGenerator = f.IDGenerator
	ctx := context.TODO()

	for _, macro := range f.Macros {
		if err := c.ReplaceMacro(ctx, macro); err != nil {
			t.Fatalf("failed to populate test macros: %v", err)
		}
	}

	done := func() {
		defer closeFn()

		for _, macro := range f.Macros {
			if err := c.DeleteMacro(ctx, macro.ID); err != nil {
				t.Fatalf("failed to clean up macros bolt test: %v", err)
			}
		}
	}

	return c, bolt.OpPrefix, done
}

func TestMacroService(t *testing.T) {
	platformtesting.MacroService(initMacroService, t)
}
