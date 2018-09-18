package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initMacroService(f platformtesting.MacroFields, t *testing.T) (platform.MacroService, func()) {
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

	return c, done
}

func TestMacroService_CreateMacro(t *testing.T) {
	platformtesting.CreateMacro(initMacroService, t)
}

func TestMacroService_FindMacroByID(t *testing.T) {
	platformtesting.FindMacroByID(initMacroService, t)
}

func TestMacroService_UpdateMacro(t *testing.T) {
	platformtesting.UpdateMacro(initMacroService, t)
}

func TestMacroService_DeleteMacro(t *testing.T) {
	platformtesting.DeleteMacro(initMacroService, t)
}
