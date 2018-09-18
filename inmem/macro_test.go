package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initMacroService(f platformtesting.MacroFields, t *testing.T) (platform.MacroService, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator

	ctx := context.TODO()
	for _, macro := range f.Macros {
		if err := s.ReplaceMacro(ctx, macro); err != nil {
			t.Fatalf("failed to populate macros")
		}
	}

	done := func() {}
	return s, done
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
