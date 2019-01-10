package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initMacroService(f platformtesting.MacroFields, t *testing.T) (platform.MacroService, string, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator

	ctx := context.TODO()
	for _, macro := range f.Macros {
		if err := s.ReplaceMacro(ctx, macro); err != nil {
			t.Fatalf("failed to populate macros")
		}
	}

	done := func() {
		for _, macro := range f.Macros {
			if err := s.DeleteMacro(ctx, macro.ID); err != nil {
				t.Fatalf("failed to clean up macros bolt test: %v", err)
			}
		}
	}
	return s, OpPrefix, done
}

func TestMacroService(t *testing.T) {
	platformtesting.MacroService(initMacroService, t)
}
