package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltMacroService(t *testing.T) {
	influxdbtesting.MacroService(initBoltMacroService, t)
}

func TestInmemMacroService(t *testing.T) {
	influxdbtesting.MacroService(initInmemMacroService, t)
}

func initBoltMacroService(f influxdbtesting.MacroFields, t *testing.T) (influxdb.MacroService, string, func()) {
	s, closeBolt, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initMacroService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initInmemMacroService(f influxdbtesting.MacroFields, t *testing.T) (influxdb.MacroService, string, func()) {
	s, closeBolt, err := NewTestInmemStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initMacroService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initMacroService(s kv.Store, f influxdbtesting.MacroFields, t *testing.T) (influxdb.MacroService, string, func()) {
	svc := kv.NewService(s)
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing macro service: %v", err)
	}
	for _, macro := range f.Macros {
		if err := svc.ReplaceMacro(ctx, macro); err != nil {
			t.Fatalf("failed to populate test macros: %v", err)
		}
	}

	done := func() {
		for _, macro := range f.Macros {
			if err := svc.DeleteMacro(ctx, macro.ID); err != nil {
				t.Fatalf("failed to clean up macros bolt test: %v", err)
			}
		}
	}

	return svc, kv.OpPrefix, done
}
