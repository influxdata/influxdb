package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltSourceService(t *testing.T) {
	t.Run("CreateSource", func(t *testing.T) { influxdbtesting.CreateSource(initBoltSourceService, t) })
	t.Run("FindSourceByID", func(t *testing.T) { influxdbtesting.FindSourceByID(initBoltSourceService, t) })
	t.Run("FindSources", func(t *testing.T) { influxdbtesting.FindSources(initBoltSourceService, t) })
	t.Run("DeleteSource", func(t *testing.T) { influxdbtesting.DeleteSource(initBoltSourceService, t) })
}

func initBoltSourceService(f influxdbtesting.SourceFields, t *testing.T) (influxdb.SourceService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initSourceService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initSourceService(s kv.Store, f influxdbtesting.SourceFields, t *testing.T) (influxdb.SourceService, string, func()) {
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing source service: %v", err)
	}
	for _, b := range f.Sources {
		if err := svc.PutSource(ctx, b); err != nil {
			t.Fatalf("failed to populate sources")
		}
	}
	return svc, kv.OpPrefix, func() {
		for _, b := range f.Sources {
			if err := svc.DeleteSource(ctx, b.ID); err != nil {
				t.Logf("failed to remove source: %v", err)
			}
		}
	}
}
