package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltKeyValueLog(t *testing.T) {
	influxdbtesting.KeyValueLog(initBoltKeyValueLog, t)
}

func TestInmemKeyValueLog(t *testing.T) {
	influxdbtesting.KeyValueLog(initInmemKeyValueLog, t)
}

func initBoltKeyValueLog(f influxdbtesting.KeyValueLogFields, t *testing.T) (influxdb.KeyValueLog, func()) {
	s, closeBolt, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initKeyValueLog(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initInmemKeyValueLog(f influxdbtesting.KeyValueLogFields, t *testing.T) (influxdb.KeyValueLog, func()) {
	s, closeBolt, err := NewTestInmemStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initKeyValueLog(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initKeyValueLog(s kv.Store, f influxdbtesting.KeyValueLogFields, t *testing.T) (influxdb.KeyValueLog, func()) {
	svc := kv.NewService(s)

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing organization service: %v", err)
	}

	for _, e := range f.LogEntries {
		if err := svc.AddLogEntry(ctx, e.Key, e.Value, e.Time); err != nil {
			t.Fatalf("failed to populate log entries")
		}
	}
	return svc, func() {
	}
}
