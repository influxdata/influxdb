package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltIDGenerator(t *testing.T) {
	influxdbtesting.ID(initBoltIDGenerator, t)
}

func TestInmemIDGenerator(t *testing.T) {
	influxdbtesting.ID(initInmemIDGenerator, t)
}

func initBoltIDGenerator(f influxdbtesting.IDFields, t *testing.T) (influxdb.IDGenerator, string, func()) {
	s, closeBolt, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initIDGenerator(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initInmemIDGenerator(f influxdbtesting.IDFields, t *testing.T) (influxdb.IDGenerator, string, func()) {
	s, closeBolt, err := NewTestInmemStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initIDGenerator(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initIDGenerator(s kv.Store, f influxdbtesting.IDFields, t *testing.T) (influxdb.IDGenerator, string, func()) {
	svc := kv.NewService(s)
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing service: %v", err)
	}

	return svc, kv.OpPrefix, func() {}
}
