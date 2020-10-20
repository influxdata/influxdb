package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltKeyValueLog(t *testing.T) {
	influxdbtesting.KeyValueLog(initBoltKeyValueLog, t)
}

func initBoltKeyValueLog(f influxdbtesting.KeyValueLogFields, t *testing.T) (influxdb.KeyValueLog, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initKeyValueLog(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initKeyValueLog(s kv.SchemaStore, f influxdbtesting.KeyValueLogFields, t *testing.T) (influxdb.KeyValueLog, func()) {
	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s, tenant.NewService(tenant.NewStore(s)))

	for _, e := range f.LogEntries {
		if err := svc.AddLogEntry(ctx, e.Key, e.Value, e.Time); err != nil {
			t.Fatalf("failed to populate log entries")
		}
	}
	return svc, func() {
	}
}
