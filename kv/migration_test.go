package kv_test

import (
	"testing"

	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
	"go.uber.org/zap"
)

func newMigrator(logger *zap.Logger, now influxdbtesting.NowFunc) *kv.Migrator {
	migrator := kv.NewMigrator(logger)
	kv.MigratorSetNow(migrator, now)
	return migrator
}

func Test_Inmem_Migrator(t *testing.T) {
	influxdbtesting.Migrator(t, inmem.NewKVStore(), newMigrator)
}

func Test_Bolt_Migrator(t *testing.T) {
	store, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	defer closeBolt()

	influxdbtesting.Migrator(t, store, newMigrator)
}
