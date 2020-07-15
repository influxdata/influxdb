package http

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"go.uber.org/zap/zaptest"
)

func NewTestInmemStore(t *testing.T) kv.Store {
	t.Helper()

	store := inmem.NewKVStore()

	if err := all.Up(context.Background(), zaptest.NewLogger(t), store); err != nil {
		t.Fatal(err)
	}

	return store
}
