package kv_test

import (
	"testing"

	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestBoltDocumentStore(t *testing.T) {
	boltStore, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new bolt kv store: %v", err)
	}
	defer closeBolt()

	t.Run("bolt", influxdbtesting.NewDocumentIntegrationTest(boltStore))
}

func TestInmemDocumentStore(t *testing.T) {
	t.Skip("https://github.com/influxdata/influxdb/issues/12403")
	inmemStore, closeInmem, err := NewTestInmemStore(t)
	if err != nil {
		t.Fatalf("failed to create new inmem kv store: %v", err)
	}
	defer closeInmem()

	t.Run("inmem", influxdbtesting.NewDocumentIntegrationTest(inmemStore))

}
