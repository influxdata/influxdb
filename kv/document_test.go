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
