package kv_test

import (
	"testing"

	"github.com/influxdata/influxdb/inmem"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func Test_Inmem_Index(t *testing.T) {
	influxdbtesting.TestIndex(t, inmem.NewKVStore())
}

func Test_Bolt_Index(t *testing.T) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	defer closeBolt()

	influxdbtesting.TestIndex(t, s)
}
