package pool_test

import (
	"testing"

	"github.com/influxdata/influxdb/pkg/pool"
)

func TestLimitedBytePool_Put_MaxSize(t *testing.T) {
	bp := pool.NewLimitedBytes(1, 10)
	bp.Put(make([]byte, 1024)) // should be dropped

	if got, exp := cap(bp.Get(10)), 10; got != exp {
		t.Fatalf("max cap size exceeded: got %v, exp %v", got, exp)
	}
}
