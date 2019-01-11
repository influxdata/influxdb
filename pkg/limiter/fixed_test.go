package limiter_test

import (
	"testing"

	"github.com/influxdata/influxdb/pkg/limiter"
)

func TestFixed_Available(t *testing.T) {
	f := limiter.NewFixed(10)
	if exp, got := 10, f.Available(); exp != got {
		t.Fatalf("available mismatch: exp %v, got %v", exp, got)
	}

	f.Take()

	if exp, got := 9, f.Available(); exp != got {
		t.Fatalf("available mismatch: exp %v, got %v", exp, got)
	}

	f.Release()

	if exp, got := 10, f.Available(); exp != got {
		t.Fatalf("available mismatch: exp %v, got %v", exp, got)
	}
}
