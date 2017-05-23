package bloom_test

import (
	"testing"

	"github.com/influxdata/influxdb/pkg/bloom"
)

// Ensure filter can insert values and verify they exist.
func TestFilter_InsertContains(t *testing.T) {
	f := bloom.NewFilter(1000, 4)

	// Insert value and validate.
	f.Insert([]byte("Bess"))
	if !f.Contains([]byte("Bess")) {
		t.Fatal("expected true")
	}

	// Insert another value and test.
	f.Insert([]byte("Emma"))
	if !f.Contains([]byte("Emma")) {
		t.Fatal("expected true")
	}

	// Validate that a non-existent value doesn't exist.
	if f.Contains([]byte("Jane")) {
		t.Fatal("expected false")
	}
}
