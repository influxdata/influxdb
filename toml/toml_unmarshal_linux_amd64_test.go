package toml_test

import (
	"github.com/influxdb/influxdb/toml"
	"testing"
)

// Ensure that gigabyte sizes can be parsed.
func TestSize_UnmarshalText_GB(t *testing.T) {
	var s toml.Size
	if err := s.UnmarshalText([]byte("10g")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if s != 10*(1<<30) {
		t.Fatalf("unexpected size: %d", s)
	}
}
