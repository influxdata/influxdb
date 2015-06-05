package hh_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/services/hh"
)

func TestConfigParse(t *testing.T) {
	// Parse configuration.
	var c hh.Config
	if _, err := toml.Decode(`
enabled = false
retry-interval = "10m"
max-backoff-time = "20m"
max-size=2048
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if exp := true; c.Enabled == true {
		t.Fatalf("unexpected enabled: got %v, exp %v", c.Enabled, exp)
	}

	if exp := 10 * time.Minute; c.RetryInterval.String() != exp.String() {
		t.Fatalf("unexpected retry interval: got %v, exp %v", c.RetryInterval, exp)
	}

	if exp := 20 * time.Minute; c.MaxBackoffTime.String() != exp.String() {
		t.Fatalf("unexpected max backoff time: got %v, exp %v", c.MaxBackoffTime, exp)
	}

	if exp := int64(2048); c.MaxSize != exp {
		t.Fatalf("unexpected retry interval: got %v, exp %v", c.MaxSize, exp)
	}
}
