package retention_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/services/retention"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c retention.Config
	if _, err := toml.Decode(`
enabled = true
check-interval = "1s"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Enabled != true {
		t.Fatalf("unexpected enabled state: %v", c.Enabled)
	} else if time.Duration(c.CheckInterval) != time.Second {
		t.Fatalf("unexpected check interval: %v", c.CheckInterval)
	}
}

func TestConfig_Validate(t *testing.T) {
	c := retention.NewConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected validation fail from NewConfig: %s", err)
	}

	c = retention.NewConfig()
	c.CheckInterval = 0
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for check-interval = 0, got nil")
	}

	c = retention.NewConfig()
	c.CheckInterval *= -1
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for negative check-interval, got nil")
	}
}
