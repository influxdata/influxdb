package continuous_querier_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/services/continuous_querier"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c continuous_querier.Config
	if _, err := toml.Decode(`
run-interval = "1m"
enabled = true
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if time.Duration(c.RunInterval) != time.Minute {
		t.Fatalf("unexpected run interval: %v", c.RunInterval)
	} else if c.Enabled != true {
		t.Fatalf("unexpected enabled: %v", c.Enabled)
	}
}

func TestConfig_Validate(t *testing.T) {
	c := continuous_querier.NewConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected validation fail from NewConfig: %s", err)
	}

	c = continuous_querier.NewConfig()
	c.RunInterval = 0
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for run-interval = 0, got nil")
	}

	c = continuous_querier.NewConfig()
	c.RunInterval *= -1
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for negative run-interval, got nil")
	}
}
