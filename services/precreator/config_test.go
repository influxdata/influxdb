package precreator_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/services/precreator"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c precreator.Config
	if _, err := toml.Decode(`
enabled = true
check-interval = "2m"
advance-period = "10m"
`, &c); err != nil {

		t.Fatal(err)
	}

	// Validate configuration.
	if !c.Enabled {
		t.Fatalf("unexpected enabled state: %v", c.Enabled)
	} else if time.Duration(c.CheckInterval) != 2*time.Minute {
		t.Fatalf("unexpected check interval: %s", c.CheckInterval)
	} else if time.Duration(c.AdvancePeriod) != 10*time.Minute {
		t.Fatalf("unexpected advance period: %s", c.AdvancePeriod)
	}
}

func TestConfig_Validate(t *testing.T) {
	c := precreator.NewConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected validation fail from NewConfig: %s", err)
	}

	c = precreator.NewConfig()
	c.CheckInterval = 0
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for check-interval = 0, got nil")
	}

	c = precreator.NewConfig()
	c.CheckInterval *= -1
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for negative check-interval, got nil")
	}

	c = precreator.NewConfig()
	c.AdvancePeriod = 0
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for advance-period = 0, got nil")
	}

	c = precreator.NewConfig()
	c.AdvancePeriod *= -1
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for negative advance-period, got nil")
	}

	c.Enabled = false
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected validation fail from disabled config: %s", err)
	}
}
