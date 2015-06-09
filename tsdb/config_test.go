package tsdb

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c Config
	if _, err := toml.Decode(`
dir = "/tmp/foo"
retention-auto-create = true
retention-check-enabled = true
retention-check-period = "45m"
retention-create-period = "10m"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Dir != "/tmp/foo" {
		t.Fatalf("unexpected dir: %s", c.Dir)
	} else if c.RetentionAutoCreate != true {
		t.Fatalf("unexpected retention auto create: %v", c.RetentionAutoCreate)
	} else if c.RetentionCheckEnabled != true {
		t.Fatalf("unexpected retention check enabled: %v", c.RetentionCheckEnabled)
	} else if time.Duration(c.RetentionCheckPeriod) != 45*time.Minute {
		t.Fatalf("unexpected retention check period: %v", c.RetentionCheckPeriod)
	}
}
