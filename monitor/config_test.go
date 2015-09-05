package monitor_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/monitor"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c monitor.Config
	if _, err := toml.Decode(`
store-enabled=true
store-database="the_db"
store-retention-policy="the_rp"
store-retention-duration="1h"
store-replication-factor=1234
store-interval="10m"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if !c.StoreEnabled {
		t.Fatalf("unexpected store-enabled: %v", c.StoreEnabled)
	} else if c.StoreDatabase != "the_db" {
		t.Fatalf("unexpected store-database: %s", c.StoreDatabase)
	} else if c.StoreRetentionPolicy != "the_rp" {
		t.Fatalf("unexpected store-retention-policy: %s", c.StoreRetentionPolicy)
	} else if time.Duration(c.StoreRetentionDuration) != 1*time.Hour {
		t.Fatalf("unexpected store-retention-duration: %s", c.StoreRetentionDuration)
	} else if c.StoreReplicationFactor != 1234 {
		t.Fatalf("unexpected store-replication-factor: %d", c.StoreReplicationFactor)
	} else if time.Duration(c.StoreInterval) != 10*time.Minute {
		t.Fatalf("unexpected store-interval:  %s", c.StoreInterval)
	}
}
