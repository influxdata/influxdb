package tsdb_test

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/tsdb"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c tsdb.Config
	if _, err := toml.Decode(`
enabled = false
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Enabled == true {
		t.Fatalf("unexpected enabled: %v", c.Enabled)
	}
	// TODO: add remaining config tests
}
