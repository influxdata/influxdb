package admin_test

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/services/admin"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c admin.Config
	if _, err := toml.Decode(`
enabled = true
bind-address = ":8083"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Enabled != true {
		t.Fatalf("unexpected enabled: %v", c.Enabled)
	} else if c.BindAddress != ":8083" {
		t.Fatalf("unexpected bind address: %s", c.BindAddress)
	}
}
