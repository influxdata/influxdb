package gorpc_test

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/services/gorpc"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c gorpc.Config
	if _, err := toml.Decode(`
enabled = true
network = "unix"
laddr = "/tmp/influxdb.sock"
auth-enabled = true
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Enabled != true {
		t.Fatalf("unexpected enabled: %v", c.Enabled)
	} else if c.Network != "unix" {
		t.Fatalf("unexpected netwrok: %s", c.Network)
	} else if c.Laddr != "/tmp/influxdb.sock" {
		t.Fatalf("unexpected local address: %s", c.Laddr)
	} else if c.AuthEnabled != true {
		t.Fatalf("unexpected auth-enabled: %v", c.AuthEnabled)
	}
}
