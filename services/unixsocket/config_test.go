package unixsocket_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/services/unixsocket"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c unixsocket.Config
	if _, err := toml.Decode(`
enabled = true
bind-socket = "/tmp/influxdb.sock"
database = "awesomedb"
retention-policy = "awesomerp"
batch-size = 100
batch-pending = 9
batch-timeout = "10ms"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Enabled != true {
		t.Fatalf("unexpected enabled: %v", c.Enabled)
	} else if c.BindSocket != "/tmp/influxdb.sock" {
		t.Fatalf("unexpected bind address: %s", c.BindSocket)
	} else if c.Database != "awesomedb" {
		t.Fatalf("unexpected database: %s", c.Database)
	} else if c.RetentionPolicy != "awesomerp" {
		t.Fatalf("unexpected retention policy: %s", c.RetentionPolicy)
	} else if c.BatchSize != 100 {
		t.Fatalf("unexpected batch size: %d", c.BatchSize)
	} else if c.BatchPending != 9 {
		t.Fatalf("unexpected batch pending: %d", c.BatchPending)
	} else if time.Duration(c.BatchTimeout) != (10 * time.Millisecond) {
		t.Fatalf("unexpected batch timeout: %v", c.BatchTimeout)
	}
}
