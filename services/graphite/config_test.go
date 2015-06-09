package graphite_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/services/graphite"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c graphite.Config
	if _, err := toml.Decode(`
bind-address = ":8080"
database = "mydb"
enabled = true
protocol = "tcp"
name-position = "first"
name-separator = "."
batch-size=100
batch-timeout="1s"
consistency-level="one"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.BindAddress != ":8080" {
		t.Fatalf("unexpected bind address: %s", c.BindAddress)
	} else if c.Database != "mydb" {
		t.Fatalf("unexpected database selected: %s", c.Database)
	} else if c.Enabled != true {
		t.Fatalf("unexpected graphite enabled: %v", c.Enabled)
	} else if c.Protocol != "tcp" {
		t.Fatalf("unexpected graphite protocol: %s", c.Protocol)
	} else if c.NamePosition != "first" {
		t.Fatalf("unexpected graphite name position: %s", c.NamePosition)
	} else if c.NameSeparator != "." {
		t.Fatalf("unexpected graphite name separator: %s", c.NameSeparator)
	} else if c.BatchSize != 100 {
		t.Fatalf("unexpected graphite batch size: %d", c.BatchSize)
	} else if time.Duration(c.BatchTimeout) != time.Second {
		t.Fatalf("unexpected graphite batch timeout: %v", c.BatchTimeout)
	} else if c.ConsistencyLevel != "one" {
		t.Fatalf("unexpected graphite consistency setting: %s", c.ConsistencyLevel)
	}
}
