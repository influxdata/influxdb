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
name-schema= "measurement"
ignore-unnamed = true
name-separator = "."
batch-size=100
batch-timeout="1s"
consistency-level="one"
templates=["servers.* .host.measurement*"]
tags=["region=us-east"]
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
	} else if c.NameSchema != "measurement" {
		t.Fatalf("unexpected graphite name schema: %s", c.NameSchema)
	} else if c.IgnoreUnnamed != true {
		t.Fatalf("unexpected ignore-unnamed: %v", c.IgnoreUnnamed)
	} else if c.NameSeparator != "." {
		t.Fatalf("unexpected graphite name separator: %s", c.NameSeparator)
	} else if c.BatchSize != 100 {
		t.Fatalf("unexpected graphite batch size: %d", c.BatchSize)
	} else if time.Duration(c.BatchTimeout) != time.Second {
		t.Fatalf("unexpected graphite batch timeout: %v", c.BatchTimeout)
	} else if c.ConsistencyLevel != "one" {
		t.Fatalf("unexpected graphite consistency setting: %s", c.ConsistencyLevel)
	}

	if len(c.Templates) != 1 && c.Templates[0] != "servers.* .host.measurement*" {
		t.Fatalf("unexpected graphite templates setting: %s", c.ConsistencyLevel)
	}
	if len(c.Tags) != 1 && c.Tags[0] != "regsion=us-east" {
		t.Fatalf("unexpected graphite templates setting: %s", c.ConsistencyLevel)
	}
}
