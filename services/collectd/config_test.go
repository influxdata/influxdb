package collectd_test

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/services/collectd"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c collectd.Config
	if _, err := toml.Decode(`
enabled = true
bind-address = ":9000"
database = "xxx"
typesdb = "yyy"
typesdb-dirs = [ "/path/to/types.db", "/path/to/custom/types.db" ]
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Enabled != true {
		t.Fatalf("unexpected enabled: %v", c.Enabled)
	} else if c.BindAddress != ":9000" {
		t.Fatalf("unexpected bind address: %s", c.BindAddress)
	} else if c.Database != "xxx" {
		t.Fatalf("unexpected database: %s", c.Database)
	} else if c.TypesDB != "yyy" {
		t.Fatalf("unexpected types db: %s", c.TypesDB)
	} else if len(c.TypesDBDirs) != 2 {
		t.Fatalf("unexpected types db dirs size: %d", len(c.TypesDBDirs))
	} else if c.TypesDBDirs[0] != "/path/to/types.db" {
		t.Fatalf("unexpected types db dir [0]: %s", c.TypesDBDirs[0])
	} else if c.TypesDBDirs[1] != "/path/to/custom/types.db" {
		t.Fatalf("unexpected types db dir [1]: %s", c.TypesDBDirs[1])
	}
}
