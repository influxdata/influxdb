package enterprise_test

import (
	"testing"
	"time"

	"github.com/influxdata/config"
	"github.com/influxdb/influxdb/services/enterprise"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c enterprise.Config
	if err := config.Decode(`
enabled = true
stats-interval = "1s"
admin-port = 4815

[[hosts]]
url = "a.b.c"
primary = true
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Enabled != true {
		t.Fatalf("unexpected enabled state: %v", c.Enabled)
	} else if c.Hosts[0].URL != "a.b.c" {
		t.Fatalf("unexpected Enterprise URL: %s", c.Hosts[0].URL)
	} else if time.Duration(c.StatsInterval) != time.Second {
		t.Fatalf("unexpected stats interval: %v", c.StatsInterval)
	} else if c.Hosts[0].Primary != true {
		t.Fatalf("unexpected Enterprise host primary state: %s", c.Hosts[0].Primary)
	} else if c.AdminPort != 4815 {
		t.Fatalf("unexpected Enterprise admin interface port: %s", c.AdminPort)
	}
}
