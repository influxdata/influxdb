package registration_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/services/registration"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c registration.Config
	if _, err := toml.Decode(`
enabled = true
url = "a.b.c"
token = "1234"
stats-interval = "1s"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Enabled != true {
		t.Fatalf("unexpected enabled state: %v", c.Enabled)
	} else if c.URL != "a.b.c" {
		t.Fatalf("unexpected Enterprise URL: %s", c.URL)
	} else if c.Token != "1234" {
		t.Fatalf("unexpected Enterprise URL: %s", c.URL)
	} else if time.Duration(c.StatsInterval) != time.Second {
		t.Fatalf("unexpected stats interval: %v", c.StatsInterval)
	}
}
