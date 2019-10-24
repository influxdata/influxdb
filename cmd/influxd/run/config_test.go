package run_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/cmd/influxd/run"
	influxtoml "github.com/influxdata/influxdb/toml"
	"go.uber.org/zap/zapcore"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// Ensure the configuration can be parsed.
func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c run.Config
	if err := c.FromToml(`
[meta]
dir = "/tmp/meta"

[data]
dir = "/tmp/data"

[coordinator]

[http]
bind-address = ":8087"

[[graphite]]
protocol = "udp"

[[graphite]]
protocol = "tcp"

[[collectd]]
bind-address = ":1000"

[[collectd]]
bind-address = ":1010"

[[opentsdb]]
bind-address = ":2000"

[[opentsdb]]
bind-address = ":2010"

[[opentsdb]]
bind-address = ":2020"

[[udp]]
bind-address = ":4444"

[monitoring]
enabled = true

[subscriber]
enabled = true

[continuous_queries]
enabled = true

[tls]
ciphers = ["cipher"]
`); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Meta.Dir != "/tmp/meta" {
		t.Fatalf("unexpected meta dir: %s", c.Meta.Dir)
	} else if c.Data.Dir != "/tmp/data" {
		t.Fatalf("unexpected data dir: %s", c.Data.Dir)
	} else if c.HTTPD.BindAddress != ":8087" {
		t.Fatalf("unexpected api bind address: %s", c.HTTPD.BindAddress)
	} else if len(c.GraphiteInputs) != 2 {
		t.Fatalf("unexpected graphiteInputs count: %d", len(c.GraphiteInputs))
	} else if c.GraphiteInputs[0].Protocol != "udp" {
		t.Fatalf("unexpected graphite protocol(0): %s", c.GraphiteInputs[0].Protocol)
	} else if c.GraphiteInputs[1].Protocol != "tcp" {
		t.Fatalf("unexpected graphite protocol(1): %s", c.GraphiteInputs[1].Protocol)
	} else if c.CollectdInputs[0].BindAddress != ":1000" {
		t.Fatalf("unexpected collectd bind address: %s", c.CollectdInputs[0].BindAddress)
	} else if c.CollectdInputs[1].BindAddress != ":1010" {
		t.Fatalf("unexpected collectd bind address: %s", c.CollectdInputs[1].BindAddress)
	} else if c.OpenTSDBInputs[0].BindAddress != ":2000" {
		t.Fatalf("unexpected opentsdb bind address: %s", c.OpenTSDBInputs[0].BindAddress)
	} else if c.OpenTSDBInputs[1].BindAddress != ":2010" {
		t.Fatalf("unexpected opentsdb bind address: %s", c.OpenTSDBInputs[1].BindAddress)
	} else if c.OpenTSDBInputs[2].BindAddress != ":2020" {
		t.Fatalf("unexpected opentsdb bind address: %s", c.OpenTSDBInputs[2].BindAddress)
	} else if c.UDPInputs[0].BindAddress != ":4444" {
		t.Fatalf("unexpected udp bind address: %s", c.UDPInputs[0].BindAddress)
	} else if !c.Subscriber.Enabled {
		t.Fatalf("unexpected subscriber enabled: %v", c.Subscriber.Enabled)
	} else if !c.ContinuousQuery.Enabled {
		t.Fatalf("unexpected continuous query enabled: %v", c.ContinuousQuery.Enabled)
	} else if c.TLS.Ciphers[0] != "cipher" {
		t.Fatalf("unexpected tls: %q", c.TLS.Ciphers)
	}
}

// Ensure the configuration can be parsed.
func TestConfig_Parse_EnvOverride(t *testing.T) {
	// Parse configuration.
	var c run.Config
	if _, err := toml.Decode(`
[meta]
dir = "/tmp/meta"

[data]
dir = "/tmp/data"

[coordinator]

[admin]
bind-address = ":8083"

[http]
bind-address = ":8087"

[[graphite]]
protocol = "udp"
templates = [
  "default.* .template.in.config"
]

[[graphite]]
protocol = "tcp"

[[collectd]]
bind-address = ":1000"

[[collectd]]
bind-address = ":1010"

[[opentsdb]]
bind-address = ":2000"

[[opentsdb]]
bind-address = ":2010"

[[udp]]
bind-address = ":4444"

[[udp]]

[monitoring]
enabled = true

[continuous_queries]
enabled = true

[tls]
min-version = "tls1.0"
`, &c); err != nil {
		t.Fatal(err)
	}

	getenv := func(s string) string {
		switch s {
		case "INFLUXDB_UDP_BIND_ADDRESS":
			return ":1234"
		case "INFLUXDB_UDP_0_BIND_ADDRESS":
			return ":5555"
		case "INFLUXDB_GRAPHITE_0_TEMPLATES_0":
			return "override.* .template.0"
		case "INFLUXDB_GRAPHITE_1_TEMPLATES":
			return "override.* .template.1.1,override.* .template.1.2"
		case "INFLUXDB_GRAPHITE_1_PROTOCOL":
			return "udp"
		case "INFLUXDB_COLLECTD_1_BIND_ADDRESS":
			return ":1020"
		case "INFLUXDB_OPENTSDB_0_BIND_ADDRESS":
			return ":2020"
		case "INFLUXDB_DATA_CACHE_MAX_MEMORY_SIZE":
			// uint64 type
			return "1000"
		case "INFLUXDB_LOGGING_LEVEL":
			// logging type
			return "warn"
		case "INFLUXDB_COORDINATOR_QUERY_TIMEOUT":
			// duration type
			return "1m"
		case "INFLUXDB_TLS_MIN_VERSION":
			return "tls1.2"
		}
		return ""
	}

	if err := c.ApplyEnvOverrides(getenv); err != nil {
		t.Fatalf("failed to apply env overrides: %v", err)
	}

	if c.UDPInputs[0].BindAddress != ":5555" {
		t.Fatalf("unexpected udp bind address: %s", c.UDPInputs[0].BindAddress)
	}

	if c.UDPInputs[1].BindAddress != ":1234" {
		t.Fatalf("unexpected udp bind address: %s", c.UDPInputs[1].BindAddress)
	}

	if len(c.GraphiteInputs[0].Templates) != 1 || c.GraphiteInputs[0].Templates[0] != "override.* .template.0" {
		t.Fatalf("unexpected graphite 0 templates: %+v", c.GraphiteInputs[0].Templates)
	}

	if len(c.GraphiteInputs[1].Templates) != 2 || c.GraphiteInputs[1].Templates[1] != "override.* .template.1.2" {
		t.Fatalf("unexpected graphite 1 templates: %+v", c.GraphiteInputs[1].Templates)
	}

	if c.GraphiteInputs[1].Protocol != "udp" {
		t.Fatalf("unexpected graphite protocol: %s", c.GraphiteInputs[1].Protocol)
	}

	if c.CollectdInputs[1].BindAddress != ":1020" {
		t.Fatalf("unexpected collectd bind address: %s", c.CollectdInputs[1].BindAddress)
	}

	if c.OpenTSDBInputs[0].BindAddress != ":2020" {
		t.Fatalf("unexpected opentsdb bind address: %s", c.OpenTSDBInputs[0].BindAddress)
	}

	if c.Data.CacheMaxMemorySize != 1000 {
		t.Fatalf("unexpected cache max memory size: %v", c.Data.CacheMaxMemorySize)
	}

	if c.Logging.Level != zapcore.WarnLevel {
		t.Fatalf("unexpected logging level: %v", c.Logging.Level)
	}

	if c.Coordinator.QueryTimeout != influxtoml.Duration(time.Minute) {
		t.Fatalf("unexpected query timeout: %v", c.Coordinator.QueryTimeout)
	}

	if c.TLS.MinVersion != "tls1.2" {
		t.Fatalf("unexpected tls min version: %q", c.TLS.MinVersion)
	}
}

func TestConfig_ValidateNoServiceConfigured(t *testing.T) {
	var c run.Config
	if _, err := toml.Decode(`
[meta]
enabled = false

[data]
enabled = false
`, &c); err != nil {
		t.Fatal(err)
	}

	if e := c.Validate(); e == nil {
		t.Fatalf("got nil, expected error")
	}
}

func TestConfig_ValidateMonitorStore_MetaOnly(t *testing.T) {
	c := run.NewConfig()
	if _, err := toml.Decode(`
[monitor]
store-enabled = true

[meta]
dir = "foo"

[data]
enabled = false
`, &c); err != nil {
		t.Fatal(err)
	}

	if err := c.Validate(); err == nil {
		t.Fatalf("got nil, expected error")
	}
}

func TestConfig_DeprecatedOptions(t *testing.T) {
	// Parse configuration.
	var c run.Config
	if err := c.FromToml(`
[cluster]
max-select-point = 100
`); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Coordinator.MaxSelectPointN != 100 {
		t.Fatalf("unexpected coordinator max select points: %d", c.Coordinator.MaxSelectPointN)

	}
}

// Ensure that Config.Validate correctly validates the individual subsections.
func TestConfig_InvalidSubsections(t *testing.T) {
	// Precondition: NewDemoConfig must validate correctly.
	c, err := run.NewDemoConfig()
	if err != nil {
		t.Fatalf("error creating demo config: %s", err)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("new demo config failed validation: %s", err)
	}

	// For each subsection, load a config with a single invalid setting.
	for _, tc := range []struct {
		section string
		kv      string
	}{
		{"meta", `dir = ""`},
		{"data", `dir = ""`},
		{"monitor", `store-database = ""`},
		{"continuous_queries", `run-interval = "0s"`},
		{"subscriber", `http-timeout = "0s"`},
		{"retention", `check-interval = "0s"`},
		{"shard-precreation", `advance-period = "0s"`},
	} {
		c, err := run.NewDemoConfig()
		if err != nil {
			t.Fatalf("error creating demo config: %s", err)
		}

		s := fmt.Sprintf("\n[%s]\n%s\n", tc.section, tc.kv)
		if err := c.FromToml(s); err != nil {
			t.Fatalf("error loading toml %q: %s", s, err)
		}

		if err := c.Validate(); err == nil {
			t.Fatalf("expected error but got nil for config: %s", s)
		}
	}
}

// Ensure the configuration can be parsed when a Byte-Order-Mark is present.
func TestConfig_Parse_UTF8_ByteOrderMark(t *testing.T) {
	// Parse configuration.
	var c run.Config
	f, err := ioutil.TempFile("", "influxd")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	f.WriteString("\ufeff")
	f.WriteString(`
[meta]
dir = "/tmp/meta"

[data]
dir = "/tmp/data"

[coordinator]

[http]
bind-address = ":8087"

[[graphite]]
protocol = "udp"

[[graphite]]
protocol = "tcp"

[[collectd]]
bind-address = ":1000"

[[collectd]]
bind-address = ":1010"

[[opentsdb]]
bind-address = ":2000"

[[opentsdb]]
bind-address = ":2010"

[[opentsdb]]
bind-address = ":2020"

[[udp]]
bind-address = ":4444"

[monitoring]
enabled = true

[subscriber]
enabled = true

[continuous_queries]
enabled = true
`)
	if err := c.FromTomlFile(f.Name()); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Meta.Dir != "/tmp/meta" {
		t.Fatalf("unexpected meta dir: %s", c.Meta.Dir)
	} else if c.Data.Dir != "/tmp/data" {
		t.Fatalf("unexpected data dir: %s", c.Data.Dir)
	} else if c.HTTPD.BindAddress != ":8087" {
		t.Fatalf("unexpected api bind address: %s", c.HTTPD.BindAddress)
	} else if len(c.GraphiteInputs) != 2 {
		t.Fatalf("unexpected graphiteInputs count: %d", len(c.GraphiteInputs))
	} else if c.GraphiteInputs[0].Protocol != "udp" {
		t.Fatalf("unexpected graphite protocol(0): %s", c.GraphiteInputs[0].Protocol)
	} else if c.GraphiteInputs[1].Protocol != "tcp" {
		t.Fatalf("unexpected graphite protocol(1): %s", c.GraphiteInputs[1].Protocol)
	} else if c.CollectdInputs[0].BindAddress != ":1000" {
		t.Fatalf("unexpected collectd bind address: %s", c.CollectdInputs[0].BindAddress)
	} else if c.CollectdInputs[1].BindAddress != ":1010" {
		t.Fatalf("unexpected collectd bind address: %s", c.CollectdInputs[1].BindAddress)
	} else if c.OpenTSDBInputs[0].BindAddress != ":2000" {
		t.Fatalf("unexpected opentsdb bind address: %s", c.OpenTSDBInputs[0].BindAddress)
	} else if c.OpenTSDBInputs[1].BindAddress != ":2010" {
		t.Fatalf("unexpected opentsdb bind address: %s", c.OpenTSDBInputs[1].BindAddress)
	} else if c.OpenTSDBInputs[2].BindAddress != ":2020" {
		t.Fatalf("unexpected opentsdb bind address: %s", c.OpenTSDBInputs[2].BindAddress)
	} else if c.UDPInputs[0].BindAddress != ":4444" {
		t.Fatalf("unexpected udp bind address: %s", c.UDPInputs[0].BindAddress)
	} else if !c.Subscriber.Enabled {
		t.Fatalf("unexpected subscriber enabled: %v", c.Subscriber.Enabled)
	} else if !c.ContinuousQuery.Enabled {
		t.Fatalf("unexpected continuous query enabled: %v", c.ContinuousQuery.Enabled)
	}
}

// Ensure the configuration can be parsed when a Byte-Order-Mark is present.
func TestConfig_Parse_UTF16_ByteOrderMark(t *testing.T) {
	// Parse configuration.
	var c run.Config
	f, err := ioutil.TempFile("", "influxd")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	utf16 := unicode.UTF16(unicode.BigEndian, unicode.UseBOM)
	w := transform.NewWriter(f, utf16.NewEncoder())
	io.WriteString(w, `
[meta]
dir = "/tmp/meta"

[data]
dir = "/tmp/data"

[coordinator]

[http]
bind-address = ":8087"

[[graphite]]
protocol = "udp"

[[graphite]]
protocol = "tcp"

[[collectd]]
bind-address = ":1000"

[[collectd]]
bind-address = ":1010"

[[opentsdb]]
bind-address = ":2000"

[[opentsdb]]
bind-address = ":2010"

[[opentsdb]]
bind-address = ":2020"

[[udp]]
bind-address = ":4444"

[monitoring]
enabled = true

[subscriber]
enabled = true

[continuous_queries]
enabled = true
`)
	if err := c.FromTomlFile(f.Name()); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Meta.Dir != "/tmp/meta" {
		t.Fatalf("unexpected meta dir: %s", c.Meta.Dir)
	} else if c.Data.Dir != "/tmp/data" {
		t.Fatalf("unexpected data dir: %s", c.Data.Dir)
	} else if c.HTTPD.BindAddress != ":8087" {
		t.Fatalf("unexpected api bind address: %s", c.HTTPD.BindAddress)
	} else if len(c.GraphiteInputs) != 2 {
		t.Fatalf("unexpected graphiteInputs count: %d", len(c.GraphiteInputs))
	} else if c.GraphiteInputs[0].Protocol != "udp" {
		t.Fatalf("unexpected graphite protocol(0): %s", c.GraphiteInputs[0].Protocol)
	} else if c.GraphiteInputs[1].Protocol != "tcp" {
		t.Fatalf("unexpected graphite protocol(1): %s", c.GraphiteInputs[1].Protocol)
	} else if c.CollectdInputs[0].BindAddress != ":1000" {
		t.Fatalf("unexpected collectd bind address: %s", c.CollectdInputs[0].BindAddress)
	} else if c.CollectdInputs[1].BindAddress != ":1010" {
		t.Fatalf("unexpected collectd bind address: %s", c.CollectdInputs[1].BindAddress)
	} else if c.OpenTSDBInputs[0].BindAddress != ":2000" {
		t.Fatalf("unexpected opentsdb bind address: %s", c.OpenTSDBInputs[0].BindAddress)
	} else if c.OpenTSDBInputs[1].BindAddress != ":2010" {
		t.Fatalf("unexpected opentsdb bind address: %s", c.OpenTSDBInputs[1].BindAddress)
	} else if c.OpenTSDBInputs[2].BindAddress != ":2020" {
		t.Fatalf("unexpected opentsdb bind address: %s", c.OpenTSDBInputs[2].BindAddress)
	} else if c.UDPInputs[0].BindAddress != ":4444" {
		t.Fatalf("unexpected udp bind address: %s", c.UDPInputs[0].BindAddress)
	} else if !c.Subscriber.Enabled {
		t.Fatalf("unexpected subscriber enabled: %v", c.Subscriber.Enabled)
	} else if !c.ContinuousQuery.Enabled {
		t.Fatalf("unexpected continuous query enabled: %v", c.ContinuousQuery.Enabled)
	}
}
