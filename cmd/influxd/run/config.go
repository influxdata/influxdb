package run

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/continuous_querier"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/opentsdb"
	"github.com/influxdata/influxdb/services/precreator"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/services/subscriber"
	"github.com/influxdata/influxdb/services/udp"
	itoml "github.com/influxdata/influxdb/toml"
	"github.com/influxdata/influxdb/tsdb"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

const (
	// DefaultBindAddress is the default address for various RPC services.
	DefaultBindAddress = "127.0.0.1:8088"
)

// Config represents the configuration format for the influxd binary.
type Config struct {
	Meta        *meta.Config       `toml:"meta"`
	Data        tsdb.Config        `toml:"data"`
	Coordinator coordinator.Config `toml:"coordinator"`
	Retention   retention.Config   `toml:"retention"`
	Precreator  precreator.Config  `toml:"shard-precreation"`

	Monitor        monitor.Config    `toml:"monitor"`
	Subscriber     subscriber.Config `toml:"subscriber"`
	HTTPD          httpd.Config      `toml:"http"`
	Logging        logger.Config     `toml:"logging"`
	GraphiteInputs []graphite.Config `toml:"graphite"`
	CollectdInputs []collectd.Config `toml:"collectd"`
	OpenTSDBInputs []opentsdb.Config `toml:"opentsdb"`
	UDPInputs      []udp.Config      `toml:"udp"`

	ContinuousQuery continuous_querier.Config `toml:"continuous_queries"`

	// Server reporting
	ReportingDisabled bool `toml:"reporting-disabled"`

	// BindAddress is the address that all TCP services use (Raft, Snapshot, Cluster, etc.)
	BindAddress string `toml:"bind-address"`

	// TLS provides configuration options for all https endpoints.
	TLS tlsconfig.Config `toml:"tls"`
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {
	c := &Config{}
	c.Meta = meta.NewConfig()
	c.Data = tsdb.NewConfig()
	c.Coordinator = coordinator.NewConfig()
	c.Precreator = precreator.NewConfig()

	c.Monitor = monitor.NewConfig()
	c.Subscriber = subscriber.NewConfig()
	c.HTTPD = httpd.NewConfig()
	c.Logging = logger.NewConfig()

	c.GraphiteInputs = []graphite.Config{graphite.NewConfig()}
	c.CollectdInputs = []collectd.Config{collectd.NewConfig()}
	c.OpenTSDBInputs = []opentsdb.Config{opentsdb.NewConfig()}
	c.UDPInputs = []udp.Config{udp.NewConfig()}

	c.ContinuousQuery = continuous_querier.NewConfig()
	c.Retention = retention.NewConfig()
	c.BindAddress = DefaultBindAddress

	return c
}

// NewDemoConfig returns the config that runs when no config is specified.
func NewDemoConfig() (*Config, error) {
	c := NewConfig()

	var homeDir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err == nil {
		homeDir = u.HomeDir
	} else if os.Getenv("HOME") != "" {
		homeDir = os.Getenv("HOME")
	} else {
		return nil, fmt.Errorf("failed to determine current user for storage")
	}

	c.Meta.Dir = filepath.Join(homeDir, ".influxdb/meta")
	c.Data.Dir = filepath.Join(homeDir, ".influxdb/data")
	c.Data.WALDir = filepath.Join(homeDir, ".influxdb/wal")

	return c, nil
}

// FromTomlFile loads the config from a TOML file.
func (c *Config) FromTomlFile(fpath string) error {
	bs, err := ioutil.ReadFile(fpath)
	if err != nil {
		return err
	}

	// Handle any potential Byte-Order-Marks that may be in the config file.
	// This is for Windows compatibility only.
	// See https://github.com/influxdata/telegraf/issues/1378 and
	// https://github.com/influxdata/influxdb/issues/8965.
	bom := unicode.BOMOverride(transform.Nop)
	bs, _, err = transform.Bytes(bom, bs)
	if err != nil {
		return err
	}
	return c.FromToml(string(bs))
}

// FromToml loads the config from TOML.
func (c *Config) FromToml(input string) error {
	// Replace deprecated [cluster] with [coordinator]
	re := regexp.MustCompile(`(?m)^\s*\[cluster\]`)
	input = re.ReplaceAllStringFunc(input, func(in string) string {
		in = strings.TrimSpace(in)
		out := "[coordinator]"
		log.Printf("deprecated config option %s replaced with %s; %s will not be supported in a future release\n", in, out, in)
		return out
	})

	_, err := toml.Decode(input, c)
	return err
}

// Validate returns an error if the config is invalid.
func (c *Config) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	if err := c.Data.Validate(); err != nil {
		return err
	}

	if err := c.Monitor.Validate(); err != nil {
		return err
	}

	if err := c.ContinuousQuery.Validate(); err != nil {
		return err
	}

	if err := c.Retention.Validate(); err != nil {
		return err
	}

	if err := c.Precreator.Validate(); err != nil {
		return err
	}

	if err := c.Subscriber.Validate(); err != nil {
		return err
	}

	for _, graphite := range c.GraphiteInputs {
		if err := graphite.Validate(); err != nil {
			return fmt.Errorf("invalid graphite config: %v", err)
		}
	}

	for _, collectd := range c.CollectdInputs {
		if err := collectd.Validate(); err != nil {
			return fmt.Errorf("invalid collectd config: %v", err)
		}
	}

	if err := c.TLS.Validate(); err != nil {
		return err
	}

	return nil
}

// ApplyEnvOverrides apply the environment configuration on top of the config.
func (c *Config) ApplyEnvOverrides(getenv func(string) string) error {
	return itoml.ApplyEnvOverrides(getenv, "INFLUXDB", c)
}

// Diagnostics returns a diagnostics representation of Config.
func (c *Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	return diagnostics.RowFromMap(map[string]interface{}{
		"reporting-disabled": c.ReportingDisabled,
		"bind-address":       c.BindAddress,
	}), nil
}

func (c *Config) diagnosticsClients() map[string]diagnostics.Client {
	// Config settings that are always present.
	m := map[string]diagnostics.Client{
		"config": c,

		"config-data":        c.Data,
		"config-meta":        c.Meta,
		"config-coordinator": c.Coordinator,
		"config-retention":   c.Retention,
		"config-precreator":  c.Precreator,

		"config-monitor":    c.Monitor,
		"config-subscriber": c.Subscriber,
		"config-httpd":      c.HTTPD,

		"config-cqs": c.ContinuousQuery,
	}

	// Config settings that can be repeated and can be disabled.
	if g := graphite.Configs(c.GraphiteInputs); g.Enabled() {
		m["config-graphite"] = g
	}
	if cc := collectd.Configs(c.CollectdInputs); cc.Enabled() {
		m["config-collectd"] = cc
	}
	if t := opentsdb.Configs(c.OpenTSDBInputs); t.Enabled() {
		m["config-opentsdb"] = t
	}
	if u := udp.Configs(c.UDPInputs); u.Enabled() {
		m["config-udp"] = u
	}

	return m
}

// registerDiagnostics registers the config settings with the Monitor.
func (c *Config) registerDiagnostics(m *monitor.Monitor) {
	for name, dc := range c.diagnosticsClients() {
		m.RegisterDiagnosticsClient(name, dc)
	}
}

// registerDiagnostics deregisters the config settings from the Monitor.
func (c *Config) deregisterDiagnostics(m *monitor.Monitor) {
	for name := range c.diagnosticsClients() {
		m.DeregisterDiagnosticsClient(name)
	}
}
