package run

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/continuous_querier"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/opentsdb"
	"github.com/influxdata/influxdb/services/precreator"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/services/subscriber"
	"github.com/influxdata/influxdb/services/udp"
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
	Storage        storage.Config    `toml:"ifql"`
	GraphiteInputs []graphite.Config `toml:"graphite"`
	CollectdInputs []collectd.Config `toml:"collectd"`
	OpenTSDBInputs []opentsdb.Config `toml:"opentsdb"`
	UDPInputs      []udp.Config      `toml:"udp"`

	ContinuousQuery continuous_querier.Config `toml:"continuous_queries"`

	// Server reporting
	ReportingDisabled bool `toml:"reporting-disabled"`

	// BindAddress is the address that all TCP services use (Raft, Snapshot, Cluster, etc.)
	BindAddress string `toml:"bind-address"`
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
	c.Storage = storage.NewConfig()

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

	return nil
}

// ApplyEnvOverrides apply the environment configuration on top of the config.
func (c *Config) ApplyEnvOverrides(getenv func(string) string) error {
	if getenv == nil {
		getenv = os.Getenv
	}
	return c.applyEnvOverrides(getenv, "INFLUXDB", reflect.ValueOf(c), "")
}

func (c *Config) applyEnvOverrides(getenv func(string) string, prefix string, spec reflect.Value, structKey string) error {
	// If we have a pointer, dereference it
	element := spec
	if spec.Kind() == reflect.Ptr {
		element = spec.Elem()
	}

	value := getenv(prefix)

	switch element.Kind() {
	case reflect.String:
		if len(value) == 0 {
			return nil
		}
		element.SetString(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var intValue int64

		// Handle toml.Duration
		if element.Type().Name() == "Duration" {
			dur, err := time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", prefix, structKey, element.Type().String(), value)
			}
			intValue = dur.Nanoseconds()
		} else {
			var err error
			intValue, err = strconv.ParseInt(value, 0, element.Type().Bits())
			if err != nil {
				return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", prefix, structKey, element.Type().String(), value)
			}
		}
		element.SetInt(intValue)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		intValue, err := strconv.ParseUint(value, 0, element.Type().Bits())
		if err != nil {
			return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", prefix, structKey, element.Type().String(), value)
		}
		element.SetUint(intValue)
	case reflect.Bool:
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", prefix, structKey, element.Type().String(), value)
		}
		element.SetBool(boolValue)
	case reflect.Float32, reflect.Float64:
		floatValue, err := strconv.ParseFloat(value, element.Type().Bits())
		if err != nil {
			return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", prefix, structKey, element.Type().String(), value)
		}
		element.SetFloat(floatValue)
	case reflect.Slice:
		// If the type is s slice, apply to each using the index as a suffix, e.g. GRAPHITE_0, GRAPHITE_0_TEMPLATES_0 or GRAPHITE_0_TEMPLATES="item1,item2"
		for j := 0; j < element.Len(); j++ {
			f := element.Index(j)
			if err := c.applyEnvOverrides(getenv, prefix, f, structKey); err != nil {
				return err
			}

			if err := c.applyEnvOverrides(getenv, fmt.Sprintf("%s_%d", prefix, j), f, structKey); err != nil {
				return err
			}
		}

		// If the type is s slice but have value not parsed as slice e.g. GRAPHITE_0_TEMPLATES="item1,item2"
		if element.Len() == 0 && len(value) > 0 {
			rules := strings.Split(value, ",")

			for _, rule := range rules {
				element.Set(reflect.Append(element, reflect.ValueOf(rule)))
			}
		}
	case reflect.Struct:
		typeOfSpec := element.Type()
		for i := 0; i < element.NumField(); i++ {
			field := element.Field(i)

			// Skip any fields that we cannot set
			if !field.CanSet() && field.Kind() != reflect.Slice {
				continue
			}

			fieldName := typeOfSpec.Field(i).Name

			configName := typeOfSpec.Field(i).Tag.Get("toml")
			// Replace hyphens with underscores to avoid issues with shells
			configName = strings.Replace(configName, "-", "_", -1)

			envKey := strings.ToUpper(configName)
			if prefix != "" {
				envKey = strings.ToUpper(fmt.Sprintf("%s_%s", prefix, configName))
			}

			// If it's a sub-config, recursively apply
			if field.Kind() == reflect.Struct || field.Kind() == reflect.Ptr ||
				field.Kind() == reflect.Slice || field.Kind() == reflect.Array {
				if err := c.applyEnvOverrides(getenv, envKey, field, fieldName); err != nil {
					return err
				}
				continue
			}

			value := getenv(envKey)
			// Skip any fields we don't have a value to set
			if len(value) == 0 {
				continue
			}

			if err := c.applyEnvOverrides(getenv, envKey, field, fieldName); err != nil {
				return err
			}
		}
	}
	return nil
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
