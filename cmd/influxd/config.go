package main

import (
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/collectd"
	"github.com/influxdb/influxdb/graphite"
)

const (
	// DefaultPointBatchSize represents the number of points to batch together.
	DefaultPointBatchSize = 100

	// DefaultPointBatchSize represents the number of writes to batch together.
	DefaultWriteBatchSize = 10 * 1024 * 1024 // 10MB

	// DefaultConcurrentShardQueryLimit represents the number of shards that
	// can be queried concurrently at one time.
	DefaultConcurrentShardQueryLimit = 10

	// DefaultAPIReadTimeout represents the duration before an API request times out.
	DefaultAPIReadTimeout = 5 * time.Second

	// DefaultBrokerPort represents the default port the broker runs on.
	DefaultBrokerPort = 8086

	// DefaultDataPort represents the default port the data server runs on.
	DefaultDataPort = 8086

	// DefaultJoinURLs represents the default URLs for joining a cluster.
	DefaultJoinURLs = ""
)

// Config represents the configuration format for the influxd binary.
type Config struct {
	Hostname          string `toml:"hostname"`
	BindAddress       string `toml:"bind-address"`
	ReportingDisabled bool   `toml:"reporting-disabled"`
	Version           string `toml:"-"`
	InfluxDBVersion   string `toml:"-"`

	Initialization struct {
		JoinURLs string `toml:"join-urls"`
	} `toml:"initialization"`

	Authentication struct {
		Enabled bool `toml:"enabled"`
	} `toml:"authentication"`

	Admin struct {
		Port   int    `toml:"port"`
		Assets string `toml:"assets"`
	} `toml:"admin"`

	HTTPAPI struct {
		Port        int      `toml:"port"`
		SSLPort     int      `toml:"ssl-port"`
		SSLCertPath string   `toml:"ssl-cert"`
		ReadTimeout Duration `toml:"read-timeout"`
	} `toml:"api"`

	Graphites []Graphite `toml:"graphite"`
	Collectd  Collectd   `toml:"collectd"`

	InputPlugins struct {
		UDPInput struct {
			Enabled  bool   `toml:"enabled"`
			Port     int    `toml:"port"`
			Database string `toml:"database"`
		} `toml:"udp"`
		UDPServersInput []struct {
			Enabled  bool   `toml:"enabled"`
			Port     int    `toml:"port"`
			Database string `toml:"database"`
		} `toml:"udp_servers"`
	} `toml:"input_plugins"`

	Broker struct {
		Port    int      `toml:"port"`
		Dir     string   `toml:"dir"`
		Timeout Duration `toml:"election-timeout"`
	} `toml:"broker"`

	Data struct {
		Dir                  string                    `toml:"dir"`
		Port                 int                       `toml:"port"`
		WriteBufferSize      int                       `toml:"write-buffer-size"`
		MaxOpenShards        int                       `toml:"max-open-shards"`
		PointBatchSize       int                       `toml:"point-batch-size"`
		WriteBatchSize       int                       `toml:"write-batch-size"`
		Engines              map[string]toml.Primitive `toml:"engines"`
		RetentionSweepPeriod Duration                  `toml:"retention-sweep-period"`
	} `toml:"data"`

	Cluster struct {
		Dir string `toml:"dir"`
	} `toml:"cluster"`

	Logging struct {
		File string `toml:"file"`
	} `toml:"logging"`
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {
	u, _ := user.Current()

	c := &Config{}
	c.Data.RetentionSweepPeriod = Duration(10 * time.Minute)
	c.Broker.Dir = filepath.Join(u.HomeDir, ".influxdb/broker")
	c.Broker.Port = DefaultBrokerPort
	c.Broker.Timeout = Duration(1 * time.Second)
	c.Data.Dir = filepath.Join(u.HomeDir, ".influxdb/data")
	c.Data.Port = DefaultDataPort
	c.Data.WriteBufferSize = 1000

	// Detect hostname (or set to localhost).
	if c.Hostname, _ = os.Hostname(); c.Hostname == "" {
		c.Hostname = "localhost"
	}

	// FIX(benbjohnson): Append where the udp servers are actually used.
	// config.UDPServers = append(config.UDPServers, UDPInputConfig{
	// 	Enabled:  tomlConfiguration.InputPlugins.UDPInput.Enabled,
	// 	Database: tomlConfiguration.InputPlugins.UDPInput.Database,
	// 	Port:     tomlConfiguration.InputPlugins.UDPInput.Port,
	// })

	return c
}

// PointBatchSize returns the data point batch size, if set.
// If not set, the LevelDB point batch size is returned.
// If that is not set then the default point batch size is returned.
func (c *Config) PointBatchSize() int {
	if c.Data.PointBatchSize != 0 {
		return c.Data.PointBatchSize
	}
	return DefaultPointBatchSize
}

// WriteBatchSize returns the data write batch size, if set.
// If not set, the LevelDB write batch size is returned.
// If that is not set then the default write batch size is returned.
func (c *Config) WriteBatchSize() int {
	if c.Data.WriteBatchSize != 0 {
		return c.Data.WriteBatchSize
	}
	return DefaultWriteBatchSize
}

// MaxOpenShards returns the maximum number of shards to keep open at once.
func (c *Config) MaxOpenShards() int {
	return c.Data.MaxOpenShards
}

// DataAddr returns the binding address the data server
func (c *Config) DataAddr() string {
	return net.JoinHostPort(c.BindAddress, strconv.Itoa(c.Data.Port))
}

// DataURL returns the URL required to contact the data server.
func (c *Config) DataURL() *url.URL {
	return &url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(c.Hostname, strconv.Itoa(c.Data.Port)),
	}
}

// BrokerAddr returns the binding address the Broker server
func (c *Config) BrokerAddr() string {
	return fmt.Sprintf("%s:%d", c.BindAddress, c.Broker.Port)
}

// BrokerURL returns the URL required to contact the Broker server.
func (c *Config) BrokerURL() *url.URL {
	return &url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(c.Hostname, strconv.Itoa(c.Broker.Port)),
	}
}

// BrokerDir returns the data directory to start up in and does home directory expansion if necessary.
func (c *Config) BrokerDir() string {
	p, e := filepath.Abs(c.Broker.Dir)
	if e != nil {
		log.Fatalf("Unable to get absolute path for Broker Directory: %q", c.Broker.Dir)
	}
	return p
}

// DataDir returns the data directory to start up in and does home directory expansion if necessary.
func (c *Config) DataDir() string {
	p, e := filepath.Abs(c.Data.Dir)
	if e != nil {
		log.Fatalf("Unable to get absolute path for Data Directory: %q", c.Data.Dir)
	}
	return p
}

func (c *Config) JoinURLs() string {
	if c.Initialization.JoinURLs == "" {
		return DefaultJoinURLs
	} else {
		return c.Initialization.JoinURLs
	}
}

// Size represents a TOML parseable file size.
// Users can specify size using "m" for megabytes and "g" for gigabytes.
type Size int

// UnmarshalText parses a byte size from text.
func (s *Size) UnmarshalText(text []byte) error {
	// Parse numeric portion of value.
	length := len(string(text))
	size, err := strconv.ParseInt(string(text[:length-1]), 10, 64)
	if err != nil {
		return err
	}

	// Parse unit of measure ("m", "g", etc).
	switch suffix := text[len(text)-1]; suffix {
	case 'm':
		size *= 1 << 20 // MB
	case 'g':
		size *= 1 << 30 // GB
	default:
		return fmt.Errorf("unknown size suffix: %c", suffix)
	}

	// Check for overflow.
	if size > maxInt {
		return fmt.Errorf("size %d cannot be represented by an int", size)
	}

	*s = Size(size)
	return nil
}

// Duration is a TOML wrapper type for time.Duration.
type Duration time.Duration

// UnmarshalText parses a TOML value into a duration value.
func (d *Duration) UnmarshalText(text []byte) error {
	// Ignore if there is no value set.
	if len(text) == 0 {
		return nil
	}

	// Otherwise parse as a duration formatted string.
	duration, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}

	// Set duration and return.
	*d = Duration(duration)
	return nil
}

// ParseConfigFile parses a configuration file at a given path.
func ParseConfigFile(path string) (*Config, error) {
	c := NewConfig()
	if _, err := toml.DecodeFile(path, &c); err != nil {
		return nil, err
	}
	return c, nil
}

// ParseConfig parses a configuration string into a config object.
func ParseConfig(s string) (*Config, error) {
	c := NewConfig()
	if _, err := toml.Decode(s, &c); err != nil {
		return nil, err
	}
	return c, nil
}

type Collectd struct {
	Addr string `toml:"address"`
	Port uint16 `toml:"port"`

	Database string `toml:"database"`
	Enabled  bool   `toml:"enabled"`
	TypesDB  string `toml:"typesdb"`
}

// ConnnectionString returns the connection string for this collectd config in the form host:port.
func (c *Collectd) ConnectionString(defaultBindAddr string) string {
	addr := c.Addr
	// If no address specified, use default.
	if addr == "" {
		addr = defaultBindAddr
	}

	port := c.Port
	// If no port specified, use default.
	if port == 0 {
		port = collectd.DefaultPort
	}

	return fmt.Sprintf("%s:%d", addr, port)
}

type Graphite struct {
	Addr string `toml:"address"`
	Port uint16 `toml:"port"`

	Database      string `toml:"database"`
	Enabled       bool   `toml:"enabled"`
	Protocol      string `toml:"protocol"`
	NamePosition  string `toml:"name-position"`
	NameSeparator string `toml:"name-separator"`
}

// ConnnectionString returns the connection string for this Graphite config in the form host:port.
func (g *Graphite) ConnectionString(defaultBindAddr string) string {

	addr := g.Addr
	// If no address specified, use default.
	if addr == "" {
		addr = defaultBindAddr
	}

	port := g.Port
	// If no port specified, use default.
	if port == 0 {
		port = graphite.DefaultGraphitePort
	}

	return fmt.Sprintf("%s:%d", addr, port)
}

// NameSeparatorString returns the character separating fields for Graphite data, or the default
// if no separator is set.
func (g *Graphite) NameSeparatorString() string {
	if g.NameSeparator == "" {
		return graphite.DefaultGraphiteNameSeparator
	}
	return g.NameSeparator
}

// LastEnabled returns whether the Graphite Server shoudl intepret the last field as "name".
func (g *Graphite) LastEnabled() bool {
	return g.NamePosition == strings.ToLower("last")
}

// maxInt is the largest integer representable by a word (architeture dependent).
const maxInt = int64(^uint(0) >> 1)
