package main

import (
	"fmt"
	"io"
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

	// DefaultSnapshotBindAddress is the default bind address to serve snapshots from.
	DefaultSnapshotBindAddress = "127.0.0.1"

	// DefaultSnapshotPort is the default port to serve snapshots from.
	DefaultSnapshotPort = 8087

	// DefaultJoinURLs represents the default URLs for joining a cluster.
	DefaultJoinURLs = ""

	// DefaultRetentionCreatePeriod represents how often the server will check to see if new
	// shard groups need to be created in advance for writing
	DefaultRetentionCreatePeriod = 45 * time.Minute

	// DefaultGraphiteDatabaseName is the default Graphite database if none is specified
	DefaultGraphiteDatabaseName = "graphite"
)

var DefaultSnapshotURL = url.URL{
	Scheme: "http",
	Host:   net.JoinHostPort(DefaultSnapshotBindAddress, strconv.Itoa(DefaultSnapshotPort)),
}

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
		Enabled bool `toml:"enabled"`
		Port    int  `toml:"port"`
	} `toml:"admin"`

	HTTPAPI struct {
		Port        int      `toml:"port"`
		SSLPort     int      `toml:"ssl-port"`
		SSLCertPath string   `toml:"ssl-cert"`
		ReadTimeout Duration `toml:"read-timeout"`
	} `toml:"api"`

	Graphites []Graphite `toml:"graphite"`
	Collectd  Collectd   `toml:"collectd"`

	UDP struct {
		Enabled     bool   `toml:"enabled"`
		BindAddress string `toml:"bind-address"`
		Port        int    `toml:"port"`
	} `toml:"udp"`

	Broker struct {
		Port    int      `toml:"port"`
		Dir     string   `toml:"dir"`
		Timeout Duration `toml:"election-timeout"`
	} `toml:"broker"`

	Data struct {
		Dir                   string   `toml:"dir"`
		Port                  int      `toml:"port"`
		RetentionAutoCreate   bool     `toml:"retention-auto-create"`
		RetentionCheckEnabled bool     `toml:"retention-check-enabled"`
		RetentionCheckPeriod  Duration `toml:"retention-check-period"`
		RetentionCreatePeriod Duration `toml:"retention-create-period"`
	} `toml:"data"`

	Snapshot struct {
		Enabled     bool   `toml:"enabled"`
		BindAddress string `toml:"bind-address"`
		Port        int    `toml:"port"`
	}
	Cluster struct {
		Dir string `toml:"dir"`
	} `toml:"cluster"`

	Logging struct {
		WriteTracing bool `toml:"write-tracing"`
		RaftTracing  bool `toml:"raft-tracing"`
	} `toml:"logging"`

	Statistics struct {
		Enabled         bool     `toml:"enabled"`
		Database        string   `toml:"database"`
		RetentionPolicy string   `toml:"retention-policy"`
		WriteInterval   Duration `toml:"write-interval"`
	}

	ContinuousQuery struct {
		// when continuous queries are run we'll automatically recompute previous intervals
		// in case lagged data came in. Set to zero if you never have lagged data. We do
		// it this way because invalidating previously computed intervals would be insanely hard
		// and expensive.
		RecomputePreviousN int `toml:"recompute-previous-n"`

		// The RecomputePreviousN setting provides guidance for how far back to recompute, the RecomputeNoOlderThan
		// setting sets a ceiling on how far back in time it will go. For example, if you have 2 PreviousN
		// and have this set to 10m, then we'd only compute the previous two intervals for any
		// CQs that have a group by time <= 5m. For all others, we'd only recompute the previous window
		RecomputeNoOlderThan Duration `toml:"recompute-no-older-than"`

		// ComputeRunsPerInterval will determine how many times the current and previous N intervals
		// will be computed. The group by time will be divided by this and it will get computed  this many times:
		// group by time seconds / runs per interval
		// This will give partial results for current group by intervals and will determine how long it will
		// be until lagged data is recomputed. For example, if this number is 10 and the group by time is 10m, it
		// will be a minute past the previous 10m bucket of time before lagged data is picked up
		ComputeRunsPerInterval int `toml:"compute-runs-per-interval"`

		// ComputeNoMoreThan paired with the RunsPerInterval will determine the ceiling of how many times smaller
		// group by times will be computed. For example, if you have RunsPerInterval set to 10 and this setting
		// to 1m. Then for a group by time(1m) will actually only get computed once per interval (and once per PreviousN).
		// If you have a group by time(5m) then you'll get five computes per interval. Any group by time window larger
		// than 10m will get computed 10 times for each interval.
		ComputeNoMoreThan Duration `toml:"compute-no-more-than"`

		// If this flag is set to true, both the brokers and data nodes should ignore any CQ processing.
		Disable bool `toml:"disable"`
	} `toml:"continuous_queries"`
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() (*Config, error) {
	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to determine current user for storage")
	}

	c := &Config{}
	c.Broker.Dir = filepath.Join(u.HomeDir, ".influxdb/broker")
	c.Broker.Port = DefaultBrokerPort
	c.Broker.Timeout = Duration(1 * time.Second)
	c.Data.Dir = filepath.Join(u.HomeDir, ".influxdb/data")
	c.Data.Port = DefaultDataPort
	c.Data.RetentionAutoCreate = true
	c.Data.RetentionCheckEnabled = true
	c.Data.RetentionCheckPeriod = Duration(10 * time.Minute)
	c.Data.RetentionCreatePeriod = Duration(DefaultRetentionCreatePeriod)
	c.Snapshot.Enabled = true
	c.Snapshot.BindAddress = DefaultSnapshotBindAddress
	c.Snapshot.Port = DefaultSnapshotPort
	c.Admin.Enabled = true
	c.Admin.Port = 8083
	c.ContinuousQuery.RecomputePreviousN = 2
	c.ContinuousQuery.RecomputeNoOlderThan = Duration(10 * time.Minute)
	c.ContinuousQuery.ComputeRunsPerInterval = 10
	c.ContinuousQuery.ComputeNoMoreThan = Duration(2 * time.Minute)
	c.ContinuousQuery.Disable = false
	c.ReportingDisabled = false

	c.Statistics.Enabled = false
	c.Statistics.Database = "_internal"
	c.Statistics.RetentionPolicy = "default"
	c.Statistics.WriteInterval = Duration(1 * time.Minute)

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

	return c, nil
}

// DataAddr returns the TCP binding address for the data server.
func (c *Config) DataAddr() string {
	return net.JoinHostPort(c.BindAddress, strconv.Itoa(c.Data.Port))
}

// DataAddrUDP returns the UDP address for the series listener.
func (c *Config) DataAddrUDP() string {
	return net.JoinHostPort(c.UDP.BindAddress, strconv.Itoa(c.UDP.Port))
}

// DataURL returns the URL required to contact the data server.
func (c *Config) DataURL() url.URL {
	return url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(c.Hostname, strconv.Itoa(c.Data.Port)),
	}
}

// SnapshotAddr returns the TCP binding address for the snapshot handler.
func (c *Config) SnapshotAddr() string {
	return net.JoinHostPort(c.Snapshot.BindAddress, strconv.Itoa(c.Snapshot.Port))
}

// BrokerAddr returns the binding address the Broker server
func (c *Config) BrokerAddr() string {
	return fmt.Sprintf("%s:%d", c.BindAddress, c.Broker.Port)
}

// BrokerURL returns the URL required to contact the Broker server.
func (c *Config) BrokerURL() url.URL {
	return url.URL{
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

// ShardGroupPreCreateCheckPeriod returns the check interval to pre-create shard groups.
// If it was not defined in the config, it defaults to DefaultShardGroupPreCreatePeriod
func (c *Config) ShardGroupPreCreateCheckPeriod() time.Duration {
	if c.Data.RetentionCreatePeriod != 0 {
		return time.Duration(c.Data.RetentionCreatePeriod)
	}
	return DefaultRetentionCreatePeriod
}

// WriteConfigFile writes the config to the specified writer
func (c *Config) Write(w io.Writer) error {
	return toml.NewEncoder(w).Encode(c)
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
	c, err := NewConfig()
	if err != nil {
		return nil, err
	}
	if _, err := toml.DecodeFile(path, &c); err != nil {
		return nil, err
	}
	return c, nil
}

// ParseConfig parses a configuration string into a config object.
func ParseConfig(s string) (*Config, error) {
	c, err := NewConfig()
	if err != nil {
		return nil, err
	}
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

func (g *Graphite) DatabaseString() string {
	if g.Database == "" {
		return DefaultGraphiteDatabaseName
	}
	return g.Database
}

// LastEnabled returns whether the Graphite Server shoudl intepret the last field as "name".
func (g *Graphite) LastEnabled() bool {
	return g.NamePosition == strings.ToLower("last")
}

// maxInt is the largest integer representable by a word (architeture dependent).
const maxInt = int64(^uint(0) >> 1)
