package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
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

	// DefaultHostName represents the default host name to use if it is never provided
	DefaultHostName = "localhost"

	// DefaultBindAddress represents the bind address to use if none is specified
	DefaultBindAddress = "0.0.0.0"

	// DefaultClusterPort represents the default port the cluster runs ons.
	DefaultClusterPort = 8086

	// DefaultBrokerEnabled is the default for starting a node as a broker
	DefaultBrokerEnabled = true

	// DefaultDataEnabled is the default for starting a node as a data node
	DefaultDataEnabled = true

	// DefaultRetentionCreatePeriod represents how often the server will check to see if new
	// shard groups need to be created in advance for writing
	DefaultRetentionCreatePeriod = 45 * time.Minute

	// DefaultBrokerTruncationInterval is the default period between truncating topics.
	DefaultBrokerTruncationInterval = 10 * time.Minute

	// DefaultMaxTopicSize is the default maximum size in bytes a segment can consume on disk of a broker.
	DefaultBrokerMaxSegmentSize = 10 * 1024 * 1024

	// DefaultMaxTopicSize is the default maximum size in bytes a topic can consume on disk of a broker.
	DefaultBrokerMaxTopicSize = 5 * DefaultBrokerMaxSegmentSize

	// DefaultRaftApplyInterval is the period between applying commited Raft log entries.
	DefaultRaftApplyInterval = 10 * time.Millisecond

	// DefaultRaftElectionTimeout is the default Leader Election timeout.
	DefaultRaftElectionTimeout = 1 * time.Second

	// DefaultRaftHeartbeatInterval is the interval between leader heartbeats.
	DefaultRaftHeartbeatInterval = 100 * time.Millisecond

	// DefaultRaftReconnectTimeout is the default wait time between reconnections.
	DefaultRaftReconnectTimeout = 10 * time.Millisecond

	// DefaultGraphiteDatabaseName is the default Graphite database if none is specified
	DefaultGraphiteDatabaseName = "graphite"

	// DefaultOpenTSDBDatabaseName is the default OpenTSDB database if none is specified
	DefaultOpenTSDBDatabaseName = "opentsdb"

	// DefaultRetentionAutoCreate is the default for auto-creating retention policies
	DefaultRetentionAutoCreate = true

	// DefaultRetentionCheckEnabled is the default for checking for retention policy enforcement
	DefaultRetentionCheckEnabled = true

	// DefaultRetentionCheckPeriod is the period of time between retention policy checks are run
	DefaultRetentionCheckPeriod = 10 * time.Minute

	// DefaultRecomputePreviousN is ???
	DefaultContinuousQueryRecomputePreviousN = 2

	// DefaultContinuousQueryRecomputeNoOlderThan is ???
	DefaultContinuousQueryRecomputeNoOlderThan = 10 * time.Minute

	// DefaultContinuousQueryComputeRunsPerInterval is ???
	DefaultContinuousQueryComputeRunsPerInterval = 10

	// DefaultContinousQueryComputeNoMoreThan is ???
	DefaultContinousQueryComputeNoMoreThan = 2 * time.Minute

	// DefaultStatisticsEnabled is the default setting for whether internal statistics are collected
	DefaultStatisticsEnabled = false

	// DefaultStatisticsDatabase is the default database internal statistics are written
	DefaultStatisticsDatabase = "_internal"

	// DefaultStatisticsRetentionPolicy is he default internal statistics rentention policy name
	DefaultStatisticsRetentionPolicy = "default"

	// DefaultStatisticsWriteInterval is the interval of time between internal stats are written
	DefaultStatisticsWriteInterval = 1 * time.Minute
)

var DefaultSnapshotURL = url.URL{
	Scheme: "http",
	Host:   net.JoinHostPort("127.0.0.1", strconv.Itoa(DefaultClusterPort)),
}

// Broker represents the configuration for a broker node
type Broker struct {
	Dir                string   `toml:"dir"`
	Enabled            bool     `toml:"enabled"`
	TruncationInterval Duration `toml:"truncation-interval"`
	MaxTopicSize       int64    `toml:"max-topic-size"`
	MaxSegmentSize     int64    `toml:"max-segment-size"`
}

// Raft represents the Raft configuration for broker nodes.
type Raft struct {
	ApplyInterval     Duration `toml:"apply-interval"`
	ElectionTimeout   Duration `toml:"election-timeout"`
	HeartbeatInterval Duration `toml:"heartbeat-interval"`
	ReconnectTimeout  Duration `toml:"reconnect-timeout"`
}

// Snapshot represents the configuration for a snapshot service. Snapshot configuration
// is only valid for data nodes.
type Snapshot struct {
	Enabled bool `toml:"enabled"`
}

// Data represents the configuration for a data node
type Data struct {
	Dir                   string   `toml:"dir"`
	Enabled               bool     `toml:"enabled"`
	RetentionAutoCreate   bool     `toml:"retention-auto-create"`
	RetentionCheckEnabled bool     `toml:"retention-check-enabled"`
	RetentionCheckPeriod  Duration `toml:"retention-check-period"`
	RetentionCreatePeriod Duration `toml:"retention-create-period"`
}

// Initialization contains configuration options for the first time a node boots
type Initialization struct {
	// JoinURLs are cluster URLs to use when joining a node to a cluster the first time it boots.  After,
	// a node is joined to a cluster, these URLS are ignored.  These will be overriden at runtime if
	// the node is started with the `-join` flag.
	JoinURLs string `toml:"join-urls"`
}

// Config represents the configuration format for the influxd binary.
type Config struct {
	Hostname          string `toml:"hostname"`
	BindAddress       string `toml:"bind-address"`
	Port              int    `toml:"port"`
	ReportingDisabled bool   `toml:"reporting-disabled"`
	Version           string `toml:"-"`
	InfluxDBVersion   string `toml:"-"`

	Initialization Initialization `toml:"initialization"`

	Authentication struct {
		Enabled bool `toml:"enabled"`
	} `toml:"authentication"`

	Admin struct {
		Enabled bool `toml:"enabled"`
		Port    int  `toml:"port"`
	} `toml:"admin"`

	HTTPAPI struct {
		BindAddress string   `toml:"bind-address"`
		Port        int      `toml:"port"`
		SSLPort     int      `toml:"ssl-port"`
		SSLCertPath string   `toml:"ssl-cert"`
		ReadTimeout Duration `toml:"read-timeout"`
	} `toml:"api"`

	Graphites []Graphite `toml:"graphite"`
	Collectd  Collectd   `toml:"collectd"`
	OpenTSDB  OpenTSDB   `toml:"opentsdb"`

	UDP struct {
		Enabled     bool   `toml:"enabled"`
		BindAddress string `toml:"bind-address"`
		Port        int    `toml:"port"`
	} `toml:"udp"`

	Broker Broker `toml:"broker"`

	Raft Raft `toml:"raft"`

	Data Data `toml:"data"`

	Snapshot Snapshot `toml:"snapshot"`

	Logging struct {
		HTTPAccess   bool `toml:"http-access"`
		WriteTracing bool `toml:"write-tracing"`
		RaftTracing  bool `toml:"raft-tracing"`
	} `toml:"logging"`

	Monitoring struct {
		Enabled       bool     `toml:"enabled"`
		WriteInterval Duration `toml:"write-interval"`
	} `toml:"monitoring"`

	Debugging struct {
		PprofEnabled bool `toml:"pprof-enabled"`
	} `toml:"debugging"`

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
		Disabled bool `toml:"disabled"`
	} `toml:"continuous_queries"`
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {
	c := &Config{}
	c.Hostname = DefaultHostName
	c.BindAddress = DefaultBindAddress
	c.Port = DefaultClusterPort

	c.Data.Enabled = DefaultDataEnabled
	c.Broker.Enabled = DefaultBrokerEnabled

	c.Data.RetentionAutoCreate = DefaultRetentionAutoCreate
	c.Data.RetentionCheckEnabled = DefaultRetentionCheckEnabled
	c.Data.RetentionCheckPeriod = Duration(DefaultRetentionCheckPeriod)
	c.Data.RetentionCreatePeriod = Duration(DefaultRetentionCreatePeriod)

	c.Logging.HTTPAccess = true
	c.Logging.WriteTracing = false
	c.Logging.RaftTracing = false

	c.Monitoring.Enabled = false
	c.Monitoring.WriteInterval = Duration(DefaultStatisticsWriteInterval)
	c.ContinuousQuery.RecomputePreviousN = DefaultContinuousQueryRecomputePreviousN
	c.ContinuousQuery.RecomputeNoOlderThan = Duration(DefaultContinuousQueryRecomputeNoOlderThan)
	c.ContinuousQuery.ComputeRunsPerInterval = DefaultContinuousQueryComputeRunsPerInterval
	c.ContinuousQuery.ComputeNoMoreThan = Duration(DefaultContinousQueryComputeNoMoreThan)

	c.Broker.TruncationInterval = Duration(DefaultBrokerTruncationInterval)
	c.Broker.MaxTopicSize = DefaultBrokerMaxTopicSize
	c.Broker.MaxSegmentSize = DefaultBrokerMaxSegmentSize

	c.Raft.ApplyInterval = Duration(DefaultRaftApplyInterval)
	c.Raft.ElectionTimeout = Duration(DefaultRaftElectionTimeout)
	c.Raft.HeartbeatInterval = Duration(DefaultRaftHeartbeatInterval)
	c.Raft.ReconnectTimeout = Duration(DefaultRaftReconnectTimeout)

	// FIX(benbjohnson): Append where the udp servers are actually used.
	// config.UDPServers = append(config.UDPServers, UDPInputConfig{
	// 	Enabled:  tomlConfiguration.InputPlugins.UDPInput.Enabled,
	// 	Database: tomlConfiguration.InputPlugins.UDPInput.Database,
	// 	Port:     tomlConfiguration.InputPlugins.UDPInput.Port,
	// })

	return c
}

// NewTestConfig returns an instance of Config with reasonable defaults suitable
// for testing a local server w/ broker and data nodes active
func NewTestConfig() (*Config, error) {
	c := NewConfig()

	// By default, store broker and data files in current users home directory
	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to determine current user for storage")
	}

	c.Broker.Enabled = true
	c.Broker.Dir = filepath.Join(u.HomeDir, ".influxdb/broker")

	c.Raft.ApplyInterval = Duration(DefaultRaftApplyInterval)
	c.Raft.ElectionTimeout = Duration(DefaultRaftElectionTimeout)
	c.Raft.HeartbeatInterval = Duration(DefaultRaftHeartbeatInterval)
	c.Raft.ReconnectTimeout = Duration(DefaultRaftReconnectTimeout)

	c.Data.Enabled = true
	c.Data.Dir = filepath.Join(u.HomeDir, ".influxdb/data")

	c.Admin.Enabled = true
	c.Admin.Port = 8083

	c.Monitoring.Enabled = false
	c.Snapshot.Enabled = true

	return c, nil
}

// APIAddr returns the TCP binding address for the API server.
func (c *Config) APIAddr() string {
	// Default to cluster bind address if not overriden
	ba := c.BindAddress
	if c.HTTPAPI.BindAddress != "" {
		ba = c.HTTPAPI.BindAddress
	}

	// Default to cluster port if not overridden
	bp := c.Port
	if c.HTTPAPI.Port != 0 {
		bp = c.HTTPAPI.Port
	}
	return net.JoinHostPort(ba, strconv.Itoa(bp))
}

// APIAddrUDP returns the UDP address for the series listener.
func (c *Config) APIAddrUDP() string {
	return net.JoinHostPort(c.UDP.BindAddress, strconv.Itoa(c.UDP.Port))
}

// ClusterAddr returns the binding address for the cluster
func (c *Config) ClusterAddr() string {
	return net.JoinHostPort(c.BindAddress, strconv.Itoa(c.Port))
}

// ClusterURL returns the URL required to contact the server cluster endpoints.
func (c *Config) ClusterURL() url.URL {
	return url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(c.Hostname, strconv.Itoa(c.Port)),
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

func (d Duration) String() string {
	return time.Duration(d).String()
}

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

// MarshalText converts a duration to a string for decoding toml
func (d Duration) MarshalText() (text []byte, err error) {
	return []byte(d.String()), nil
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
	BindAddress string `toml:"bind-address"`
	Port        uint16 `toml:"port"`

	Database string `toml:"database"`
	Enabled  bool   `toml:"enabled"`
	TypesDB  string `toml:"typesdb"`
}

// ConnnectionString returns the connection string for this collectd config in the form host:port.
func (c *Collectd) ConnectionString(defaultBindAddr string) string {
	addr := c.BindAddress
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
	BindAddress string `toml:"bind-address"`
	Port        int    `toml:"port"`

	Database      string `toml:"database"`
	Enabled       bool   `toml:"enabled"`
	Protocol      string `toml:"protocol"`
	NamePosition  string `toml:"name-position"`
	NameSeparator string `toml:"name-separator"`
}

// ConnnectionString returns the connection string for this Graphite config in the form host:port.
func (g *Graphite) ConnectionString() string {
	return net.JoinHostPort(g.BindAddress, strconv.Itoa(g.Port))
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

type OpenTSDB struct {
	Addr string `toml:"address"`
	Port int    `toml:"port"`

	Enabled         bool   `toml:"enabled"`
	Database        string `toml:"database"`
	RetentionPolicy string `toml:"retention-policy"`
}

func (o OpenTSDB) DatabaseString() string {
	if o.Database == "" {
		return DefaultOpenTSDBDatabaseName
	}
	return o.Database
}

func (o OpenTSDB) ListenAddress() string {
	return net.JoinHostPort(o.Addr, strconv.Itoa(o.Port))
}
