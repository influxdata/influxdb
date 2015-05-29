package run

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os/user"
	"path/filepath"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/admin"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/collectd"
	"github.com/influxdb/influxdb/graphite"
	"github.com/influxdb/influxdb/httpd"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/meta/continuous_querier"
	"github.com/influxdb/influxdb/monitor"
	"github.com/influxdb/influxdb/opentsdb"
	"github.com/influxdb/influxdb/tsdb"
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

	// DefaultOpenTSDBDatabaseName is the default OpenTSDB database if none is specified
	DefaultOpenTSDBDatabaseName = "opentsdb"

	// DefaultStatisticsEnabled is the default setting for whether internal statistics are collected
	DefaultStatisticsEnabled = false

	// DefaultStatisticsDatabase is the default database internal statistics are written
	DefaultStatisticsDatabase = "_internal"

	// DefaultStatisticsRetentionPolicy is he default internal statistics rentention policy name
	DefaultStatisticsRetentionPolicy = "default"
)

var DefaultSnapshotURL = url.URL{
	Scheme: "http",
	Host:   net.JoinHostPort("127.0.0.1", strconv.Itoa(DefaultClusterPort)),
}

// SnapshotConfig represents the configuration for a snapshot service.
// type SnapshotConfig struct {
// 	Enabled bool `toml:"enabled"`
// }

// Config represents the configuration format for the influxd binary.
type Config struct {
	Hostname         string `toml:"hostname"`
	BindAddress      string `toml:"bind-address"`
	ReportingEnabled bool   `toml:"reporting-enabled"`
	// Version           string `toml:"-"`
	// InfluxDBVersion   string `toml:"-"`

	Initialization struct {
		// JoinURLs are cluster URLs to use when joining a node to a cluster the first time it boots.  After,
		// a node is joined to a cluster, these URLS are ignored.  These will be overriden at runtime if
		// the node is started with the `-join` flag.
		JoinURLs string `toml:"join-urls"`
	} `toml:"initialization"`

	Meta    meta.Config    `toml:"meta"`
	Data    tsdb.Config    `toml:"data"`
	Cluster cluster.Config `toml:"cluster"`

	Admin     admin.Config      `toml:"admin"`
	HTTPD     httpd.Config      `toml:"api"`
	Graphites []graphite.Config `toml:"graphite"`
	Collectd  collectd.Config   `toml:"collectd"`
	OpenTSDB  opentsdb.Config   `toml:"opentsdb"`

	// Snapshot SnapshotConfig `toml:"snapshot"`
	Monitoring      monitor.Config            `toml:"monitoring"`
	ContinuousQuery continuous_querier.Config `toml:"continuous_queries"`
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {
	c := &Config{}
	c.Hostname = DefaultHostName
	c.BindAddress = DefaultBindAddress

	c.Meta = meta.NewConfig()
	c.Data = tsdb.NewConfig()
	c.HTTPD = httpd.NewConfig()
	c.Monitoring = monitor.NewConfig()
	c.ContinuousQuery = continuous_querier.NewConfig()

	return c
}

// NewTestConfig returns an instance of Config with reasonable defaults suitable
// for testing a local server w/ broker and data nodes active
func NewTestConfig() (*Config, error) {
	c := NewConfig()

	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to determine current user for storage")
	}

	c.Meta.Dir = filepath.Join(u.HomeDir, ".influxdb/meta")
	c.Data.Dir = filepath.Join(u.HomeDir, ".influxdb/data")

	c.Admin.Enabled = true
	c.Admin.BindAddress = ":8083"

	c.Monitoring.Enabled = false
	//c.Snapshot.Enabled = true

	return c, nil
}

// Normalize sets default values on config.
func (c *Config) Normalize() {
	// Normalize Graphite configs.
	for i, _ := range c.Graphites {
		if c.Graphites[i].BindAddress == "" {
			c.Graphites[i].BindAddress = c.BindAddress
		}
	}

	// Normalize OpenTSDB config.
	if c.OpenTSDB.BindAddress == "" {
		c.OpenTSDB.BindAddress = c.BindAddress
	}
}

// Validate returns an error if the config is invalid.
func (c *Config) Validate() error {
	if c.Meta.Dir == "" {
		return errors.New("Meta.Dir must be specified")
	} else if c.Data.Dir == "" {
		return errors.New("Data.Dir must be specified")
	}
	return nil
}

// DataDir returns the data directory to start up in and does home directory expansion if necessary.
func (c *Config) DataDir() string {
	p, e := filepath.Abs(c.Data.Dir)
	if e != nil {
		log.Fatalf("Unable to get absolute path for Data Directory: %q", c.Data.Dir)
	}
	return p
}

// WriteConfigFile writes the config to the specified writer
func (c *Config) Write(w io.Writer) error {
	return toml.NewEncoder(w).Encode(c)
}
