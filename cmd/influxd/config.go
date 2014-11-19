package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	// DefaultPointBatchSize represents the number of points to batch together.
	DefaultPointBatchSize = 100

	// DefaultPointBatchSize represents the number of writes to batch together.
	DefaultWriteBatchSize = 10 * 1024 * 1024 // 10MB

	// DefaultConcurrentShardQueryLimit represents the number of shards that
	// can be queried concurrently at one time.
	DefaultConcurrentShardQueryLimit = 10

	// DefaultAPIReadTimeout represents the amount time before an API request
	// times out.
	DefaultAPIReadTimeout = 5 * time.Second
)

// Config represents the configuration format for the influxd binary.
type Config struct {
	Hostname          string `toml:"hostname"`
	BindAddress       string `toml:"bind-address"`
	ReportingDisabled bool   `toml:"reporting-disabled"`
	Version           string `toml:"-"`
	InfluxDBVersion   string `toml:"-"`

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

	InputPlugins struct {
		Graphite struct {
			Enabled    bool   `toml:"enabled"`
			Port       int    `toml:"port"`
			Database   string `toml:"database"`
			UDPEnabled bool   `toml:"udp_enabled"`
		} `toml:"graphite"`
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

	Raft struct {
		Port    int      `toml:"port"`
		Dir     string   `toml:"dir"`
		Timeout Duration `toml:"election-timeout"`
	} `toml:"raft"`

	Storage struct {
		Dir                  string                    `toml:"dir"`
		WriteBufferSize      int                       `toml:"write-buffer-size"`
		MaxOpenShards        int                       `toml:"max-open-shards"`
		PointBatchSize       int                       `toml:"point-batch-size"`
		WriteBatchSize       int                       `toml:"write-batch-size"`
		Engines              map[string]toml.Primitive `toml:"engines"`
		RetentionSweepPeriod Duration                  `toml:"retention-sweep-period"`
	} `toml:"storage"`

	Cluster struct {
		SeedServers               []string `toml:"seed-servers"`
		ProtobufPort              int      `toml:"protobuf_port"`
		ProtobufTimeout           Duration `toml:"protobuf_timeout"`
		ProtobufHeartbeatInterval Duration `toml:"protobuf_heartbeat"`
		MinBackoff                Duration `toml:"protobuf_min_backoff"`
		MaxBackoff                Duration `toml:"protobuf_max_backoff"`
		WriteBufferSize           int      `toml:"write-buffer-size"`
		ConcurrentShardQueryLimit int      `toml:"concurrent-shard-query-limit"`
		MaxResponseBufferSize     int      `toml:"max-response-buffer-size"`
	} `toml:"cluster"`

	Logging struct {
		File  string `toml:"file"`
		Level string `toml:"level"`
	} `toml:"logging"`
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {
	c := &Config{}
	c.Storage.RetentionSweepPeriod = Duration(10 * time.Minute)
	c.Cluster.ConcurrentShardQueryLimit = DefaultConcurrentShardQueryLimit
	c.Raft.Timeout = Duration(1 * time.Second)
	c.HTTPAPI.ReadTimeout = Duration(DefaultAPIReadTimeout)
	c.Cluster.MinBackoff = Duration(1 * time.Second)
	c.Cluster.MaxBackoff = Duration(10 * time.Second)
	c.Cluster.ProtobufHeartbeatInterval = Duration(10 * time.Millisecond)
	c.Storage.WriteBufferSize = 1000
	c.Cluster.WriteBufferSize = 1000
	c.Cluster.MaxResponseBufferSize = 100

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

// PointBatchSize returns the storage point batch size, if set.
// If not set, the LevelDB point batch size is returned.
// If that is not set then the default point batch size is returned.
func (c *Config) PointBatchSize() int {
	if c.Storage.PointBatchSize != 0 {
		return c.Storage.PointBatchSize
	}
	return DefaultPointBatchSize
}

// WriteBatchSize returns the storage write batch size, if set.
// If not set, the LevelDB write batch size is returned.
// If that is not set then the default write batch size is returned.
func (c *Config) WriteBatchSize() int {
	if c.Storage.WriteBatchSize != 0 {
		return c.Storage.WriteBatchSize
	}
	return DefaultWriteBatchSize
}

// MaxOpenShards returns the maximum number of shards to keep open at once.
func (c *Config) MaxOpenShards() int {
	return c.Storage.MaxOpenShards
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

/*
func (c *Config) AdminHTTPPortString() string {
	if c.AdminHTTPPort <= 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", c.BindAddress, c.AdminHTTPPort)
}

func (c *Config) ApiHTTPPortString() string {
	if c.HTTPAPI.Port <= 0 {
		return ""
	}

	return fmt.Sprintf("%s:%d", c.BindAddress, c.HTTPAPI.Port)
}

func (c *Config) APIHTTPSPortString() string {
	return fmt.Sprintf("%s:%d", c.BindAddress, c.APIHTTPSPort)
}

func (c *Config) GraphitePortString() string {
	if c.GraphitePort <= 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", c.BindAddress, c.GraphitePort)
}

func (c *Config) UDPInputPortString(port int) string {
	if port <= 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", c.BindAddress, port)
}

func (c *Config) ProtobufConnectionString() string {
	return fmt.Sprintf("%s:%d", c.Hostname, c.ProtobufPort)
}

func (c *Config) RaftConnectionString() string {
	return fmt.Sprintf("http://%s:%d", c.Hostname, c.RaftServerPort)
}

func (c *Config) ProtobufListenString() string {
	return fmt.Sprintf("%s:%d", c.BindAddress, c.ProtobufPort)
}

func (c *Config) RaftListenString() string {
	return fmt.Sprintf("%s:%d", c.BindAddress, c.RaftServerPort)
}
*/

// maxInt is the largest integer representable by a word (architeture dependent).
const maxInt = int64(^uint(0) >> 1)
