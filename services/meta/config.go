package meta

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultHostname is the default hostname if one is not provided.
	DefaultHostname = "localhost"

	// DefaultRaftBindAddress is the default address to bind to.
	DefaultRaftBindAddress = ":8088"

	// DefaultHTTPdBindAddress is the default address to bind to.
	DefaultHTTPdBindAddress = ":8091"

	// DefaultHeartbeatTimeout is the default heartbeat timeout for the store.
	DefaultHeartbeatTimeout = 1000 * time.Millisecond

	// DefaultElectionTimeout is the default election timeout for the store.
	DefaultElectionTimeout = 1000 * time.Millisecond

	// DefaultLeaderLeaseTimeout is the default leader lease for the store.
	DefaultLeaderLeaseTimeout = 500 * time.Millisecond

	// DefaultCommitTimeout is the default commit timeout for the store.
	DefaultCommitTimeout = 50 * time.Millisecond

	// DefaultRaftPromotionEnabled is the default for auto promoting a node to a raft node when needed
	DefaultRaftPromotionEnabled = true

	// DefaultLoggingEnabled determines if log messages are printed for the meta service
	DefaultLoggingEnabled = true
)

// Config represents the meta configuration.
type Config struct {
	Dir                  string        `toml:"dir"`
	Hostname             string        `toml:"hostname"`
	RaftBindAddress      string        `toml:"raft-bind-address"`
	HTTPdBindAddress     string        `toml:"httpd-bind-address"`
	HTTPSEnabled         bool          `toml:"https-enabled"`
	HTTPSCertificate     string        `toml:"https-certificate"`
	Peers                []string      `toml:"-"`
	RetentionAutoCreate  bool          `toml:"retention-autocreate"`
	ElectionTimeout      toml.Duration `toml:"election-timeout"`
	HeartbeatTimeout     toml.Duration `toml:"heartbeat-timeout"`
	LeaderLeaseTimeout   toml.Duration `toml:"leader-lease-timeout"`
	CommitTimeout        toml.Duration `toml:"commit-timeout"`
	ClusterTracing       bool          `toml:"cluster-tracing"`
	RaftPromotionEnabled bool          `toml:"raft-promotion-enabled"`
	LoggingEnabled       bool          `toml:"logging-enabled"`
	PprofEnabled         bool          `toml:"pprof-enabled"`
}

// NewConfig builds a new configuration with default values.
func NewConfig() *Config {
	return &Config{
		Hostname:             DefaultHostname,
		RaftBindAddress:      DefaultRaftBindAddress,
		HTTPdBindAddress:     DefaultHTTPdBindAddress,
		RetentionAutoCreate:  true,
		ElectionTimeout:      toml.Duration(DefaultElectionTimeout),
		HeartbeatTimeout:     toml.Duration(DefaultHeartbeatTimeout),
		LeaderLeaseTimeout:   toml.Duration(DefaultLeaderLeaseTimeout),
		CommitTimeout:        toml.Duration(DefaultCommitTimeout),
		RaftPromotionEnabled: DefaultRaftPromotionEnabled,
		LoggingEnabled:       DefaultLoggingEnabled,
	}
}
