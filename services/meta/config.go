package meta

import (
	"errors"
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultEnabled is the default state for the meta service to run
	DefaultEnabled = true

	// DefaultHostname is the default hostname if one is not provided.
	DefaultHostname = "localhost"

	// DefaultRaftBindAddress is the default address to bind to.
	DefaultRaftBindAddress = ":8088"

	// DefaultHTTPBindAddress is the default address to bind the API to.
	DefaultHTTPBindAddress = ":8091"

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
	Enabled  bool   `toml:"enabled"`
	Dir      string `toml:"dir"`
	Hostname string `toml:"hostname"`

	// this is deprecated. Should use the address from run/config.go
	BindAddress string `toml:"bind-address"`

	// HTTPBindAddress is the bind address for the metaservice HTTP API
	HTTPBindAddress  string `toml:"http-bind-address"`
	HTTPSEnabled     bool   `toml:"https-enabled"`
	HTTPSCertificate string `toml:"https-certificate"`

	// JoinPeers if specified gives other metastore servers to join this server to the cluster
	JoinPeers            []string      `toml:"-"`
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
		Enabled:              true, // enabled by default
		Hostname:             DefaultHostname,
		BindAddress:          DefaultRaftBindAddress,
		HTTPBindAddress:      DefaultHTTPBindAddress,
		RetentionAutoCreate:  true,
		ElectionTimeout:      toml.Duration(DefaultElectionTimeout),
		HeartbeatTimeout:     toml.Duration(DefaultHeartbeatTimeout),
		LeaderLeaseTimeout:   toml.Duration(DefaultLeaderLeaseTimeout),
		CommitTimeout:        toml.Duration(DefaultCommitTimeout),
		RaftPromotionEnabled: DefaultRaftPromotionEnabled,
		LoggingEnabled:       DefaultLoggingEnabled,
	}
}

func (c *Config) Validate() error {
	if c.Enabled && c.Dir == "" {
		return errors.New("Meta.Dir must be specified")
	}
	return nil
}
