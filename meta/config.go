package meta

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultHostname is the default hostname if one is not provided.
	DefaultHostname = "localhost"

	// DefaultBindAddress is the default address to bind to.
	DefaultBindAddress = ":8088"

	// DefaultHeartbeatTimeout is the default heartbeat timeout for the store.
	DefaultHeartbeatTimeout = 1000 * time.Millisecond

	// DefaultElectionTimeout is the default election timeout for the store.
	DefaultElectionTimeout = 1000 * time.Millisecond

	// DefaultLeaderLeaseTimeout is the default leader lease for the store.
	DefaultLeaderLeaseTimeout = 500 * time.Millisecond

	// DefaultCommitTimeout is the default commit timeout for the store.
	DefaultCommitTimeout = 50 * time.Millisecond

	// DefaultRetentionPolicyPeriod is the period of time that data will be kept around
	DefaultRetentionPolicyDuration = 24 * 7 * time.Hour

	// DefaultShardDuration is the amount of time it date is kept after the retention policy
	DefaultShardDuration = 24 * time.Hour
)

// Config represents the meta configuration.
type Config struct {
	Dir                     string        `toml:"dir"`
	Hostname                string        `toml:"hostname"`
	BindAddress             string        `toml:"bind-address"`
	Peers                   []string      `toml:"peers"`
	RetentionAutoCreate     bool          `toml:"retention-autocreate"`
	ElectionTimeout         toml.Duration `toml:"election-timeout"`
	HeartbeatTimeout        toml.Duration `toml:"heartbeat-timeout"`
	LeaderLeaseTimeout      toml.Duration `toml:"leader-lease-timeout"`
	CommitTimeout           toml.Duration `toml:"commit-timeout"`
	RetentionPolicyDuration toml.Duration `toml:"retention-policy"`
	ShardDuration           toml.Duration `toml:"shard-duration"`
}

func NewConfig() Config {
	return Config{
		Hostname:                DefaultHostname,
		BindAddress:             DefaultBindAddress,
		RetentionAutoCreate:     true,
		ElectionTimeout:         toml.Duration(DefaultElectionTimeout),
		HeartbeatTimeout:        toml.Duration(DefaultHeartbeatTimeout),
		LeaderLeaseTimeout:      toml.Duration(DefaultLeaderLeaseTimeout),
		CommitTimeout:           toml.Duration(DefaultCommitTimeout),
		RetentionPolicyDuration: toml.Duration(DefaultRetentionPolicyDuration),
		ShardDuration:           toml.Duration(DefaultShardDuration),
	}
}
