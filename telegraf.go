package platform

import (
	"context"
	"time"
)

// TelegrafConfigStore represents a service for managing telegraf config data.
type TelegrafConfigStore interface {
	// FindTelegrafConfigByID returns a single telegraf config by ID.
	FindTelegrafConfigByID(ctx context.Context, id ID) (*TelegrafConfig, error)

	// FindTelegrafConfig returns the first telegraf config that matches filter.
	FindTelegrafConfig(ctx context.Context, filter UserResourceMappingFilter) (*TelegrafConfig, error)

	// FindTelegrafConfigs returns a list of telegraf configs that match filter and the total count of matching telegraf configs.
	// Additional options provide pagination & sorting.
	FindTelegrafConfigs(ctx context.Context, filter UserResourceMappingFilter, opt ...FindOptions) ([]*TelegrafConfig, int, error)

	// CreateTelegrafConfigFilter creates a new telegraf config and sets b.ID with the new identifier.
	CreateTelegrafConfigFilter(ctx context.Context, filter *UserResourceMappingFilter) error

	// UpdateTelegrafConfig updates a single telegraf config.
	// Returns the new telegraf config after update.
	UpdateTelegrafConfig(ctx context.Context, id ID, tc *TelegrafConfig) (*TelegrafConfig, error)

	// DeleteTelegrafConfig removes a telegraf config by ID.
	DeleteTelegrafConfig(ctx context.Context, id ID) error
}

// TelegrafConfig stores telegraf config for one telegraf instance.
type TelegrafConfig struct {
	ID        ID        `json:"id"`
	Name      string    `json:"name"`
	Created   time.Time `json:"created"`
	LastMod   time.Time `json:"last_modified"`
	LastModBy ID        `json:"last_modified_by"`

	Agent TelegrafAgentConfig `json:"agent"`

	Plugins []TelegrafPlugin `json:"plugins"`
}

// TelegrafPlugin is the general wrapper of the telegraf plugin config
type TelegrafPlugin struct {
	// Name of the telegraf plugin, exp "docker"
	Name    string                `json:"name"`
	Type    TelegrafPluginType    `json:"type"`
	Comment string                `json:"comment"`
	Configs []TelegrafAgentConfig `json:"configs"`
}

// TelegrafAgentConfig is based telegraf/internal/config AgentConfig.
type TelegrafAgentConfig struct {
	// Interval at which to gather information
	Interval time.Duration `json:"collectionInterval"`
}

// TelegrafPluginConfig interface for all plugins.
type TelegrafPluginConfig interface {
	// TOML encodes to toml string
	TOML() string
}

// TelegrafPluginType is a string enum type: input/output/processor/aggregator.
type TelegrafPluginType string

// telegraf plugin types
const (
	TelegrafPluginTypeInput      TelegrafPluginType = "input"      // TelegrafPluginTypeInput is an input plugin.
	TelegrafPluginTypeOutput     TelegrafPluginType = "output"     // TelegrafPluginTypeOutput is an output plugin.
	TelegrafPluginTypeProcessor  TelegrafPluginType = "processor"  // TelegrafPluginTypeProcessor is a processor plugin.
	TelegrafPluginTypeAggregator TelegrafPluginType = "aggregator" // TelegrafPluginTypeAggregator is an aggregator plugin.
)
