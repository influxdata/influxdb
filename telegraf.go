package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/influxdata/platform/telegraf/plugins"
	"github.com/influxdata/platform/telegraf/plugins/inputs"
	"github.com/influxdata/platform/telegraf/plugins/outputs"
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

	// CreateTelegrafConfig creates a new telegraf config and sets b.ID with the new identifier.
	CreateTelegrafConfig(ctx context.Context, tc *TelegrafConfig, userID ID, now time.Time) error

	// UpdateTelegrafConfig updates a single telegraf config.
	// Returns the new telegraf config after update.
	UpdateTelegrafConfig(ctx context.Context, id ID, tc *TelegrafConfig, userID ID, now time.Time) (*TelegrafConfig, error)

	// DeleteTelegrafConfig removes a telegraf config by ID.
	DeleteTelegrafConfig(ctx context.Context, id ID) error
}

// TelegrafConfig stores telegraf config for one telegraf instance.
type TelegrafConfig struct {
	ID        ID
	Name      string
	Created   time.Time
	LastMod   time.Time
	LastModBy ID

	Agent   TelegrafAgentConfig
	Plugins []TelegrafPlugin
}

// telegrafConfigEncode is the helper struct for json encoding.
type telegrafConfigEncode struct {
	ID        ID        `json:"id"`
	Name      string    `json:"name"`
	Created   time.Time `json:"created"`
	LastMod   time.Time `json:"last_modified"`
	LastModBy ID        `json:"last_modified_by"`

	Agent TelegrafAgentConfig `json:"agent"`

	Plugins []telegrafPluginEncode `json:"plugins"`
}

// telegrafPluginEncode is the helper struct for json encoding.
type telegrafPluginEncode struct {
	// Name of the telegraf plugin, exp "docker"
	Name    string               `json:"name"`
	Type    plugins.Type         `json:"type"`
	Comment string               `json:"comment"`
	Config  TelegrafPluginConfig `json:"config"`
}

// telegrafConfigDecode is the helper struct for json decoding.
type telegrafConfigDecode struct {
	ID        ID        `json:"id"`
	Name      string    `json:"name"`
	Created   time.Time `json:"created"`
	LastMod   time.Time `json:"last_modified"`
	LastModBy ID        `json:"last_modified_by"`

	Agent TelegrafAgentConfig `json:"agent"`

	Plugins []telegrafPluginDecode `json:"plugins"`
}

// telegrafPluginDecode is the helper struct for json decoding.
type telegrafPluginDecode struct {
	// Name of the telegraf plugin, exp "docker"
	Name    string          `json:"name"`
	Type    plugins.Type    `json:"type"`
	Comment string          `json:"comment"`
	Config  json.RawMessage `json:"config"`
}

// TelegrafPlugin is the general wrapper of the telegraf plugin config
type TelegrafPlugin struct {
	Comment string               `json:"comment"`
	Config  TelegrafPluginConfig `json:"config"`
}

// TelegrafAgentConfig is based telegraf/internal/config AgentConfig.
type TelegrafAgentConfig struct {
	// Interval at which to gather information in miliseconds.
	Interval int64 `json:"collectionInterval"`
}

// TelegrafPluginConfig interface for all plugins.
type TelegrafPluginConfig interface {
	// TOML encodes to toml string
	TOML() string
	// Type is the plugin type
	Type() plugins.Type
	// PluginName is the string value of telegraf plugin package name.
	PluginName() string
}

// errors
const (
	ErrTelegrafPluginNameUnmatch   = "the telegraf plugin is name %s doesn't match the config %s"
	ErrNoTelegrafPlugins           = "there is no telegraf plugin in the config"
	ErrUnsupportTelegrafPluginType = "unsupported telegraf plugin type %s"
	ErrUnsupportTelegrafPluginName = "unsupported telegraf plugin %s, type %s"
)

// MarshalJSON implement the json.Marshaler interface.
func (tc *TelegrafConfig) MarshalJSON() ([]byte, error) {
	tce := new(telegrafConfigEncode)
	*tce = telegrafConfigEncode{
		ID:        tc.ID,
		Name:      tc.Name,
		Agent:     tc.Agent,
		Created:   tc.Created,
		LastMod:   tc.LastMod,
		LastModBy: tc.LastModBy,
		Plugins:   make([]telegrafPluginEncode, len(tc.Plugins)),
	}
	for k, p := range tc.Plugins {
		tce.Plugins[k] = telegrafPluginEncode{
			Name:    p.Config.PluginName(),
			Type:    p.Config.Type(),
			Comment: p.Comment,
			Config:  p.Config,
		}
	}
	return json.Marshal(tce)
}

// UnmarshalJSON implement the json.Unmarshaler interface.
func (tc *TelegrafConfig) UnmarshalJSON(b []byte) error {
	tcd := new(telegrafConfigDecode)
	if err := json.Unmarshal(b, tcd); err != nil {
		return err
	}
	*tc = TelegrafConfig{
		ID:        tcd.ID,
		Name:      tcd.Name,
		Created:   tcd.Created,
		LastMod:   tcd.LastMod,
		LastModBy: tcd.LastModBy,
		Agent:     tcd.Agent,
		Plugins:   make([]TelegrafPlugin, len(tcd.Plugins)),
	}
	return decodePluginRaw(tcd, tc)
}

func decodePluginRaw(tcd *telegrafConfigDecode, tc *TelegrafConfig) (err error) {
	op := "unmarshal telegraf config raw plugin"
	for k, pr := range tcd.Plugins {
		var config TelegrafPluginConfig
		var ok bool
		switch pr.Type {
		case plugins.Input:
			config, ok = availableInputPlugins[pr.Name]
		case plugins.Output:
			config, ok = availableOutputPlugins[pr.Name]
		default:
			return &Error{
				Code: EInvalid,
				Msg:  fmt.Sprintf(ErrUnsupportTelegrafPluginType, pr.Type),
				Op:   op,
			}
		}
		if ok {
			if err = json.Unmarshal(pr.Config, config); err != nil {
				return &Error{
					Code: EInvalid,
					Err:  err,
					Op:   op,
				}
			}
			tc.Plugins[k] = TelegrafPlugin{
				Comment: pr.Comment,
				Config:  config,
			}
			continue
		}
		return &Error{
			Code: EInvalid,
			Op:   op,
			Msg:  fmt.Sprintf(ErrUnsupportTelegrafPluginName, pr.Name, pr.Type),
		}

	}
	return nil
}

var availableInputPlugins = map[string]TelegrafPluginConfig{
	"cpu":          &inputs.CPUStats{},
	"disk":         &inputs.DiskStats{},
	"diskio":       &inputs.DiskIO{},
	"docker":       &inputs.Docker{},
	"file":         &inputs.File{},
	"kernel":       &inputs.Kernel{},
	"kubernetes":   &inputs.Kubernetes{},
	"logparser":    &inputs.LogParserPlugin{},
	"mem":          &inputs.MemStats{},
	"net_response": &inputs.NetResponse{},
	"net":          &inputs.NetIOStats{},
	"ngnix":        &inputs.Nginx{},
	"processes":    &inputs.Processes{},
	"procstats":    &inputs.Procstat{},
	"prometheus":   &inputs.Prometheus{},
	"redis":        &inputs.Redis{},
	"swap":         &inputs.SwapStats{},
	"syslog":       &inputs.Syslog{},
	"system":       &inputs.SystemStats{},
	"tail":         &inputs.Tail{},
}

var availableOutputPlugins = map[string]TelegrafPluginConfig{
	"file":        &outputs.File{},
	"influxdb_v2": &outputs.InfluxDBV2{},
}
