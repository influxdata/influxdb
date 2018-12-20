package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/platform/telegraf/plugins"
	"github.com/influxdata/platform/telegraf/plugins/inputs"
	"github.com/influxdata/platform/telegraf/plugins/outputs"
)

// TelegrafConfigStore represents a service for managing telegraf config data.
type TelegrafConfigStore interface {
	// UserResourceMappingService must be part of all TelegrafConfigStore service,
	// for create, search, delete.
	UserResourceMappingService

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

// TOML returns the telegraf toml config string.
func (tc TelegrafConfig) TOML() string {
	plugins := ""
	for _, p := range tc.Plugins {
		plugins += p.Config.TOML()
	}
	interval := time.Duration(tc.Agent.Interval * 1000000)
	return fmt.Sprintf(`# Configuration for telegraf agent
[agent]
  ## Default data collection interval for all inputs
  interval = "%s"
  ## Rounds collection interval to 'interval'
  ## ie, if interval="10s" then always collect on :00, :10, :20, etc.
  round_interval = true

  ## Telegraf will send metrics to outputs in batches of at most
  ## metric_batch_size metrics.
  ## This controls the size of writes that Telegraf sends to output plugins.
  metric_batch_size = 1000

  ## For failed writes, telegraf will cache metric_buffer_limit metrics for each
  ## output, and will flush this buffer on a successful write. Oldest metrics
  ## are dropped first when this buffer fills.
  ## This buffer only fills when writes fail to output plugin(s).
  metric_buffer_limit = 10000

  ## Collection jitter is used to jitter the collection by a random amount.
  ## Each plugin will sleep for a random time within jitter before collecting.
  ## This can be used to avoid many plugins querying things like sysfs at the
  ## same time, which can have a measurable effect on the system.
  collection_jitter = "0s"

  ## Default flushing interval for all outputs. Maximum flush_interval will be
  ## flush_interval + flush_jitter
  flush_interval = "10s"
  ## Jitter the flush interval by a random amount. This is primarily to avoid
  ## large write spikes for users running a large number of telegraf instances.
  ## ie, a jitter of 5s and interval 10s means flushes will happen every 10-15s
  flush_jitter = "0s"

  ## By default or when set to "0s", precision will be set to the same
  ## timestamp order as the collection interval, with the maximum being 1s.
  ##   ie, when interval = "10s", precision will be "1s"
  ##       when interval = "250ms", precision will be "1ms"
  ## Precision will NOT be used for service inputs. It is up to each individual
  ## service input to set the timestamp at the appropriate precision.
  ## Valid time units are "ns", "us" (or "µs"), "ms", "s".
  precision = ""

  ## Logging configuration:
  ## Run telegraf with debug log messages.
  debug = false
  ## Run telegraf in quiet mode (error log messages only).
  quiet = false
  ## Specify the log file name. The empty string means to log to stderr.
  logfile = ""

  ## Override default hostname, if empty use os.Hostname()
  hostname = ""
  ## If set to true, do no set the "host" tag in the telegraf agent.
  omit_hostname = false
%s`, interval.String(), plugins)
}

// telegrafConfigEncode is the helper struct for json encoding.
type telegrafConfigEncode struct {
	ID        ID        `json:"id"`
	Name      string    `json:"name"`
	Created   time.Time `json:"created"`
	LastMod   time.Time `json:"lastModified"`
	LastModBy ID        `json:"lastModifiedBy"`

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
	LastMod   time.Time `json:"lastModified"`
	LastModBy ID        `json:"lastModifiedBy"`

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
	// UnmarshalTOML decodes the parsed data to the object
	UnmarshalTOML(data interface{}) error
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

// UnmarshalTOML implements toml.Unmarshaler interface.
func (tc *TelegrafConfig) UnmarshalTOML(data interface{}) error {
	dataOk, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("blank string")
	}
	agent, ok := dataOk["agent"].(map[string]interface{})
	if !ok {
		return errors.New("agent is missing")
	}

	intervalStr, ok := agent["interval"].(string)
	if !ok {
		return errors.New("agent interval is not string")
	}

	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return err
	}
	tc.Agent = TelegrafAgentConfig{
		Interval: interval.Nanoseconds() / 1000000,
	}

	for tp, ps := range dataOk {
		if tp == "agent" {
			continue
		}
		plugins, ok := ps.(map[string]interface{})
		if !ok {
			return &Error{
				Msg: "bad plugin type",
			}
		}
		for name, configDataArray := range plugins {
			if configDataArray == nil {
				if err := tc.parseTOMLPluginConfig(tp, name, configDataArray); err != nil {
					return err
				}
				continue
			}
			for _, configData := range configDataArray.([]map[string]interface{}) {
				if err := tc.parseTOMLPluginConfig(tp, name, configData); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (tc *TelegrafConfig) parseTOMLPluginConfig(typ, name string, configData interface{}) error {
	var ok bool
	var p TelegrafPluginConfig
	switch typ {
	case "inputs":
		p, ok = availableInputPlugins[name]
	case "outputs":
		p, ok = availableOutputPlugins[name]
	default:
		return &Error{
			Msg: fmt.Sprintf(ErrUnsupportTelegrafPluginType, typ),
		}
	}
	if !ok {
		return &Error{
			Msg: fmt.Sprintf(ErrUnsupportTelegrafPluginName, name, typ),
		}
	}
	if err := p.UnmarshalTOML(configData); err != nil {
		return err
	}
	tc.Plugins = append(tc.Plugins, TelegrafPlugin{
		Config: p,
	})
	return nil
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
