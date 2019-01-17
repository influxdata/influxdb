package influxdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/telegraf/plugins"
	"github.com/influxdata/influxdb/telegraf/plugins/inputs"
	"github.com/influxdata/influxdb/telegraf/plugins/outputs"
)

// ErrTelegrafConfigInvalidOrganizationID is the error message for a missing or invalid organization ID.
const ErrTelegrafConfigInvalidOrganizationID = "invalid organization ID"

// ErrTelegrafConfigNotFound is the error message for a missing telegraf config.
const ErrTelegrafConfigNotFound = "telegraf config not found"

// ops for buckets error and buckets op logs.
var (
	OpFindTelegrafConfigByID = "FindTelegrafConfigByID"
	OpFindTelegrafConfig     = "FindTelegrafConfig"
	OpFindTelegrafConfigs    = "FindTelegrafConfigs"
	OpCreateTelegrafConfig   = "CreateTelegrafConfig"
	OpUpdateTelegrafConfig   = "UpdateTelegrafConfig"
	OpDeleteTelegrafConfig   = "DeleteTelegrafConfig"
)

// TelegrafConfigStore represents a service for managing telegraf config data.
type TelegrafConfigStore interface {
	// UserResourceMappingService must be part of all TelegrafConfigStore service,
	// for create, search, delete.
	UserResourceMappingService

	// FindTelegrafConfigByID returns a single telegraf config by ID.
	FindTelegrafConfigByID(ctx context.Context, id ID) (*TelegrafConfig, error)

	// FindTelegrafConfig returns the first telegraf config that matches filter.
	FindTelegrafConfig(ctx context.Context, filter TelegrafConfigFilter) (*TelegrafConfig, error)

	// FindTelegrafConfigs returns a list of telegraf configs that match filter and the total count of matching telegraf configs.
	// Additional options provide pagination & sorting.
	FindTelegrafConfigs(ctx context.Context, filter TelegrafConfigFilter, opt ...FindOptions) ([]*TelegrafConfig, int, error)

	// CreateTelegrafConfig creates a new telegraf config and sets b.ID with the new identifier.
	CreateTelegrafConfig(ctx context.Context, tc *TelegrafConfig, userID ID) error

	// UpdateTelegrafConfig updates a single telegraf config.
	// Returns the new telegraf config after update.
	UpdateTelegrafConfig(ctx context.Context, id ID, tc *TelegrafConfig, userID ID) (*TelegrafConfig, error)

	// DeleteTelegrafConfig removes a telegraf config by ID.
	DeleteTelegrafConfig(ctx context.Context, id ID) error
}

// TelegrafConfigFilter represents a set of filter that restrict the returned telegraf configs.
type TelegrafConfigFilter struct {
	OrganizationID *ID
	Organization   *string
	UserResourceMappingFilter
}

// TelegrafConfig stores telegraf config for one telegraf instance.
type TelegrafConfig struct {
	ID             ID
	OrganizationID ID
	Name           string

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
	ID             ID     `json:"id"`
	OrganizationID ID     `json:"organizationID,omitempty"`
	Name           string `json:"name"`

	Agent TelegrafAgentConfig `json:"agent"`

	Plugins []telegrafPluginEncode `json:"plugins"`
}

// telegrafPluginEncode is the helper struct for json encoding.
type telegrafPluginEncode struct {
	// Name of the telegraf plugin, exp "docker"
	Name    string         `json:"name"`
	Type    plugins.Type   `json:"type"`
	Comment string         `json:"comment"`
	Config  plugins.Config `json:"config"`
}

// telegrafConfigDecode is the helper struct for json decoding.
type telegrafConfigDecode struct {
	ID             ID     `json:"id"`
	OrganizationID ID     `json:"organizationID,omitempty"`
	Name           string `json:"name"`

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
	Comment string         `json:"comment"`
	Config  plugins.Config `json:"config"`
}

// TelegrafAgentConfig is based telegraf/internal/config AgentConfig.
type TelegrafAgentConfig struct {
	// Interval at which to gather information in miliseconds.
	Interval int64 `json:"collectionInterval"`
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
		ID:             tc.ID,
		OrganizationID: tc.OrganizationID,
		Name:           tc.Name,
		Agent:          tc.Agent,
		Plugins:        make([]telegrafPluginEncode, len(tc.Plugins)),
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
	var tpFn func() plugins.Config
	switch typ {
	case "inputs":
		tpFn, ok = availableInputPlugins[name]
	case "outputs":
		tpFn, ok = availableOutputPlugins[name]
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
	p := tpFn()

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
		ID:             tcd.ID,
		OrganizationID: tcd.OrganizationID,
		Name:           tcd.Name,
		Agent:          tcd.Agent,
		Plugins:        make([]TelegrafPlugin, len(tcd.Plugins)),
	}
	return decodePluginRaw(tcd, tc)
}

func decodePluginRaw(tcd *telegrafConfigDecode, tc *TelegrafConfig) (err error) {
	op := "unmarshal telegraf config raw plugin"
	for k, pr := range tcd.Plugins {
		var tpFn func() plugins.Config
		var config plugins.Config
		var ok bool
		switch pr.Type {
		case plugins.Input:
			tpFn, ok = availableInputPlugins[pr.Name]
		case plugins.Output:
			tpFn, ok = availableOutputPlugins[pr.Name]
		default:
			return &Error{
				Code: EInvalid,
				Msg:  fmt.Sprintf(ErrUnsupportTelegrafPluginType, pr.Type),
				Op:   op,
			}
		}
		if ok {
			config = tpFn()
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

var availableInputPlugins = map[string](func() plugins.Config){
	"cpu":          func() plugins.Config { return &inputs.CPUStats{} },
	"disk":         func() plugins.Config { return &inputs.DiskStats{} },
	"diskio":       func() plugins.Config { return &inputs.DiskIO{} },
	"docker":       func() plugins.Config { return &inputs.Docker{} },
	"file":         func() plugins.Config { return &inputs.File{} },
	"kernel":       func() plugins.Config { return &inputs.Kernel{} },
	"kubernetes":   func() plugins.Config { return &inputs.Kubernetes{} },
	"logparser":    func() plugins.Config { return &inputs.LogParserPlugin{} },
	"mem":          func() plugins.Config { return &inputs.MemStats{} },
	"net_response": func() plugins.Config { return &inputs.NetResponse{} },
	"net":          func() plugins.Config { return &inputs.NetIOStats{} },
	"nginx":        func() plugins.Config { return &inputs.Nginx{} },
	"processes":    func() plugins.Config { return &inputs.Processes{} },
	"procstat":     func() plugins.Config { return &inputs.Procstat{} },
	"prometheus":   func() plugins.Config { return &inputs.Prometheus{} },
	"redis":        func() plugins.Config { return &inputs.Redis{} },
	"swap":         func() plugins.Config { return &inputs.SwapStats{} },
	"syslog":       func() plugins.Config { return &inputs.Syslog{} },
	"system":       func() plugins.Config { return &inputs.SystemStats{} },
	"tail":         func() plugins.Config { return &inputs.Tail{} },
}

var availableOutputPlugins = map[string](func() plugins.Config){
	"file":        func() plugins.Config { return &outputs.File{} },
	"influxdb_v2": func() plugins.Config { return &outputs.InfluxDBV2{} },
}
