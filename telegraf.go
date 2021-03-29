package influxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/v2/telegraf/plugins"
	"github.com/influxdata/influxdb/v2/telegraf/plugins/inputs"
	"github.com/influxdata/influxdb/v2/telegraf/plugins/outputs"
)

const (
	ErrTelegrafConfigInvalidOrgID  = "invalid org ID"                   // ErrTelegrafConfigInvalidOrgID is the error message for a missing or invalid organization ID.
	ErrTelegrafConfigNotFound      = "telegraf configuration not found" // ErrTelegrafConfigNotFound is the error message for a missing telegraf config.
	ErrTelegrafPluginNameUnmatch   = "the telegraf plugin is name %s doesn't match the config %s"
	ErrNoTelegrafPlugins           = "there is no telegraf plugin in the config"
	ErrUnsupportTelegrafPluginType = "unsupported telegraf plugin type %s"
	ErrUnsupportTelegrafPluginName = "unsupported telegraf plugin %s, type %s"
)

// ops for buckets error and buckets op logs.
var (
	OpFindTelegrafConfigByID = "FindTelegrafConfigByID"
	OpFindTelegrafConfigs    = "FindTelegrafConfigs"
	OpCreateTelegrafConfig   = "CreateTelegrafConfig"
	OpUpdateTelegrafConfig   = "UpdateTelegrafConfig"
	OpDeleteTelegrafConfig   = "DeleteTelegrafConfig"
)

// TelegrafConfigStore represents a service for managing telegraf config data.
type TelegrafConfigStore interface {
	// FindTelegrafConfigByID returns a single telegraf config by ID.
	FindTelegrafConfigByID(ctx context.Context, id platform.ID) (*TelegrafConfig, error)

	// FindTelegrafConfigs returns a list of telegraf configs that match filter and the total count of matching telegraf configs.
	// Additional options provide pagination & sorting.
	FindTelegrafConfigs(ctx context.Context, filter TelegrafConfigFilter, opt ...FindOptions) ([]*TelegrafConfig, int, error)

	// CreateTelegrafConfig creates a new telegraf config and sets b.ID with the new identifier.
	CreateTelegrafConfig(ctx context.Context, tc *TelegrafConfig, userID platform.ID) error

	// UpdateTelegrafConfig updates a single telegraf config.
	// Returns the new telegraf config after update.
	UpdateTelegrafConfig(ctx context.Context, id platform.ID, tc *TelegrafConfig, userID platform.ID) (*TelegrafConfig, error)

	// DeleteTelegrafConfig removes a telegraf config by ID.
	DeleteTelegrafConfig(ctx context.Context, id platform.ID) error
}

// TelegrafConfigFilter represents a set of filter that restrict the returned telegraf configs.
type TelegrafConfigFilter struct {
	OrgID        *platform.ID
	Organization *string
}

// TelegrafConfig stores telegraf config for one telegraf instance.
type TelegrafConfig struct {
	ID          platform.ID            `json:"id,omitempty"`          // ID of this config object.
	OrgID       platform.ID            `json:"orgID,omitempty"`       // OrgID is the id of the owning organization.
	Name        string                 `json:"name,omitempty"`        // Name of this config object.
	Description string                 `json:"description,omitempty"` // Decription of this config object.
	Config      string                 `json:"config,omitempty"`      // ConfigTOML contains the raw toml config.
	Metadata    map[string]interface{} `json:"metadata,omitempty"`    // Metadata for the config.
}

var pluginCount = regexp.MustCompilePOSIX(`\[\[(inputs\..*|outputs\..*|aggregators\..*|processors\..*)\]\]`)

// CountPlugins returns a map of the number of times each plugin is used.
func (tc *TelegrafConfig) CountPlugins() map[string]float64 {
	plugins := map[string]float64{}
	founds := pluginCount.FindAllStringSubmatch(tc.Config, -1)

	for _, v := range founds {
		if len(v) < 2 {
			continue
		}
		plugins[v[1]]++
	}

	return plugins
}

// UnmarshalJSON implement the json.Unmarshaler interface.
// Gets called when reading from the kv db. mostly legacy so loading old/stored configs still work.
// May not remove for a while. Primarily will get hit when user views/downloads config.
func (tc *TelegrafConfig) UnmarshalJSON(b []byte) error {
	tcd := new(telegrafConfigDecode)

	if err := json.Unmarshal(b, tcd); err != nil {
		return err
	}

	orgID := tcd.OrgID
	if orgID == nil || !orgID.Valid() {
		orgID = tcd.OrganizationID
	}

	if tcd.ID != nil {
		tc.ID = *tcd.ID
	}

	if orgID != nil {
		tc.OrgID = *orgID
	}

	tc.Name = tcd.Name
	tc.Description = tcd.Description

	// Prefer new structure; use full toml config.
	tc.Config = tcd.Config
	tc.Metadata = tcd.Metadata

	if tcd.Plugins != nil {
		// legacy, remove after some moons. or a migration.
		if len(tcd.Plugins) > 0 {
			bkts, conf, err := decodePluginRaw(tcd)
			if err != nil {
				return err
			}
			tc.Config = plugins.AgentConfig + conf
			tc.Metadata = map[string]interface{}{"buckets": bkts}
		} else if c, ok := plugins.GetPlugin("output", "influxdb_v2"); ok {
			// Handles legacy adding of default plugins (agent and output).
			tc.Config = plugins.AgentConfig + c.Config
			tc.Metadata = map[string]interface{}{
				"buckets": []string{},
			}
		}
	} else if tcd.Metadata == nil || len(tcd.Metadata) == 0 {
		// Get buckets from the config.
		m, err := parseMetadata(tc.Config)
		if err != nil {
			return err
		}

		tc.Metadata = m
	}

	return nil
}

type buckets []string

func (t *buckets) UnmarshalTOML(data interface{}) error {
	dataOk, ok := data.(map[string]interface{})
	if !ok {
		return &errors.Error{
			Code: errors.EEmptyValue,
			Msg:  "no config to get buckets",
		}
	}
	bkts := []string{}
	for tp, ps := range dataOk {
		if tp != "outputs" {
			continue
		}
		plugins, ok := ps.(map[string]interface{})
		if !ok {
			return &errors.Error{
				Code: errors.EEmptyValue,
				Msg:  "no plugins in config to get buckets",
			}
		}
		for name, configDataArray := range plugins {
			if name != "influxdb_v2" {
				continue
			}
			config, ok := configDataArray.([]map[string]interface{})
			if !ok {
				return &errors.Error{
					Code: errors.EEmptyValue,
					Msg:  "influxdb_v2 output has no config",
				}
			}
			for i := range config {
				if b, ok := config[i]["bucket"]; ok {
					bkts = append(bkts, b.(string))
				}
			}
		}
	}

	*t = bkts

	return nil
}

func parseMetadata(cfg string) (map[string]interface{}, error) {
	bs := []string{}

	this := &buckets{}
	_, err := toml.Decode(cfg, this)
	if err != nil {
		return nil, err
	}

	for _, i := range *this {
		if i != "" {
			bs = append(bs, i)
		}
	}

	return map[string]interface{}{"buckets": bs}, nil
}

// return bucket, config, error
func decodePluginRaw(tcd *telegrafConfigDecode) ([]string, string, error) {
	op := "unmarshal telegraf config raw plugin"
	ps := ""
	bucket := []string{}

	for _, pr := range tcd.Plugins {
		var tpFn func() plugins.Config
		var ok bool

		switch pr.Type {
		case "input":
			tpFn, ok = availableInputPlugins[pr.Name]
		case "output":
			tpFn, ok = availableOutputPlugins[pr.Name]
		default:
			return nil, "", &errors.Error{
				Code: errors.EInvalid,
				Op:   op,
				Msg:  fmt.Sprintf(ErrUnsupportTelegrafPluginType, pr.Type),
			}
		}

		if !ok {
			return nil, "", &errors.Error{
				Code: errors.EInvalid,
				Op:   op,
				Msg:  fmt.Sprintf(ErrUnsupportTelegrafPluginName, pr.Name, pr.Type),
			}
		}

		config := tpFn()
		// if pr.Config if empty, make it a blank obj,
		// so it will still go to the unmarshalling process to validate.
		if len(string(pr.Config)) == 0 {
			pr.Config = []byte("{}")
		}

		if err := json.Unmarshal(pr.Config, config); err != nil {
			return nil, "", &errors.Error{
				Code: errors.EInvalid,
				Err:  err,
				Op:   op,
			}
		}

		if pr.Name == "influxdb_v2" {
			if b := config.(*outputs.InfluxDBV2).Bucket; b != "" {
				bucket = []string{b}
			}
		}

		ps += config.TOML()
	}

	return bucket, ps, nil
}

// telegrafConfigDecode is the helper struct for json decoding. legacy.
type telegrafConfigDecode struct {
	ID             *platform.ID           `json:"id,omitempty"`
	OrganizationID *platform.ID           `json:"organizationID,omitempty"`
	OrgID          *platform.ID           `json:"orgID,omitempty"`
	Name           string                 `json:"name,omitempty"`
	Description    string                 `json:"description,omitempty"`
	Config         string                 `json:"config,omitempty"`
	Plugins        []telegrafPluginDecode `json:"plugins,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

// telegrafPluginDecode is the helper struct for json decoding. legacy.
type telegrafPluginDecode struct {
	Type        string          `json:"type,omitempty"`        // Type of the plugin.
	Name        string          `json:"name,omitempty"`        // Name of the plugin.
	Alias       string          `json:"alias,omitempty"`       // Alias of the plugin.
	Description string          `json:"description,omitempty"` // Description of the plugin.
	Config      json.RawMessage `json:"config,omitempty"`      // Config is the currently stored plugin configuration.
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
