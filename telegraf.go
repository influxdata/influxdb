package influxdb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/telegraf/plugins"
	"github.com/influxdata/influxdb/telegraf/plugins/inputs"
	"github.com/influxdata/influxdb/telegraf/plugins/outputs"
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
	// UserResourceMappingService must be part of all TelegrafConfigStore service,
	// for create, search, delete.
	UserResourceMappingService

	// FindTelegrafConfigByID returns a single telegraf config by ID.
	FindTelegrafConfigByID(ctx context.Context, id ID) (*TelegrafConfig, error)

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
	OrgID        *ID
	Organization *string
	UserResourceMappingFilter
}

// TelegrafConfig stores telegraf config for one telegraf instance.
type TelegrafConfig struct {
	ID          ID     `json:"id,omitempty"`          // ID of this config object.
	OrgID       ID     `json:"orgID,omitempty"`       // OrgID is the id of the owning organization.
	Name        string `json:"name,omitempty"`        // Name of this config object.
	Description string `json:"description,omitempty"` // Decription of this config object.
	Config      string `json:"config,omitempty"`      // ConfigTOML contains the raw toml config.
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
	if !orgID.Valid() {
		orgID = tcd.OrganizationID
	}

	*tc = TelegrafConfig{
		ID:          tcd.ID,
		OrgID:       orgID,
		Name:        tcd.Name,
		Description: tcd.Description,
		Config:      tcd.Config,
	}

	// legacy, remove after some moons. or a migration.
	if lp := len(tcd.Plugins); lp > 0 {
		conf, err := decodePluginRaw(tcd)
		if err != nil {
			return err
		}
		tc.Config = plugins.AgentConfig + conf
	}

	return nil
}

func decodePluginRaw(tcd *telegrafConfigDecode) (string, error) {
	op := "unmarshal telegraf config raw plugin"
	ps := ""

	for _, pr := range tcd.Plugins {
		var tpFn func() plugins.Config
		var ok bool

		switch pr.Type {
		case "input":
			tpFn, ok = availableInputPlugins[pr.Name]
		case "output":
			tpFn, ok = availableOutputPlugins[pr.Name]
		default:
			// TODO(glinton): log something here
			continue
		}

		if !ok {
			return "", &Error{
				Code: EInvalid,
				Op:   op,
				Msg:  fmt.Sprintf(ErrUnsupportTelegrafPluginName, pr.Name, pr.Type),
			}
		}

		var config plugins.Config
		config = tpFn()
		// if pr.Config if empty, make it a blank obj,
		// so it will still go to the unmarshalling process to validate.
		if len(string(pr.Config)) == 0 {
			pr.Config = []byte("{}")
		}

		if err := json.Unmarshal(pr.Config, config); err != nil {
			return "", &Error{
				Code: EInvalid,
				Err:  err,
				Op:   op,
			}
		}

		ps += config.TOML()
	}

	return ps, nil
}

// telegrafConfigDecode is the helper struct for json decoding. legacy.
type telegrafConfigDecode struct {
	ID             ID                     `json:"id,omitempty"`
	OrganizationID ID                     `json:"organizationID,omitempty"`
	OrgID          ID                     `json:"orgID,omitempty"`
	Name           string                 `json:"name,omitempty"`
	Description    string                 `json:"description,omitempty"`
	Config         string                 `json:"config,omitempty"`
	Plugins        []telegrafPluginDecode `json:"plugins,omitempty"`
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
