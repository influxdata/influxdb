package inputs

import (
	"fmt"
)

// SystemStats is based on telegraf SystemStats.
type SystemStats struct {
	baseInput
}

// PluginName is based on telegraf plugin name.
func (s *SystemStats) PluginName() string {
	return "system"
}

// TOML encodes to toml string
func (s *SystemStats) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
  ## Uncomment to remove deprecated metrics.
  # fielddrop = ["uptime_format"]
`, s.PluginName())
}

// UnmarshalTOML decodes the parsed data to the object
func (s *SystemStats) UnmarshalTOML(data interface{}) error {
	return nil
}
