package inputs

import (
	"fmt"
)

// SwapStats is based on telegraf SwapStats.
type SwapStats struct {
	baseInput
}

// PluginName is based on telegraf plugin name.
func (s *SwapStats) PluginName() string {
	return "swap"
}

// TOML encodes to toml string.
func (s *SwapStats) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
  # no configuration
`, s.PluginName())
}

// UnmarshalTOML decodes the parsed data to the object
func (s *SwapStats) UnmarshalTOML(data interface{}) error {
	return nil
}
