package inputs

import (
	"fmt"
)

// CPUStats is based on telegraf CPUStats.
type CPUStats struct {
	baseInput
}

// PluginName is based on telegraf plugin name.
func (c *CPUStats) PluginName() string {
	return "cpu"
}

// TOML encodes to toml string
func (c *CPUStats) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
`, c.PluginName())
}
