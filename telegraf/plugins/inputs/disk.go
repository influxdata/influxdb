package inputs

import (
	"fmt"
)

// DiskStats is based on telegraf DiskStats.
type DiskStats struct {
	baseInput
}

// PluginName is based on telegraf plugin name.
func (d *DiskStats) PluginName() string {
	return "disk"
}

// TOML encodes to toml string
func (d *DiskStats) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
`, d.PluginName())
}
