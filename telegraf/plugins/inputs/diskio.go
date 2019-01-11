package inputs

import (
	"fmt"
)

// DiskIO is based on telegraf DiskIO.
type DiskIO struct {
	baseInput
}

// PluginName is based on telegraf plugin name.
func (d *DiskIO) PluginName() string {
	return "diskio"
}

// UnmarshalTOML decodes the parsed data to the object
func (d *DiskIO) UnmarshalTOML(data interface{}) error {
	return nil
}

// TOML encodes to toml string.
func (d *DiskIO) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
`, d.PluginName())
}
