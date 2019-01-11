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

// UnmarshalTOML decodes the parsed data to the object
func (d *DiskStats) UnmarshalTOML(data interface{}) error {
	return nil
}

// TOML encodes to toml string
func (d *DiskStats) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
  ## By default stats will be gathered for all mount points.
  ## Set mount_points will restrict the stats to only the specified mount points.
  # mount_points = ["/"]
  ## Ignore mount points by filesystem type.
  ignore_fs = ["tmpfs", "devtmpfs", "devfs", "overlay", "aufs", "squashfs"]
`, d.PluginName())
}
