package inputs

import (
	"fmt"
)

// NetIOStats is based on telegraf NetIOStats.
type NetIOStats struct {
	baseInput
}

// PluginName is based on telegraf plugin name.
func (n *NetIOStats) PluginName() string {
	return "net"
}

// TOML encodes to toml string
func (n *NetIOStats) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
`, n.PluginName())
}

// UnmarshalTOML decodes the parsed data to the object
func (n *NetIOStats) UnmarshalTOML(data interface{}) error {
	return nil
}
