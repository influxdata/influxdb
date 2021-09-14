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
  ## By default, telegraf gathers stats from any up interface (excluding loopback)
  ## Setting interfaces will tell it to gather these explicit interfaces,
  ## regardless of status.
  ##
  # interfaces = ["eth0"]
  ##
  ## On linux systems telegraf also collects protocol stats.
  ## Setting ignore_protocol_stats to true will skip reporting of protocol metrics.
  ##
  # ignore_protocol_stats = false
  ##
`, n.PluginName())
}

// UnmarshalTOML decodes the parsed data to the object
func (n *NetIOStats) UnmarshalTOML(data interface{}) error {
	return nil
}
