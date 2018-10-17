package inputs

import (
	"errors"
	"fmt"
)

// Syslog is based on telegraf Syslog plugin.
type Syslog struct {
	baseInput
	Address string `json:"server"`
}

// PluginName is based on telegraf plugin name.
func (s *Syslog) PluginName() string {
	return "syslog"
}

// TOML encodes to toml string
func (s *Syslog) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
  ## Specify an ip or hostname with port - eg., tcp://localhost:6514, tcp://10.0.0.1:6514
  ## Protocol, address and port to host the syslog receiver.
  ## If no host is specified, then localhost is used.
  ## If no port is specified, 6514 is used (RFC5425#section-4.1).
  server = "%s"
`, s.PluginName(), s.Address)
}

// UnmarshalTOML decodes the parsed data to the object
func (s *Syslog) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad server for syslog input plugin")
	}
	s.Address, _ = dataOK["server"].(string)
	return nil
}
