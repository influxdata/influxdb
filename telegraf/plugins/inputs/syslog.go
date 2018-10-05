package inputs

import (
	"fmt"
)

// Syslog is based on telegraf Syslog plugin.
type Syslog struct {
	Address string `json:"server"`
}

// TOML encodes to toml string
func (s *Syslog) TOML() string {
	return fmt.Sprintf(`[[inputs.syslog]]
  ## Specify an ip or hostname with port - eg., tcp://localhost:6514, tcp://10.0.0.1:6514
  ## Protocol, address and port to host the syslog receiver.
  ## If no host is specified, then localhost is used.
  ## If no port is specified, 6514 is used (RFC5425#section-4.1).
  server = "%s"	
`, s.Address)
}
