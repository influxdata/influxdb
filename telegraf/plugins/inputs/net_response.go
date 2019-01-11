package inputs

import (
	"fmt"
)

// NetResponse is based on telegraf NetResponse.
type NetResponse struct {
	baseInput
}

// PluginName is based on telegraf plugin name.
func (n *NetResponse) PluginName() string {
	return "net_response"
}

// TOML encodes to toml string
func (n *NetResponse) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
  ## Protocol, must be "tcp" or "udp"
  ## NOTE: because the "udp" protocol does not respond to requests, it requires
  ## a send/expect string pair (see below).
  protocol = "tcp"
  ## Server address (default localhost)
  address = "localhost:80"
`, n.PluginName())
}

// UnmarshalTOML decodes the parsed data to the object
func (n *NetResponse) UnmarshalTOML(data interface{}) error {
	return nil
}
