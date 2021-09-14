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

  ## Set timeout
  # timeout = "1s"

  ## Set read timeout (only used if expecting a response)
  # read_timeout = "1s"

  ## The following options are required for UDP checks. For TCP, they are
  ## optional. The plugin will send the given string to the server and then
  ## expect to receive the given 'expect' string back.
  ## string sent to the server
  # send = "ssh"
  ## expected string in answer
  # expect = "ssh"

  ## Uncomment to remove deprecated fields
  # fielddrop = ["result_type", "string_found"]
`, n.PluginName())
}

// UnmarshalTOML decodes the parsed data to the object
func (n *NetResponse) UnmarshalTOML(data interface{}) error {
	return nil
}
