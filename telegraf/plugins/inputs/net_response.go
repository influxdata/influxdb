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
`, n.PluginName())
}
