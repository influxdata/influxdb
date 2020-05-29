package inputs

import (
	"fmt"
)

// Processes is based on telegraf Processes.
type Processes struct {
	baseInput
}

// PluginName is based on telegraf plugin name.
func (p *Processes) PluginName() string {
	return "processes"
}

// TOML encodes to toml string
func (p *Processes) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
`, p.PluginName())
}

// UnmarshalTOML decodes the parsed data to the object
func (p *Processes) UnmarshalTOML(data interface{}) error {
	return nil
}
