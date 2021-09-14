package inputs

import (
	"fmt"
)

// Kernel is based on telegraf Kernel.
type Kernel struct {
	baseInput
}

// PluginName is based on telegraf plugin name.
func (k *Kernel) PluginName() string {
	return "kernel"
}

// TOML encodes to toml string
func (k *Kernel) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
  # no configuration
`, k.PluginName())
}

// UnmarshalTOML decodes the parsed data to the object
func (k *Kernel) UnmarshalTOML(data interface{}) error {
	return nil
}
