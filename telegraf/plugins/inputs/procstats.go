package inputs

import "fmt"

// Procstat is based on telegraf procstat input plugin.
type Procstat struct {
	baseInput
	Exe string `json:"exe,omitempty"`
}

// PluginName is based on telegraf plugin name.
func (p *Procstat) PluginName() string {
	return "procstat"
}

// TOML encodes to toml string.
func (p *Procstat) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
  ## executable name (ie, pgrep <exe>)
  exe = "%s"
`, p.PluginName(), p.Exe)
}
