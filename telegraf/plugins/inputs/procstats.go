package inputs

import (
	"errors"
	"fmt"
)

// Procstat is based on telegraf procstat input plugin.
type Procstat struct {
	baseInput
	Exe string `json:"exe"`
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

// UnmarshalTOML decodes the parsed data to the object
func (p *Procstat) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad exe for procstat input plugin")
	}
	p.Exe, _ = dataOK["exe"].(string)
	return nil
}
