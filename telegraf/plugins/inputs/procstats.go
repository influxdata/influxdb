package inputs

import "fmt"

// Procstat is based on telegraf procstat input plugin.
type Procstat struct {
	Exe string `json:"exe,omitempty"`
}

// TOML encodes to toml string.
func (p *Procstat) TOML() string {
	return fmt.Sprintf(`[[inputs.procstat]]
  ## executable name (ie, pgrep <exe>)
  exe = "%s"
`, p.Exe)
}
