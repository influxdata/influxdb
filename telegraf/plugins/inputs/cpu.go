package inputs

import (
	"fmt"
)

// CPUStats is based on telegraf CPUStats.
type CPUStats struct {
	baseInput
}

// PluginName is based on telegraf plugin name.
func (c *CPUStats) PluginName() string {
	return "cpu"
}

// UnmarshalTOML decodes the parsed data to the object
func (c *CPUStats) UnmarshalTOML(data interface{}) error {
	return nil
}

// TOML encodes to toml string
func (c *CPUStats) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states
  report_active = false
`, c.PluginName())
}
