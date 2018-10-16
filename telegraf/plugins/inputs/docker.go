package inputs

import (
	"fmt"
)

// Docker is based on telegraf Docker plugin.
type Docker struct {
	baseInput
	Endpoint string `json:"endpoint"`
}

// PluginName is based on telegraf plugin name.
func (d *Docker) PluginName() string {
	return "docker"
}

// TOML encodes to toml string
func (d *Docker) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]	
  ## Docker Endpoint
  ##   To use TCP, set endpoint = "tcp://[ip]:[port]"
  ##   To use environment variables (ie, docker-machine), set endpoint = "ENV"
  ##   exp: unix:///var/run/docker.sock
  endpoint = "%s"
`, d.PluginName(), d.Endpoint)
}
