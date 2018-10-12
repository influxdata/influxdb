package inputs

import (
	"fmt"
)

// Docker is based on telegraf Docker plugin.
type Docker struct {
	Endpoint string `json:"endpoint"`
}

// TOML encodes to toml string
func (d *Docker) TOML() string {
	return fmt.Sprintf(`[[inputs.docker]]	
  ## Docker Endpoint
  ##   To use TCP, set endpoint = "tcp://[ip]:[port]"
  ##   To use environment variables (ie, docker-machine), set endpoint = "ENV"
  ##   exp: unix:///var/run/docker.sock
  endpoint = "%s"
`, d.Endpoint)
}
