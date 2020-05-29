package inputs

import (
	"errors"
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

// UnmarshalTOML decodes the parsed data to the object
func (d *Docker) UnmarshalTOML(data interface{}) error {
	dataOK, ok := data.(map[string]interface{})
	if !ok {
		return errors.New("bad endpoint for docker input plugin")
	}
	d.Endpoint, _ = dataOK["endpoint"].(string)
	return nil
}

// TOML encodes to toml string
func (d *Docker) TOML() string {
	return fmt.Sprintf(`[[inputs.%s]]	
  ## Docker Endpoint
  ##   To use TCP, set endpoint = "tcp://[ip]:[port]"
  ##   To use environment variables (ie, docker-machine), set endpoint = "ENV"
  ##   exp: unix:///var/run/docker.sock
  endpoint = "%s"

  ## Set to true to collect Swarm metrics(desired_replicas, running_replicas)
  gather_services = false

  ## Only collect metrics for these containers, collect all if empty
  container_names = []

  ## Containers to include and exclude. Globs accepted.
  ## Note that an empty array for both will include all containers
  container_name_include = []
  container_name_exclude = []

  ## Container states to include and exclude. Globs accepted.
  ## When empty only containers in the "running" state will be captured.
  # container_state_include = []
  # container_state_exclude = []

  ## Timeout for docker list, info, and stats commands
  timeout = "5s"

  ## Whether to report for each container per-device blkio (8:0, 8:1...) and
  ## network (eth0, eth1, ...) stats or not
  perdevice = true

  ## Whether to report for each container total blkio and network stats or not
  total = false
  
  ## Which environment variables should we use as a tag
  ##tag_env = ["JAVA_HOME", "HEAP_SIZE"]
  ## docker labels to include and exclude as tags.  Globs accepted.
  ## Note that an empty array for both will include all labels as tags
  docker_label_include = []
  docker_label_exclude = []
`, d.PluginName(), d.Endpoint)
}
