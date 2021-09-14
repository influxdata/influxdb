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
  endpoint = "%s"
  #
  ## Set to true to collect Swarm metrics(desired_replicas, running_replicas)
  gather_services = false
  #
  ## Only collect metrics for these containers, collect all if empty
  container_names = []
  #
  ## Set the source tag for the metrics to the container ID hostname, eg first 12 chars
  source_tag = false
  #
  ## Containers to include and exclude. Globs accepted.
  ## Note that an empty array for both will include all containers
  container_name_include = []
  container_name_exclude = []
  #
  ## Container states to include and exclude. Globs accepted.
  ## When empty only containers in the "running" state will be captured.
  ## example: container_state_include = ["created", "restarting", "running", "removing", "paused", "exited", "dead"]
  ## example: container_state_exclude = ["created", "restarting", "running", "removing", "paused", "exited", "dead"]
  # container_state_include = []
  # container_state_exclude = []
  #
  ## Timeout for docker list, info, and stats commands
  timeout = "5s"
  #
  ## Whether to report for each container per-device blkio (8:0, 8:1...),
  ## network (eth0, eth1, ...) and cpu (cpu0, cpu1, ...) stats or not.
  ## Usage of this setting is discouraged since it will be deprecated in favor of 'perdevice_include'.
  ## Default value is 'true' for backwards compatibility, please set it to 'false' so that 'perdevice_include' setting
  ## is honored.
  perdevice = true
  #
  ## Specifies for which classes a per-device metric should be issued
  ## Possible values are 'cpu' (cpu0, cpu1, ...), 'blkio' (8:0, 8:1, ...) and 'network' (eth0, eth1, ...)
  ## Please note that this setting has no effect if 'perdevice' is set to 'true'
  # perdevice_include = ["cpu"]
  #
  ## Whether to report for each container total blkio and network stats or not.
  ## Usage of this setting is discouraged since it will be deprecated in favor of 'total_include'.
  ## Default value is 'false' for backwards compatibility, please set it to 'true' so that 'total_include' setting
  ## is honored.
  total = false
  #
  ## Specifies for which classes a total metric should be issued. Total is an aggregated of the 'perdevice' values.
  ## Possible values are 'cpu', 'blkio' and 'network'
  ## Total 'cpu' is reported directly by Docker daemon, and 'network' and 'blkio' totals are aggregated by this plugin.
  ## Please note that this setting has no effect if 'total' is set to 'false'
  # total_include = ["cpu", "blkio", "network"]
  #
  ## Which environment variables should we use as a tag
  ##tag_env = ["JAVA_HOME", "HEAP_SIZE"]
  #
  ## docker labels to include and exclude as tags.  Globs accepted.
  ## Note that an empty array for both will include all labels as tags
  docker_label_include = []
  docker_label_exclude = []
  #
  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`, d.PluginName(), d.Endpoint)
}
