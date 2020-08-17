# Docker Log Input Plugin

The docker log plugin uses the Docker Engine API to get logs on running
docker containers.

The docker plugin uses the [Official Docker Client][] to gather logs from the
[Engine API][].

**Note:** This plugin works only for containers with the `local` or
`json-file` or `journald` logging driver.

[Official Docker Client]: https://github.com/moby/moby/tree/master/client
[Engine API]: https://docs.docker.com/engine/api/v1.24/

### Configuration

```toml
[[inputs.docker_log]]
  ## Docker Endpoint
  ##   To use TCP, set endpoint = "tcp://[ip]:[port]"
  ##   To use environment variables (ie, docker-machine), set endpoint = "ENV"
  # endpoint = "unix:///var/run/docker.sock"

  ## When true, container logs are read from the beginning; otherwise
  ## reading begins at the end of the log.
  # from_beginning = false

  ## Timeout for Docker API calls.
  # timeout = "5s"

  ## Containers to include and exclude. Globs accepted.
  ## Note that an empty array for both will include all containers
  # container_name_include = []
  # container_name_exclude = []

  ## Container states to include and exclude. Globs accepted.
  ## When empty only containers in the "running" state will be captured.
  # container_state_include = []
  # container_state_exclude = []

  ## docker labels to include and exclude as tags.  Globs accepted.
  ## Note that an empty array for both will include all labels as tags
  # docker_label_include = []
  # docker_label_exclude = []

  ## Set the source tag for the metrics to the container ID hostname, eg first 12 chars
  source_tag = false

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

#### Environment Configuration

When using the `"ENV"` endpoint, the connection is configured using the
[CLI Docker environment variables][env]

[env]: https://godoc.org/github.com/moby/moby/client#NewEnvClient

### source tag

Selecting the containers can be tricky if you have many containers with the same name.
To alleviate this issue you can set the below value to `true`

```toml
source_tag = true
```

This will cause all data points to have the `source` tag be set to the first 12 characters of the container id. The first 12 characters is the common hostname for containers that have no explicit hostname set, as defined by docker.

### Metrics

- docker_log
  - tags:
    - container_image
    - container_version
    - container_name
    - stream (stdout, stderr, or tty)
    - source
  - fields:
    - container_id
    - message

### Example Output

```
docker_log,container_image=telegraf,container_name=sharp_bell,container_version=alpine,stream=stderr container_id="371ee5d3e58726112f499be62cddef800138ca72bbba635ed2015fbf475b1023",message="2019-06-19T03:11:11Z I! [agent] Config: Interval:10s, Quiet:false, Hostname:\"371ee5d3e587\", Flush Interval:10s" 1560913872000000000
docker_log,container_image=telegraf,container_name=sharp_bell,container_version=alpine,stream=stderr container_id="371ee5d3e58726112f499be62cddef800138ca72bbba635ed2015fbf475b1023",message="2019-06-19T03:11:11Z I! Tags enabled: host=371ee5d3e587" 1560913872000000000
docker_log,container_image=telegraf,container_name=sharp_bell,container_version=alpine,stream=stderr container_id="371ee5d3e58726112f499be62cddef800138ca72bbba635ed2015fbf475b1023",message="2019-06-19T03:11:11Z I! Loaded outputs: file" 1560913872000000000
docker_log,container_image=telegraf,container_name=sharp_bell,container_version=alpine,stream=stderr container_id="371ee5d3e58726112f499be62cddef800138ca72bbba635ed2015fbf475b1023",message="2019-06-19T03:11:11Z I! Loaded processors:" 1560913872000000000
docker_log,container_image=telegraf,container_name=sharp_bell,container_version=alpine,stream=stderr container_id="371ee5d3e58726112f499be62cddef800138ca72bbba635ed2015fbf475b1023",message="2019-06-19T03:11:11Z I! Loaded aggregators:" 1560913872000000000
docker_log,container_image=telegraf,container_name=sharp_bell,container_version=alpine,stream=stderr container_id="371ee5d3e58726112f499be62cddef800138ca72bbba635ed2015fbf475b1023",message="2019-06-19T03:11:11Z I! Loaded inputs: net" 1560913872000000000
docker_log,container_image=telegraf,container_name=sharp_bell,container_version=alpine,stream=stderr container_id="371ee5d3e58726112f499be62cddef800138ca72bbba635ed2015fbf475b1023",message="2019-06-19T03:11:11Z I! Using config file: /etc/telegraf/telegraf.conf" 1560913872000000000
docker_log,container_image=telegraf,container_name=sharp_bell,container_version=alpine,stream=stderr container_id="371ee5d3e58726112f499be62cddef800138ca72bbba635ed2015fbf475b1023",message="2019-06-19T03:11:11Z I! Starting Telegraf 1.10.4" 1560913872000000000
```
