# Docker Input Plugin

The docker plugin uses the Docker Engine API to gather metrics on running
docker containers.

The docker plugin uses the [Official Docker Client](https://github.com/moby/moby/tree/master/client)
to gather stats from the [Engine API](https://docs.docker.com/engine/api/v1.24/).

### Configuration:

```toml
# Read metrics about docker containers
[[inputs.docker]]
  ## Docker Endpoint
  ##   To use TCP, set endpoint = "tcp://[ip]:[port]"
  ##   To use environment variables (ie, docker-machine), set endpoint = "ENV"
  endpoint = "unix:///var/run/docker.sock"

  ## Set to true to collect Swarm metrics(desired_replicas, running_replicas)
  ## Note: configure this in one of the manager nodes in a Swarm cluster.
  ## configuring in multiple Swarm managers results in duplication of metrics.
  gather_services = false

  ## Only collect metrics for these containers. Values will be appended to
  ## container_name_include.
  ## Deprecated (1.4.0), use container_name_include
  container_names = []

  ## Set the source tag for the metrics to the container ID hostname, eg first 12 chars
  source_tag = false

  ## Containers to include and exclude. Collect all if empty. Globs accepted.
  container_name_include = []
  container_name_exclude = []

  ## Container states to include and exclude. Globs accepted.
  ## When empty only containers in the "running" state will be captured.
  ## example: container_state_include = ["created", "restarting", "running", "removing", "paused", "exited", "dead"]
  ## example: container_state_exclude = ["created", "restarting", "running", "removing", "paused", "exited", "dead"]
  # container_state_include = []
  # container_state_exclude = []

  ## Timeout for docker list, info, and stats commands
  timeout = "5s"

  ## Whether to report for each container per-device blkio (8:0, 8:1...) and
  ## network (eth0, eth1, ...) stats or not
  perdevice = true

  ## Whether to report for each container total blkio and network stats or not
  total = false

  ## docker labels to include and exclude as tags.  Globs accepted.
  ## Note that an empty array for both will include all labels as tags
  docker_label_include = []
  docker_label_exclude = []

  ## Which environment variables should we use as a tag
  tag_env = ["JAVA_HOME", "HEAP_SIZE"]

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

#### Environment Configuration

When using the `"ENV"` endpoint, the connection is configured using the
[cli Docker environment variables](https://godoc.org/github.com/moby/moby/client#NewEnvClient).

#### Security

Giving telegraf access to the Docker daemon expands the [attack surface](https://docs.docker.com/engine/security/security/#docker-daemon-attack-surface) that could result in an attacker gaining root access to a machine. This is especially relevant if the telegraf configuration can be changed by untrusted users.

#### Docker Daemon Permissions

Typically, telegraf must be given permission to access the docker daemon unix
socket when using the default endpoint. This can be done by adding the
`telegraf` unix user (created when installing a Telegraf package) to the
`docker` unix group with the following command:

```
sudo usermod -aG docker telegraf
```

If telegraf is run within a container, the unix socket will need to be exposed
within the telegraf container. This can be done in the docker CLI by add the
option `-v /var/run/docker.sock:/var/run/docker.sock` or adding the following
lines to the telegraf container definition in a docker compose file:

```
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

#### source tag

Selecting the containers measurements can be tricky if you have many containers with the same name.
To alleviate this issue you can set the below value to `true`

```toml
source_tag = true
```

This will cause all measurements to have the `source` tag be set to the first 12 characters of the container id. The first 12 characters is the common hostname for containers that have no explicit hostname set, as defined by docker.

#### Kubernetes Labels

Kubernetes may add many labels to your containers, if they are not needed you
may prefer to exclude them:
```
  docker_label_exclude = ["annotation.kubernetes*"]
```

### Metrics:

- docker
  - tags:
    - unit
    - engine_host
    - server_version
  + fields:
    - n_used_file_descriptors
    - n_cpus
    - n_containers
    - n_containers_running
    - n_containers_stopped
    - n_containers_paused
    - n_images
    - n_goroutines
    - n_listener_events
    - memory_total
    - pool_blocksize (requires devicemapper storage driver) (deprecated see: `docker_devicemapper`)

The `docker_data` and `docker_metadata` measurements are available only for
some storage drivers such as devicemapper.

+ docker_data (deprecated see: `docker_devicemapper`)
  - tags:
    - unit
    - engine_host
    - server_version
  + fields:
    - available
    - total
    - used

- docker_metadata (deprecated see: `docker_devicemapper`)
  - tags:
    - unit
    - engine_host
    - server_version
  + fields:
    - available
    - total
    - used

The above measurements for the devicemapper storage driver can now be found in the new `docker_devicemapper` measurement

- docker_devicemapper
  - tags:
    - engine_host
    - server_version
    - pool_name
  + fields:
    - pool_blocksize_bytes
    - data_space_used_bytes
    - data_space_total_bytes
    - data_space_available_bytes
    - metadata_space_used_bytes
    - metadata_space_total_bytes
    - metadata_space_available_bytes
    - thin_pool_minimum_free_space_bytes

+ docker_container_mem
  - tags:
    - engine_host
    - server_version
    - container_image
    - container_name
    - container_status
    - container_version
  + fields:
    - total_pgmajfault
    - cache
    - mapped_file
    - total_inactive_file
    - pgpgout
    - rss
    - total_mapped_file
    - writeback
    - unevictable
    - pgpgin
    - total_unevictable
    - pgmajfault
    - total_rss
    - total_rss_huge
    - total_writeback
    - total_inactive_anon
    - rss_huge
    - hierarchical_memory_limit
    - total_pgfault
    - total_active_file
    - active_anon
    - total_active_anon
    - total_pgpgout
    - total_cache
    - inactive_anon
    - active_file
    - pgfault
    - inactive_file
    - total_pgpgin
    - max_usage
    - usage
    - failcnt
    - limit
    - container_id

- docker_container_cpu
  - tags:
    - engine_host
    - server_version
    - container_image
    - container_name
    - container_status
    - container_version
    - cpu
  + fields:
    - throttling_periods
    - throttling_throttled_periods
    - throttling_throttled_time
    - usage_in_kernelmode
    - usage_in_usermode
    - usage_system
    - usage_total
    - usage_percent
    - container_id

+ docker_container_net
  - tags:
    - engine_host
    - server_version
    - container_image
    - container_name
    - container_status
    - container_version
    - network
  + fields:
    - rx_dropped
    - rx_bytes
    - rx_errors
    - tx_packets
    - tx_dropped
    - rx_packets
    - tx_errors
    - tx_bytes
    - container_id

- docker_container_blkio
  - tags:
    - engine_host
    - server_version
    - container_image
    - container_name
    - container_status
    - container_version
    - device
  - fields:
    - io_service_bytes_recursive_async
    - io_service_bytes_recursive_read
    - io_service_bytes_recursive_sync
    - io_service_bytes_recursive_total
    - io_service_bytes_recursive_write
    - io_serviced_recursive_async
    - io_serviced_recursive_read
    - io_serviced_recursive_sync
    - io_serviced_recursive_total
    - io_serviced_recursive_write
    - container_id

The `docker_container_health` measurements report on a containers
[HEALTHCHECK](https://docs.docker.com/engine/reference/builder/#healthcheck)
status if configured.

- docker_container_health (container must use the HEALTHCHECK)
  - tags:
    - engine_host
    - server_version
    - container_image
    - container_name
    - container_status
    - container_version
  - fields:
  	- health_status (string)
  	- failing_streak (integer)

- docker_container_status
  - tags:
    - engine_host
    - server_version
    - container_image
    - container_name
    - container_status
    - container_version
  - fields:
    - container_id
    - oomkilled (boolean)
    - pid (integer)
    - exitcode (integer)
    - started_at (integer)
    - finished_at (integer)
    - uptime_ns (integer)

- docker_swarm
  - tags:
    - service_id
    - service_name
    - service_mode
  - fields:
    - tasks_desired
    - tasks_running

### Example Output:

```
docker,engine_host=debian-stretch-docker,server_version=17.09.0-ce n_containers=6i,n_containers_paused=0i,n_containers_running=1i,n_containers_stopped=5i,n_cpus=2i,n_goroutines=41i,n_images=2i,n_listener_events=0i,n_used_file_descriptors=27i 1524002041000000000
docker,engine_host=debian-stretch-docker,server_version=17.09.0-ce,unit=bytes memory_total=2101661696i 1524002041000000000
docker_container_mem,container_image=telegraf,container_name=zen_ritchie,container_status=running,container_version=unknown,engine_host=debian-stretch-docker,server_version=17.09.0-ce active_anon=8327168i,active_file=2314240i,cache=27402240i,container_id="adc4ba9593871bf2ab95f3ffde70d1b638b897bb225d21c2c9c84226a10a8cf4",hierarchical_memory_limit=9223372036854771712i,inactive_anon=0i,inactive_file=25088000i,limit=2101661696i,mapped_file=20582400i,max_usage=36646912i,pgfault=4193i,pgmajfault=214i,pgpgin=9243i,pgpgout=520i,rss=8327168i,rss_huge=0i,total_active_anon=8327168i,total_active_file=2314240i,total_cache=27402240i,total_inactive_anon=0i,total_inactive_file=25088000i,total_mapped_file=20582400i,total_pgfault=4193i,total_pgmajfault=214i,total_pgpgin=9243i,total_pgpgout=520i,total_rss=8327168i,total_rss_huge=0i,total_unevictable=0i,total_writeback=0i,unevictable=0i,usage=36528128i,usage_percent=0.4342225020025297,writeback=0i 1524002042000000000
docker_container_cpu,container_image=telegraf,container_name=zen_ritchie,container_status=running,container_version=unknown,cpu=cpu-total,engine_host=debian-stretch-docker,server_version=17.09.0-ce container_id="adc4ba9593871bf2ab95f3ffde70d1b638b897bb225d21c2c9c84226a10a8cf4",throttling_periods=0i,throttling_throttled_periods=0i,throttling_throttled_time=0i,usage_in_kernelmode=40000000i,usage_in_usermode=100000000i,usage_percent=0,usage_system=6394210000000i,usage_total=117319068i 1524002042000000000
docker_container_cpu,container_image=telegraf,container_name=zen_ritchie,container_status=running,container_version=unknown,cpu=cpu0,engine_host=debian-stretch-docker,server_version=17.09.0-ce container_id="adc4ba9593871bf2ab95f3ffde70d1b638b897bb225d21c2c9c84226a10a8cf4",usage_total=20825265i 1524002042000000000
docker_container_cpu,container_image=telegraf,container_name=zen_ritchie,container_status=running,container_version=unknown,cpu=cpu1,engine_host=debian-stretch-docker,server_version=17.09.0-ce container_id="adc4ba9593871bf2ab95f3ffde70d1b638b897bb225d21c2c9c84226a10a8cf4",usage_total=96493803i 1524002042000000000
docker_container_net,container_image=telegraf,container_name=zen_ritchie,container_status=running,container_version=unknown,engine_host=debian-stretch-docker,network=eth0,server_version=17.09.0-ce container_id="adc4ba9593871bf2ab95f3ffde70d1b638b897bb225d21c2c9c84226a10a8cf4",rx_bytes=1576i,rx_dropped=0i,rx_errors=0i,rx_packets=20i,tx_bytes=0i,tx_dropped=0i,tx_errors=0i,tx_packets=0i 1524002042000000000
docker_container_blkio,container_image=telegraf,container_name=zen_ritchie,container_status=running,container_version=unknown,device=254:0,engine_host=debian-stretch-docker,server_version=17.09.0-ce container_id="adc4ba9593871bf2ab95f3ffde70d1b638b897bb225d21c2c9c84226a10a8cf4",io_service_bytes_recursive_async=27398144i,io_service_bytes_recursive_read=27398144i,io_service_bytes_recursive_sync=0i,io_service_bytes_recursive_total=27398144i,io_service_bytes_recursive_write=0i,io_serviced_recursive_async=529i,io_serviced_recursive_read=529i,io_serviced_recursive_sync=0i,io_serviced_recursive_total=529i,io_serviced_recursive_write=0i 1524002042000000000
docker_container_health,container_image=telegraf,container_name=zen_ritchie,container_status=running,container_version=unknown,engine_host=debian-stretch-docker,server_version=17.09.0-ce failing_streak=0i,health_status="healthy" 1524007529000000000
docker_swarm,service_id=xaup2o9krw36j2dy1mjx1arjw,service_mode=replicated,service_name=test tasks_desired=3,tasks_running=3 1508968160000000000
```
