# DC/OS Input Plugin

This input plugin gathers metrics from a DC/OS cluster's [metrics component](https://docs.mesosphere.com/1.10/metrics/).

**Series Cardinality Warning**

Depending on the work load of your DC/OS cluster, this plugin can quickly
create a high number of series which, when unchecked, can cause high load on
your database.

- Use the
  [measurement filtering](https://docs.influxdata.com/telegraf/latest/administration/configuration/#measurement-filtering)
  options to exclude unneeded tags.
- Write to a database with an appropriate
  [retention policy](https://docs.influxdata.com/influxdb/latest/guides/downsampling_and_retention/).
- Limit series cardinality in your database using the
  [`max-series-per-database`](https://docs.influxdata.com/influxdb/latest/administration/config/#max-series-per-database-1000000) and
  [`max-values-per-tag`](https://docs.influxdata.com/influxdb/latest/administration/config/#max-values-per-tag-100000) settings.
- Consider using the
  [Time Series Index](https://docs.influxdata.com/influxdb/latest/concepts/time-series-index/).
- Monitor your databases
  [series cardinality](https://docs.influxdata.com/influxdb/latest/query_language/spec/#show-cardinality).

### Configuration:
```toml
[[inputs.dcos]]
  ## The DC/OS cluster URL.
  cluster_url = "https://dcos-master-1"

  ## The ID of the service account.
  service_account_id = "telegraf"
  ## The private key file for the service account.
  service_account_private_key = "/etc/telegraf/telegraf-sa-key.pem"

  ## Path containing login token.  If set, will read on every gather.
  # token_file = "/home/dcos/.dcos/token"

  ## In all filter options if both include and exclude are empty all items
  ## will be collected.  Arrays may contain glob patterns.
  ##
  ## Node IDs to collect metrics from.  If a node is excluded, no metrics will
  ## be collected for its containers or apps.
  # node_include = []
  # node_exclude = []
  ## Container IDs to collect container metrics from.
  # container_include = []
  # container_exclude = []
  ## Container IDs to collect app metrics from.
  # app_include = []
  # app_exclude = []

  ## Maximum concurrent connections to the cluster.
  # max_connections = 10
  ## Maximum time to receive a response from cluster.
  # response_timeout = "20s"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## If false, skip chain & host verification
  # insecure_skip_verify = true

  ## Recommended filtering to reduce series cardinality.
  # [inputs.dcos.tagdrop]
  #   path = ["/var/lib/mesos/slave/slaves/*"]
```

#### Enterprise Authentication

When using Enterprise DC/OS, it is recommended to use a service account to
authenticate with the cluster.

The plugin requires the following permissions:
```
dcos:adminrouter:ops:system-metrics full
dcos:adminrouter:ops:mesos full
```

Follow the directions to [create a service account and assign permissions](https://docs.mesosphere.com/1.10/security/service-auth/custom-service-auth/).

Quick configuration using the Enterprise CLI:
```
dcos security org service-accounts keypair telegraf-sa-key.pem telegraf-sa-cert.pem
dcos security org service-accounts create -p telegraf-sa-cert.pem -d "Telegraf DC/OS input plugin" telegraf
dcos security org users grant telegraf dcos:adminrouter:ops:system-metrics full
dcos security org users grant telegraf dcos:adminrouter:ops:mesos full
```

#### Open Source Authentication

The Open Source DC/OS does not provide service accounts.  Instead you can use
of the following options:

1. [Disable authentication](https://dcos.io/docs/1.10/security/managing-authentication/#authentication-opt-out)
2. Use the `token_file` parameter to read a authentication token from a file.

Then `token_file` can be set by using the [dcos cli] to login periodically.
The cli can login for at most XXX days, you will need to ensure the cli
performs a new login before this time expires.
```
dcos auth login --username foo --password bar
dcos config show core.dcos_acs_token > ~/.dcos/token
```

Another option to create a `token_file` is to generate a token using the
cluster secret.  This will allow you to set the expiration date manually or
even create a never expiring token.  However, if the cluster secret or the
token is compromised it cannot be revoked and may require a full reinstall of
the cluster.  For more information on this technique reference
[this blog post](https://medium.com/@richardgirges/authenticating-open-source-dc-os-with-third-party-services-125fa33a5add).

### Metrics:

Please consult the [Metrics Reference](https://docs.mesosphere.com/1.10/metrics/reference/)
for details about field interpretation.

- dcos_node
  - tags:
    - cluster
    - hostname
    - path (filesystem fields only)
    - interface (network fields only)
  - fields:
    - system_uptime (float)
    - cpu_cores (float)
    - cpu_total (float)
    - cpu_user (float)
    - cpu_system (float)
    - cpu_idle (float)
    - cpu_wait (float)
    - load_1min (float)
    - load_5min (float)
    - load_15min (float)
    - filesystem_capacity_total_bytes (int)
    - filesystem_capacity_used_bytes (int)
    - filesystem_capacity_free_bytes (int)
    - filesystem_inode_total (float)
    - filesystem_inode_used (float)
    - filesystem_inode_free (float)
    - memory_total_bytes (int)
    - memory_free_bytes (int)
    - memory_buffers_bytes (int)
    - memory_cached_bytes (int)
    - swap_total_bytes (int)
    - swap_free_bytes (int)
    - swap_used_bytes (int)
    - network_in_bytes (int)
    - network_out_bytes (int)
    - network_in_packets (float)
    - network_out_packets (float)
    - network_in_dropped (float)
    - network_out_dropped (float)
    - network_in_errors (float)
    - network_out_errors (float)
    - process_count (float)

- dcos_container
  - tags:
    - cluster
    - hostname
    - container_id
    - task_name
  - fields:
    - cpus_limit (float)
    - cpus_system_time (float)
    - cpus_throttled_time (float)
    - cpus_user_time (float)
    - disk_limit_bytes (int)
    - disk_used_bytes (int)
    - mem_limit_bytes (int)
    - mem_total_bytes (int)
    - net_rx_bytes (int)
    - net_rx_dropped (float)
    - net_rx_errors (float)
    - net_rx_packets (float)
    - net_tx_bytes (int)
    - net_tx_dropped (float)
    - net_tx_errors (float)
    - net_tx_packets (float)

- dcos_app
  - tags:
    - cluster
    - hostname
    - container_id
    - task_name
  - fields:
    - fields are application specific

### Example Output:

```
dcos_node,cluster=enterprise,hostname=192.168.122.18,path=/boot filesystem_capacity_free_bytes=918188032i,filesystem_capacity_total_bytes=1063256064i,filesystem_capacity_used_bytes=145068032i,filesystem_inode_free=523958,filesystem_inode_total=524288,filesystem_inode_used=330 1511859222000000000
dcos_node,cluster=enterprise,hostname=192.168.122.18,interface=dummy0 network_in_bytes=0i,network_in_dropped=0,network_in_errors=0,network_in_packets=0,network_out_bytes=0i,network_out_dropped=0,network_out_errors=0,network_out_packets=0 1511859222000000000
dcos_node,cluster=enterprise,hostname=192.168.122.18,interface=docker0 network_in_bytes=0i,network_in_dropped=0,network_in_errors=0,network_in_packets=0,network_out_bytes=0i,network_out_dropped=0,network_out_errors=0,network_out_packets=0 1511859222000000000
dcos_node,cluster=enterprise,hostname=192.168.122.18 cpu_cores=2,cpu_idle=81.62,cpu_system=4.19,cpu_total=13.670000000000002,cpu_user=9.48,cpu_wait=0,load_15min=0.7,load_1min=0.22,load_5min=0.6,memory_buffers_bytes=970752i,memory_cached_bytes=1830473728i,memory_free_bytes=1178636288i,memory_total_bytes=3975073792i,process_count=198,swap_free_bytes=859828224i,swap_total_bytes=859828224i,swap_used_bytes=0i,system_uptime=18874 1511859222000000000
dcos_node,cluster=enterprise,hostname=192.168.122.18,interface=lo network_in_bytes=1090992450i,network_in_dropped=0,network_in_errors=0,network_in_packets=1546938,network_out_bytes=1090992450i,network_out_dropped=0,network_out_errors=0,network_out_packets=1546938 1511859222000000000
dcos_node,cluster=enterprise,hostname=192.168.122.18,path=/ filesystem_capacity_free_bytes=1668378624i,filesystem_capacity_total_bytes=6641680384i,filesystem_capacity_used_bytes=4973301760i,filesystem_inode_free=3107856,filesystem_inode_total=3248128,filesystem_inode_used=140272 1511859222000000000
dcos_node,cluster=enterprise,hostname=192.168.122.18,interface=minuteman network_in_bytes=0i,network_in_dropped=0,network_in_errors=0,network_in_packets=0,network_out_bytes=210i,network_out_dropped=0,network_out_errors=0,network_out_packets=3 1511859222000000000
dcos_node,cluster=enterprise,hostname=192.168.122.18,interface=eth0 network_in_bytes=539886216i,network_in_dropped=1,network_in_errors=0,network_in_packets=979808,network_out_bytes=112395836i,network_out_dropped=0,network_out_errors=0,network_out_packets=891239 1511859222000000000
dcos_node,cluster=enterprise,hostname=192.168.122.18,interface=spartan network_in_bytes=0i,network_in_dropped=0,network_in_errors=0,network_in_packets=0,network_out_bytes=210i,network_out_dropped=0,network_out_errors=0,network_out_packets=3 1511859222000000000
dcos_node,cluster=enterprise,hostname=192.168.122.18,path=/var/lib/docker/overlay filesystem_capacity_free_bytes=1668378624i,filesystem_capacity_total_bytes=6641680384i,filesystem_capacity_used_bytes=4973301760i,filesystem_inode_free=3107856,filesystem_inode_total=3248128,filesystem_inode_used=140272 1511859222000000000
dcos_node,cluster=enterprise,hostname=192.168.122.18,interface=vtep1024 network_in_bytes=0i,network_in_dropped=0,network_in_errors=0,network_in_packets=0,network_out_bytes=0i,network_out_dropped=0,network_out_errors=0,network_out_packets=0 1511859222000000000
dcos_node,cluster=enterprise,hostname=192.168.122.18,path=/var/lib/docker/plugins filesystem_capacity_free_bytes=1668378624i,filesystem_capacity_total_bytes=6641680384i,filesystem_capacity_used_bytes=4973301760i,filesystem_inode_free=3107856,filesystem_inode_total=3248128,filesystem_inode_used=140272 1511859222000000000
dcos_node,cluster=enterprise,hostname=192.168.122.18,interface=d-dcos network_in_bytes=0i,network_in_dropped=0,network_in_errors=0,network_in_packets=0,network_out_bytes=0i,network_out_dropped=0,network_out_errors=0,network_out_packets=0 1511859222000000000
dcos_app,cluster=enterprise,container_id=9a78d34a-3bbf-467e-81cf-a57737f154ee,hostname=192.168.122.18 container_received_bytes_per_sec=0,container_throttled_bytes_per_sec=0 1511859222000000000
dcos_container,cluster=enterprise,container_id=cbf19b77-3b8d-4bcf-b81f-824b67279629,hostname=192.168.122.18 cpus_limit=0.3,cpus_system_time=307.31,cpus_throttled_time=102.029930607,cpus_user_time=268.57,disk_limit_bytes=268435456i,disk_used_bytes=30953472i,mem_limit_bytes=570425344i,mem_total_bytes=13316096i,net_rx_bytes=0i,net_rx_dropped=0,net_rx_errors=0,net_rx_packets=0,net_tx_bytes=0i,net_tx_dropped=0,net_tx_errors=0,net_tx_packets=0 1511859222000000000
dcos_app,cluster=enterprise,container_id=cbf19b77-3b8d-4bcf-b81f-824b67279629,hostname=192.168.122.18 container_received_bytes_per_sec=0,container_throttled_bytes_per_sec=0 1511859222000000000
dcos_container,cluster=enterprise,container_id=5725e219-f66e-40a8-b3ab-519d85f4c4dc,hostname=192.168.122.18,task_name=hello-world cpus_limit=0.6,cpus_system_time=25.6,cpus_throttled_time=327.977109217,cpus_user_time=566.54,disk_limit_bytes=0i,disk_used_bytes=0i,mem_limit_bytes=1107296256i,mem_total_bytes=335941632i,net_rx_bytes=0i,net_rx_dropped=0,net_rx_errors=0,net_rx_packets=0,net_tx_bytes=0i,net_tx_dropped=0,net_tx_errors=0,net_tx_packets=0 1511859222000000000
dcos_app,cluster=enterprise,container_id=5725e219-f66e-40a8-b3ab-519d85f4c4dc,hostname=192.168.122.18 container_received_bytes_per_sec=0,container_throttled_bytes_per_sec=0 1511859222000000000
dcos_app,cluster=enterprise,container_id=c76e1488-4fb7-4010-a4cf-25725f8173f9,hostname=192.168.122.18 container_received_bytes_per_sec=0,container_throttled_bytes_per_sec=0 1511859222000000000
dcos_container,cluster=enterprise,container_id=cbe0b2f9-061f-44ac-8f15-4844229e8231,hostname=192.168.122.18,task_name=telegraf cpus_limit=0.2,cpus_system_time=8.109999999,cpus_throttled_time=93.183916045,cpus_user_time=17.97,disk_limit_bytes=0i,disk_used_bytes=0i,mem_limit_bytes=167772160i,mem_total_bytes=0i,net_rx_bytes=0i,net_rx_dropped=0,net_rx_errors=0,net_rx_packets=0,net_tx_bytes=0i,net_tx_dropped=0,net_tx_errors=0,net_tx_packets=0 1511859222000000000
dcos_container,cluster=enterprise,container_id=b64115de-3d2a-431d-a805-76e7c46453f1,hostname=192.168.122.18 cpus_limit=0.2,cpus_system_time=2.69,cpus_throttled_time=20.064861214,cpus_user_time=6.56,disk_limit_bytes=268435456i,disk_used_bytes=29360128i,mem_limit_bytes=297795584i,mem_total_bytes=13733888i,net_rx_bytes=0i,net_rx_dropped=0,net_rx_errors=0,net_rx_packets=0,net_tx_bytes=0i,net_tx_dropped=0,net_tx_errors=0,net_tx_packets=0 1511859222000000000
dcos_app,cluster=enterprise,container_id=b64115de-3d2a-431d-a805-76e7c46453f1,hostname=192.168.122.18 container_received_bytes_per_sec=0,container_throttled_bytes_per_sec=0 1511859222000000000
```
