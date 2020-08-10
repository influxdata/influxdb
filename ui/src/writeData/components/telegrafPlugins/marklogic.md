# MarkLogic Plugin

The MarkLogic Telegraf plugin gathers health status metrics from one or more host.

### Configuration:

```toml
[[inputs.marklogic]]
  ## Base URL of the MarkLogic HTTP Server.
  url = "http://localhost:8002"

  ## List of specific hostnames to retrieve information. At least (1) required.
  # hosts = ["hostname1", "hostname2"]

  ## Using HTTP Basic Authentication. Management API requires 'manage-user' role privileges
  # username = "myuser"
  # password = "mypassword"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

### Metrics

- marklogic
  - tags:
    - source (the hostname of the server address, ex. `ml1.local`)
    - id (the host node unique id ex. `2592913110757471141`)
  - fields:
    - online
    - total_load
    - total_rate
    - ncpus
    - ncores
    - total_cpu_stat_user
    - total_cpu_stat_system
    - total_cpu_stat_idle
    - total_cpu_stat_iowait
    - memory_process_size
    - memory_process_rss
    - memory_system_total
    - memory_system_free
    - memory_process_swap_size
    - memory_size
    - host_size
    - log_device_space
    - data_dir_space
    - query_read_bytes
    - query_read_load
    - merge_read_bytes
    - merge_write_load
    - http_server_receive_bytes
    - http_server_send_bytes

### Example Output:

```
$> marklogic,host=localhost,id=2592913110757471141,source=ml1.local total_cpu_stat_iowait=0.0125649003311992,memory_process_swap_size=0i,host_size=380i,data_dir_space=28216i,query_read_load=0i,ncpus=1i,log_device_space=28216i,query_read_bytes=13947332i,merge_write_load=0i,http_server_receive_bytes=225893i,online=true,ncores=4i,total_cpu_stat_user=0.150778993964195,total_cpu_stat_system=0.598927974700928,total_cpu_stat_idle=99.2210006713867,memory_system_total=3947i,memory_system_free=2669i,memory_size=4096i,total_rate=14.7697010040283,http_server_send_bytes=0i,memory_process_size=903i,memory_process_rss=486i,merge_read_load=0i,total_load=0.00502600101754069 1566373000000000000

```
