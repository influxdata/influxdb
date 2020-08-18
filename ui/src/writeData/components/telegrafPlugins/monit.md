# Monit Input Plugin

The `monit` plugin gathers metrics and status information about local processes,
remote hosts, file, file systems, directories and network interfaces managed
and watched over by [Monit][monit].

The use this plugin you should first enable the [HTTPD TCP port][httpd] in
Monit.

Minimum Version of Monit tested with is 5.16.

[monit]: https://mmonit.com/
[httpd]: https://mmonit.com/monit/documentation/monit.html#TCP-PORT

### Configuration

```toml
[[inputs.monit]]
  ## Monit HTTPD address
  address = "http://127.0.0.1:2812"

  ## Username and Password for Monit
  # username = ""
  # password = ""

  ## Amount of time allowed to complete the HTTP request
  # timeout = "5s"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

### Metrics

- monit_filesystem
  - tags:
    - address
    - version
    - service
    - platform_name
    - status
    - monitoring_status
    - monitoring_mode
  - fields:
    - status_code
    - monitoring_status_code
    - monitoring_mode_code
    - mode
    - block_percent
    - block_usage
    - block_total
    - inode_percent
    - inode_usage
    - inode_total

+ monit_directory
  - tags:
    - address
    - version
    - service
    - platform_name
    - status
    - monitoring_status
    - monitoring_mode
  - fields:
    - status_code
    - monitoring_status_code
    - monitoring_mode_code
    - permissions

- monit_file
  - tags:
    - address
    - version
    - service
    - platform_name
    - status
    - monitoring_status
    - monitoring_mode
  - fields:
    - status_code
    - monitoring_status_code
    - monitoring_mode_code
    - size
    - permissions

+ monit_process
  - tags:
    - address
    - version
    - service
    - platform_name
    - status
    - monitoring_status
    - monitoring_mode
  - fields:
    - status_code
    - monitoring_status_code
    - monitoring_mode_code
    - cpu_percent
    - cpu_percent_total
    - mem_kb
    - mem_kb_total
    - mem_percent
    - mem_percent_total
    - pid
    - parent_pid
    - threads
    - children

- monit_remote_host
  - tags:
    - address
    - version
    - service
    - platform_name
    - status
    - monitoring_status
    - monitoring_mode
  - fields:
    - status_code
    - monitoring_status_code
    - monitoring_mode_code
    - hostname
    - port_number
    - request
    - protocol
    - type

+ monit_system
  - tags:
    - address
    - version
    - service
    - platform_name
    - status
    - monitoring_status
    - monitoring_mode
  - fields:
    - status_code
    - monitoring_status_code
    - monitoring_mode_code
    - cpu_system
    - cpu_user
    - cpu_wait
    - cpu_load_avg_1m
    - cpu_load_avg_5m
    - cpu_load_avg_15m
    - mem_kb
    - mem_percent
    - swap_kb
    - swap_percent

- monit_fifo
  - tags:
    - address
    - version
    - service
    - platform_name
    - status
    - monitoring_status
    - monitoring_mode
  - fields:
    - status_code
    - monitoring_status_code
    - monitoring_mode_code
	- permissions

+ monit_program
  - tags:
    - address
    - version
    - service
    - platform_name
    - status
    - monitoring_status
    - monitoring_mode
  - fields:
    - status_code
    - monitoring_status_code
    - monitoring_mode_code

- monit_network
  - tags:
    - address
    - version
    - service
    - platform_name
    - status
    - monitoring_status
    - monitoring_mode
  - fields:
    - status_code
    - monitoring_status_code
    - monitoring_mode_code

+ monit_program
  - tags:
    - address
    - version
    - service
    - platform_name
    - status
    - monitoring_status
    - monitoring_mode
  - fields:
    - status_code
    - monitoring_status_code
    - monitoring_mode_code

- monit_network
  - tags:
    - address
    - version
    - service
    - platform_name
    - status
    - monitoring_status
    - monitoring_mode
  - fields:
    - status_code
    - monitoring_status_code
    - monitoring_mode_code

### Example Output
```
monit_file,monitoring_mode=active,monitoring_status=monitored,pending_action=none,platform_name=Linux,service=rsyslog_pid,source=xyzzy.local,status=running,version=5.20.0 mode=644i,monitoring_mode_code=0i,monitoring_status_code=1i,pending_action_code=0i,size=3i,status_code=0i 1579735047000000000
monit_process,monitoring_mode=active,monitoring_status=monitored,pending_action=none,platform_name=Linux,service=rsyslog,source=xyzzy.local,status=running,version=5.20.0 children=0i,cpu_percent=0,cpu_percent_total=0,mem_kb=3148i,mem_kb_total=3148i,mem_percent=0.2,mem_percent_total=0.2,monitoring_mode_code=0i,monitoring_status_code=1i,parent_pid=1i,pending_action_code=0i,pid=318i,status_code=0i,threads=4i 1579735047000000000
monit_program,monitoring_mode=active,monitoring_status=initializing,pending_action=none,platform_name=Linux,service=echo,source=xyzzy.local,status=running,version=5.20.0 monitoring_mode_code=0i,monitoring_status_code=2i,pending_action_code=0i,program_started=0i,program_status=0i,status_code=0i 1579735047000000000
monit_system,monitoring_mode=active,monitoring_status=monitored,pending_action=none,platform_name=Linux,service=debian-stretch-monit.virt,source=xyzzy.local,status=running,version=5.20.0 cpu_load_avg_15m=0,cpu_load_avg_1m=0,cpu_load_avg_5m=0,cpu_system=0,cpu_user=0,cpu_wait=0,mem_kb=42852i,mem_percent=2.1,monitoring_mode_code=0i,monitoring_status_code=1i,pending_action_code=0i,status_code=0i,swap_kb=0,swap_percent=0 1579735047000000000
```
