# PHP-FPM Input Plugin

Get phpfpm stats using either HTTP status page or fpm socket.

### Configuration:

```toml
# Read metrics of phpfpm, via HTTP status page or socket
[[inputs.phpfpm]]
  ## An array of addresses to gather stats about. Specify an ip or hostname
  ## with optional port and path
  ##
  ## Plugin can be configured in three modes (either can be used):
  ##   - http: the URL must start with http:// or https://, ie:
  ##       "http://localhost/status"
  ##       "http://192.168.130.1/status?full"
  ##
  ##   - unixsocket: path to fpm socket, ie:
  ##       "/var/run/php5-fpm.sock"
  ##      or using a custom fpm status path:
  ##       "/var/run/php5-fpm.sock:fpm-custom-status-path"
  ##      glob patterns are also supported:
  ##       "/var/run/php*.sock"
  ##
  ##   - fcgi: the URL must start with fcgi:// or cgi://, and port must be present, ie:
  ##       "fcgi://10.0.0.12:9000/status"
  ##       "cgi://10.0.10.12:9001/status"
  ##
  ## Example of multiple gathering from local socket and remote host
  ## urls = ["http://192.168.1.20/status", "/tmp/fpm.sock"]
  urls = ["http://localhost/status"]

  ## Duration allowed to complete HTTP requests.
  # timeout = "5s"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

When using `unixsocket`, you have to ensure that telegraf runs on same
host, and socket path is accessible to telegraf user.

### Metrics:

- phpfpm
  - tags:
    - pool
    - url
  - fields:
    - accepted_conn
    - listen_queue
    - max_listen_queue
    - listen_queue_len
    - idle_processes
    - active_processes
    - total_processes
    - max_active_processes
    - max_children_reached
    - slow_requests

# Example Output

```
phpfpm,pool=www accepted_conn=13i,active_processes=2i,idle_processes=1i,listen_queue=0i,listen_queue_len=0i,max_active_processes=2i,max_children_reached=0i,max_listen_queue=0i,slow_requests=0i,total_processes=3i 1453011293083331187
phpfpm,pool=www2 accepted_conn=12i,active_processes=1i,idle_processes=2i,listen_queue=0i,listen_queue_len=0i,max_active_processes=2i,max_children_reached=0i,max_listen_queue=0i,slow_requests=0i,total_processes=3i 1453011293083691422
phpfpm,pool=www3 accepted_conn=11i,active_processes=1i,idle_processes=2i,listen_queue=0i,listen_queue_len=0i,max_active_processes=2i,max_children_reached=0i,max_listen_queue=0i,slow_requests=0i,total_processes=3i 1453011293083691658
```
