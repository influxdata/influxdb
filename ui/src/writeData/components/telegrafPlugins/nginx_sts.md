# Nginx Stream STS Input Plugin

This plugin gathers Nginx status using external virtual host traffic status
module -  https://github.com/vozlt/nginx-module-sts. This is an Nginx module
that provides access to stream host status information. It contains the current
status such as servers, upstreams, caches. This is similar to the live activity
monitoring of Nginx plus.  For module configuration details please see its
[documentation](https://github.com/vozlt/nginx-module-sts#synopsis).

Telegraf minimum version: Telegraf 1.15.0

### Configuration

```toml
[[inputs.nginx_sts]]
  ## An array of ngx_http_status_module or status URI to gather stats.
  urls = ["http://localhost/status"]

  ## HTTP response timeout (default: 5s)
  response_timeout = "5s"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

### Metrics

- nginx_sts_connections
  - tags:
    - source
    - port
  - fields:
    - active
    - reading
    - writing
    - waiting
    - accepted
    - handled
    - requests

+ nginx_sts_server
  - tags:
    - source
    - port
    - zone
  - fields:
    - connects
    - in_bytes
    - out_bytes
    - response_1xx_count
    - response_2xx_count
    - response_3xx_count
    - response_4xx_count
    - response_5xx_count
    - session_msec_counter
    - session_msec

- nginx_sts_filter
  - tags:
    - source
    - port
    - filter_name
    - filter_key
  - fields:
    - connects
    - in_bytes
    - out_bytes
    - response_1xx_count
    - response_2xx_count
    - response_3xx_count
    - response_4xx_count
    - response_5xx_count
    - session_msec_counter
    - session_msec

+ nginx_sts_upstream
  - tags:
    - source
    - port
    - upstream
    - upstream_address
  - fields:
    - connects
    - in_bytes
    - out_bytes
    - response_1xx_count
    - response_2xx_count
    - response_3xx_count
    - response_4xx_count
    - response_5xx_count
    - session_msec_counter
    - session_msec
    - upstream_session_msec_counter
    - upstream_session_msec
    - upstream_connect_msec_counter
    - upstream_connect_msec
    - upstream_firstbyte_msec_counter
    - upstream_firstbyte_msec
    - weight
    - max_fails
    - fail_timeout
    - backup
    - down

### Example Output:

```
nginx_sts_upstream,host=localhost,port=80,source=127.0.0.1,upstream=backend_cluster,upstream_address=1.2.3.4:8080 upstream_connect_msec_counter=0i,out_bytes=0i,down=false,connects=0i,session_msec=0i,upstream_session_msec=0i,upstream_session_msec_counter=0i,upstream_connect_msec=0i,upstream_firstbyte_msec_counter=0i,response_3xx_count=0i,session_msec_counter=0i,weight=1i,max_fails=1i,backup=false,upstream_firstbyte_msec=0i,in_bytes=0i,response_1xx_count=0i,response_2xx_count=0i,response_4xx_count=0i,response_5xx_count=0i,fail_timeout=10i 1584699180000000000
nginx_sts_upstream,host=localhost,port=80,source=127.0.0.1,upstream=backend_cluster,upstream_address=9.8.7.6:8080 upstream_firstbyte_msec_counter=0i,response_2xx_count=0i,down=false,upstream_session_msec_counter=0i,out_bytes=0i,response_5xx_count=0i,weight=1i,max_fails=1i,fail_timeout=10i,connects=0i,session_msec_counter=0i,upstream_session_msec=0i,in_bytes=0i,response_1xx_count=0i,response_3xx_count=0i,response_4xx_count=0i,session_msec=0i,upstream_connect_msec=0i,upstream_connect_msec_counter=0i,upstream_firstbyte_msec=0i,backup=false 1584699180000000000
nginx_sts_server,host=localhost,port=80,source=127.0.0.1,zone=* response_2xx_count=0i,response_4xx_count=0i,response_5xx_count=0i,session_msec_counter=0i,in_bytes=0i,out_bytes=0i,session_msec=0i,response_1xx_count=0i,response_3xx_count=0i,connects=0i 1584699180000000000
nginx_sts_connections,host=localhost,port=80,source=127.0.0.1 waiting=1i,accepted=146i,handled=146i,requests=13421i,active=3i,reading=0i,writing=2i 1584699180000000000
```
