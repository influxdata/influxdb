# GrayLog plugin

The Graylog plugin can collect data from remote Graylog service URLs.

Plugin currently support two type of end points:-

- multiple  (Ex http://[graylog-server-ip]:12900/system/metrics/multiple)
- namespace (Ex http://[graylog-server-ip]:12900/system/metrics/namespace/{namespace})

End Point can be a mix of one  multiple end point  and several namespaces end points


Note: if namespace end point specified metrics array will be ignored for that call.

### Configuration:

```toml
# Read flattened metrics from one or more GrayLog HTTP endpoints
[[inputs.graylog]]
  ## API endpoint, currently supported API:
  ##
  ##   - multiple  (Ex http://<host>:12900/system/metrics/multiple)
  ##   - namespace (Ex http://<host>:12900/system/metrics/namespace/{namespace})
  ##
  ## For namespace endpoint, the metrics array will be ignored for that call.
  ## Endpoint can contain namespace and multiple type calls.
  ##
  ## Please check http://[graylog-server-ip]:12900/api-browser for full list
  ## of endpoints
  servers = [
    "http://[graylog-server-ip]:12900/system/metrics/multiple",
  ]

  ## Metrics list
  ## List of metrics can be found on Graylog webservice documentation.
  ## Or by hitting the web service api at:
  ##   http://[graylog-host]:12900/system/metrics
  metrics = [
    "jvm.cl.loaded",
    "jvm.memory.pools.Metaspace.committed"
  ]

  ## Username and password
  username = ""
  password = ""

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

Please refer to GrayLog metrics api browser for full metric end points http://host:12900/api-browser
