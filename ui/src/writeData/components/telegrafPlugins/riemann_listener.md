# Riemann Listener Input Plugin

The Riemann Listener is a simple input plugin that listens for messages from
client that use riemann clients using riemann-protobuff format.


### Configuration:

This is a sample configuration for the plugin.

```toml
[[inputs.rimann_listener]]
  ## URL to listen on
  ## Default is "tcp://:5555"
  #  service_address = "tcp://:8094"
  #  service_address = "tcp://127.0.0.1:http"
  #  service_address = "tcp4://:8094"
  #  service_address = "tcp6://:8094"
  #  service_address = "tcp6://[2001:db8::1]:8094"

  ## Maximum number of concurrent connections.
  ## 0 (default) is unlimited.
  #  max_connections = 1024
  ## Read timeout.
  ## 0 (default) is unlimited.
  #  read_timeout = "30s"
  ## Optional TLS configuration.
  #  tls_cert = "/etc/telegraf/cert.pem"
  #  tls_key  = "/etc/telegraf/key.pem"
  ## Enables client authentication if set.
  #  tls_allowed_cacerts = ["/etc/telegraf/clientca.pem"]
  ## Maximum socket buffer size (in bytes when no unit specified).
  #  read_buffer_size = "64KiB"
  ## Period between keep alive probes.
  ## 0 disables keep alive probes.
  ## Defaults to the OS configuration.
  #  keep_alive_period = "5m"
```
Just like Riemann the default port is 5555. This can be configured, refer configuration above.

Riemann `Service` is mapped as `measurement`. `metric` and `TTL` are converted into field values.
As Riemann tags as simply an array, they are converted into the `influx_line` format key-value, where both key and value are the tags.
