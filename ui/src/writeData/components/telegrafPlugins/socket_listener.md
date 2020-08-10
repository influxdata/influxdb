# socket listener service input plugin

The Socket Listener is a service input plugin that listens for messages from
streaming (tcp, unix) or datagram (udp, unixgram) protocols.

The plugin expects messages in the
[Telegraf Input Data Formats](https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md).

### Configuration:

This is a sample configuration for the plugin.

```toml
# Generic socket listener capable of handling multiple socket types.
[[inputs.socket_listener]]
  ## URL to listen on
  # service_address = "tcp://:8094"
  # service_address = "tcp://127.0.0.1:http"
  # service_address = "tcp4://:8094"
  # service_address = "tcp6://:8094"
  # service_address = "tcp6://[2001:db8::1]:8094"
  # service_address = "udp://:8094"
  # service_address = "udp4://:8094"
  # service_address = "udp6://:8094"
  # service_address = "unix:///tmp/telegraf.sock"
  # service_address = "unixgram:///tmp/telegraf.sock"

  ## Change the file mode bits on unix sockets.  These permissions may not be
  ## respected by some platforms, to safely restrict write permissions it is best
  ## to place the socket into a directory that has previously been created
  ## with the desired permissions.
  ##   ex: socket_mode = "777"
  # socket_mode = ""

  ## Maximum number of concurrent connections.
  ## Only applies to stream sockets (e.g. TCP).
  ## 0 (default) is unlimited.
  # max_connections = 1024

  ## Read timeout.
  ## Only applies to stream sockets (e.g. TCP).
  ## 0 (default) is unlimited.
  # read_timeout = "30s"

  ## Optional TLS configuration.
  ## Only applies to stream sockets (e.g. TCP).
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key  = "/etc/telegraf/key.pem"
  ## Enables client authentication if set.
  # tls_allowed_cacerts = ["/etc/telegraf/clientca.pem"]

  ## Maximum socket buffer size (in bytes when no unit specified).
  ## For stream sockets, once the buffer fills up, the sender will start backing up.
  ## For datagram sockets, once the buffer fills up, metrics will start dropping.
  ## Defaults to the OS default.
  # read_buffer_size = "64KiB"

  ## Period between keep alive probes.
  ## Only applies to TCP sockets.
  ## 0 disables keep alive probes.
  ## Defaults to the OS configuration.
  # keep_alive_period = "5m"

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  # data_format = "influx"

  ## Content encoding for message payloads, can be set to "gzip" to or
  ## "identity" to apply no encoding.
  # content_encoding = "identity"
```

## A Note on UDP OS Buffer Sizes

The `read_buffer_size` config option can be used to adjust the size of the socket
buffer, but this number is limited by OS settings. On Linux, `read_buffer_size`
will default to `rmem_default` and will be capped by `rmem_max`. On BSD systems,
`read_buffer_size` is capped by `maxsockbuf`, and there is no OS default
setting.

Instructions on how to adjust these OS settings are available below.

Some OSes (most notably, Linux) place very restrictive limits on the performance
of UDP protocols. It is _highly_ recommended that you increase these OS limits to
at least 8MB before trying to run large amounts of UDP traffic to your instance.
8MB is just a recommendation, and can be adjusted higher.

### Linux

Check the current UDP/IP receive buffer limit & default by typing the following
commands:

```
sysctl net.core.rmem_max
sysctl net.core.rmem_default
```

If the values are less than 8388608 bytes you should add the following lines to
the /etc/sysctl.conf file:

```
net.core.rmem_max=8388608
net.core.rmem_default=8388608
```

Changes to /etc/sysctl.conf do not take effect until reboot.
To update the values immediately, type the following commands as root:

```
sysctl -w net.core.rmem_max=8388608
sysctl -w net.core.rmem_default=8388608
```

### BSD/Darwin

On BSD/Darwin systems you need to add about a 15% padding to the kernel limit
socket buffer. Meaning if you want an 8MB buffer (8388608 bytes) you need to set
the kernel limit to `8388608*1.15 = 9646900`. This is not documented anywhere but
happens
[in the kernel here.](https://github.com/freebsd/freebsd/blob/master/sys/kern/uipc_sockbuf.c#L63-L64)

Check the current UDP/IP buffer limit by typing the following command:

```
sysctl kern.ipc.maxsockbuf
```

If the value is less than 9646900 bytes you should add the following lines
to the /etc/sysctl.conf file (create it if necessary):

```
kern.ipc.maxsockbuf=9646900
```

Changes to /etc/sysctl.conf do not take effect until reboot.
To update the values immediately, type the following command as root:

```
sysctl -w kern.ipc.maxsockbuf=9646900
```
