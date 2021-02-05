# Syslog Input Plugin

The syslog plugin listens for syslog messages transmitted over
a Unix Domain socket,
[UDP](https://tools.ietf.org/html/rfc5426),
[TCP](https://tools.ietf.org/html/rfc6587), or
[TLS](https://tools.ietf.org/html/rfc5425); with or without the octet counting framing.

Syslog messages should be formatted according to
[RFC 5424](https://tools.ietf.org/html/rfc5424).

### Configuration

```toml
[[inputs.syslog]]
  ## Protocol, address and port to host the syslog receiver.
  ## If no host is specified, then localhost is used.
  ## If no port is specified, 6514 is used (RFC5425#section-4.1).
  ##   ex: server = "tcp://localhost:6514"
  ##       server = "udp://:6514"
  ##       server = "unix:///var/run/telegraf-syslog.sock"
  server = "tcp://:6514"

  ## TLS Config
  # tls_allowed_cacerts = ["/etc/telegraf/ca.pem"]
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"

  ## Period between keep alive probes.
  ## 0 disables keep alive probes.
  ## Defaults to the OS configuration.
  ## Only applies to stream sockets (e.g. TCP).
  # keep_alive_period = "5m"

  ## Maximum number of concurrent connections (default = 0).
  ## 0 means unlimited.
  ## Only applies to stream sockets (e.g. TCP).
  # max_connections = 1024

  ## Read timeout is the maximum time allowed for reading a single message (default = 5s).
  ## 0 means unlimited.
  # read_timeout = "5s"

  ## The framing technique with which it is expected that messages are transported (default = "octet-counting").
  ## Whether the messages come using the octect-counting (RFC5425#section-4.3.1, RFC6587#section-3.4.1),
  ## or the non-transparent framing technique (RFC6587#section-3.4.2).
  ## Must be one of "octect-counting", "non-transparent".
  # framing = "octet-counting"

  ## The trailer to be expected in case of non-transparent framing (default = "LF").
  ## Must be one of "LF", or "NUL".
  # trailer = "LF"

  ## Whether to parse in best effort mode or not (default = false).
  ## By default best effort parsing is off.
  # best_effort = false

  ## Character to prepend to SD-PARAMs (default = "_").
  ## A syslog message can contain multiple parameters and multiple identifiers within structured data section.
  ## Eg., [id1 name1="val1" name2="val2"][id2 name1="val1" nameA="valA"]
  ## For each combination a field is created.
  ## Its name is created concatenating identifier, sdparam_separator, and parameter name.
  # sdparam_separator = "_"
```

#### Message transport

The `framing` option only applies to streams. It governs the way we expect to receive messages within the stream.
Namely, with the [`"octet counting"`](https://tools.ietf.org/html/rfc5425#section-4.3) technique (default) or with the [`"non-transparent"`](https://tools.ietf.org/html/rfc6587#section-3.4.2) framing.

The `trailer` option only applies when `framing` option is `"non-transparent"`. It must have one of the following values: `"LF"` (default), or `"NUL"`.

#### Best effort

The [`best_effort`](https://github.com/influxdata/go-syslog#best-effort-mode)
option instructs the parser to extract partial but valid info from syslog
messages. If unset only full messages will be collected.

#### Rsyslog Integration

Rsyslog can be configured to forward logging messages to Telegraf by configuring
[remote logging](https://www.rsyslog.com/doc/v8-stable/configuration/actions.html#remote-machine).

Most system are setup with a configuration split between `/etc/rsyslog.conf`
and the files in the `/etc/rsyslog.d/` directory, it is recommended to add the
new configuration into the config directory to simplify updates to the main
config file.

Add the following lines to `/etc/rsyslog.d/50-telegraf.conf` making
adjustments to the target address as needed:
```
$ActionQueueType LinkedList # use asynchronous processing
$ActionQueueFileName srvrfwd # set file name, also enables disk mode
$ActionResumeRetryCount -1 # infinite retries on insert failure
$ActionQueueSaveOnShutdown on # save in-memory data if rsyslog shuts down

# forward over tcp with octet framing according to RFC 5425
*.* @@(o)127.0.0.1:6514;RSYSLOG_SyslogProtocol23Format

# uncomment to use udp according to RFC 5424
#*.* @127.0.0.1:6514;RSYSLOG_SyslogProtocol23Format
```

You can alternately use `advanced` format (aka RainerScript):
```
# forward over tcp with octet framing according to RFC 5425
action(type="omfwd" Protocol="tcp" TCP_Framing="octet-counted" Target="127.0.0.1" Port="6514" Template="RSYSLOG_SyslogProtocol23Format")

# uncomment to use udp according to RFC 5424
#action(type="omfwd" Protocol="udp" Target="127.0.0.1" Port="6514" Template="RSYSLOG_SyslogProtocol23Format")
```

To complete TLS setup please refer to [rsyslog docs](https://www.rsyslog.com/doc/v8-stable/tutorials/tls.html).

### Metrics

- syslog
  - tags
    - severity (string)
    - facility (string)
    - hostname (string)
    - appname (string)
  - fields
    - version (integer)
    - severity_code (integer)
    - facility_code (integer)
    - timestamp (integer): the time recorded in the syslog message
    - procid (string)
    - msgid (string)
    - sdid (bool)
    - *Structured Data* (string)
  - timestamp: the time the messages was received

#### Structured Data

Structured data produces field keys by combining the `SD_ID` with the `PARAM_NAME` combined using the `sdparam_separator` as in the following example:
```
170 <165>1 2018-10-01:14:15.000Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] An application event log entry...
```
```
syslog,appname=evntslog,facility=local4,hostname=mymachine.example.com,severity=notice exampleSDID@32473_eventID="1011",exampleSDID@32473_eventSource="Application",exampleSDID@32473_iut="3",facility_code=20i,message="An application event log entry...",msgid="ID47",severity_code=5i,timestamp=1065910455003000000i,version=1i 1538421339749472344
```

### Troubleshooting

You can send debugging messages directly to the input plugin using netcat:

```sh
# TCP with octet framing
echo "57 <13>1 2018-10-01T12:00:00.0Z example.org root - - - test" | nc 127.0.0.1 6514

# UDP
echo "<13>1 2018-10-01T12:00:00.0Z example.org root - - - test" | nc -u 127.0.0.1 6514
```

#### RFC3164

RFC3164 encoded messages are not currently supported.  You may see the following error if a message encoded in this format:
```
E! Error in plugin [inputs.syslog]: expecting a version value in the range 1-999 [col 5]
```

You can use rsyslog to translate RFC3164 syslog messages into RFC5424 format.
