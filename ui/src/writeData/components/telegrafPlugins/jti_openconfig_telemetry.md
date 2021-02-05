# JTI OpenConfig Telemetry Input Plugin

This plugin reads Juniper Networks implementation of OpenConfig telemetry data from listed sensors using Junos Telemetry Interface. Refer to
[openconfig.net](http://openconfig.net/) for more details about OpenConfig and [Junos Telemetry Interface (JTI)](https://www.juniper.net/documentation/en_US/junos/topics/concept/junos-telemetry-interface-oveview.html).

### Configuration:

```toml
# Subscribe and receive OpenConfig Telemetry data using JTI
[[inputs.jti_openconfig_telemetry]]
  ## List of device addresses to collect telemetry from
  servers = ["localhost:1883"]

  ## Authentication details. Username and password are must if device expects
  ## authentication. Client ID must be unique when connecting from multiple instances
  ## of telegraf to the same device
  username = "user"
  password = "pass"
  client_id = "telegraf"

  ## Frequency to get data
  sample_frequency = "1000ms"

  ## Sensors to subscribe for
  ## A identifier for each sensor can be provided in path by separating with space
  ## Else sensor path will be used as identifier
  ## When identifier is used, we can provide a list of space separated sensors.
  ## A single subscription will be created with all these sensors and data will
  ## be saved to measurement with this identifier name
  sensors = [
   "/interfaces/",
   "collection /components/ /lldp",
  ]

  ## We allow specifying sensor group level reporting rate. To do this, specify the
  ## reporting rate in Duration at the beginning of sensor paths / collection
  ## name. For entries without reporting rate, we use configured sample frequency
  sensors = [
   "1000ms customReporting /interfaces /lldp",
   "2000ms collection /components",
   "/interfaces",
  ]

  ## Optional TLS Config
  # enable_tls = true
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false

  ## Delay between retry attempts of failed RPC calls or streams. Defaults to 1000ms.
  ## Failed streams/calls will not be retried if 0 is provided
  retry_delay = "1000ms"

  ## To treat all string values as tags, set this to true
  str_as_tags = false
```

### Tags:

- All measurements are tagged appropriately using the identifier information
  in incoming data
