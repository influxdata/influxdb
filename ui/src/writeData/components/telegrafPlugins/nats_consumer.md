# NATS Consumer Input Plugin

The [NATS][nats] consumer plugin reads from the specified NATS subjects and
creates metrics using one of the supported [input data formats][].

A [Queue Group][queue group] is used when subscribing to subjects so multiple
instances of telegraf can read from a NATS cluster in parallel.

### Configuration:

```toml
[[inputs.nats_consumer]]
  ## urls of NATS servers
  servers = ["nats://localhost:4222"]

  ## subject(s) to consume
  subjects = ["telegraf"]

  ## name a queue group
  queue_group = "telegraf_consumers"

  ## Optional credentials
  # username = ""
  # password = ""

  ## Optional NATS 2.0 and NATS NGS compatible user credentials
  # credentials = "/etc/telegraf/nats.creds"

  ## Use Transport Layer Security
  # secure = false

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false

  ## Sets the limits for pending msgs and bytes for each subscription
  ## These shouldn't need to be adjusted except in very high throughput scenarios
  # pending_message_limit = 65536
  # pending_bytes_limit = 67108864

  ## Maximum messages to read from the broker that have not been written by an
  ## output.  For best throughput set based on the number of metrics within
  ## each message and the size of the output's metric_batch_size.
  ##
  ## For example, if each message from the queue contains 10 metrics and the
  ## output metric_batch_size is 1000, setting this to 100 will ensure that a
  ## full batch is collected and the write is triggered immediately without
  ## waiting until the next flush_interval.
  # max_undelivered_messages = 1000

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
```

[nats]: https://www.nats.io/about/
[input data formats]: /docs/DATA_FORMATS_INPUT.md
[queue group]: https://www.nats.io/documentation/concepts/nats-queueing/
