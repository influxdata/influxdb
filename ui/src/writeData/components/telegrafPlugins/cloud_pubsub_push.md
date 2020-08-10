# Google Cloud PubSub Push Input Service Plugin

The Google Cloud PubSub Push listener is a service input plugin that listens for messages sent via an HTTP POST from [Google Cloud PubSub][pubsub].
The plugin expects messages in Google's Pub/Sub JSON Format ONLY.
The intent of the plugin is to allow Telegraf to serve as an endpoint of the Google Pub/Sub 'Push' service.
Google's PubSub service will **only** send over HTTPS/TLS so this plugin must be behind a valid proxy or must be configured to use TLS.

Enable TLS by specifying the file names of a service TLS certificate and key.

Enable mutually authenticated TLS and authorize client connections by signing certificate authority by including a list of allowed CA certificate file names in `tls_allowed_cacerts`.


### Configuration:

This is a sample configuration for the plugin.

```toml
[[inputs.cloud_pubsub_push]]
  ## Address and port to host HTTP listener on
  service_address = ":8080"

  ## Application secret to verify messages originate from Cloud Pub/Sub
  # token = ""

  ## Path to listen to.
  # path = "/"

  ## Maximum duration before timing out read of the request
  # read_timeout = "10s"
  ## Maximum duration before timing out write of the response. This should be set to a value
  ## large enough that you can send at least 'metric_batch_size' number of messages within the
  ## duration.
  # write_timeout = "10s"

  ## Maximum allowed http request body size in bytes.
  ## 0 means to use the default of 524,288,00 bytes (500 mebibytes)
  # max_body_size = "500MB"

  ## Whether to add the pubsub metadata, such as message attributes and subscription as a tag.
  # add_meta = false

  ## Optional. Maximum messages to read from PubSub that have not been written
  ## to an output. Defaults to 1000.
  ## For best throughput set based on the number of metrics within
  ## each message and the size of the output's metric_batch_size.
  ##
  ## For example, if each message contains 10 metrics and the output
  ## metric_batch_size is 1000, setting this to 100 will ensure that a
  ## full batch is collected and the write is triggered immediately without
  ## waiting until the next flush_interval.
  # max_undelivered_messages = 1000

  ## Set one or more allowed client CA certificate file names to
  ## enable mutually authenticated TLS connections
  # tls_allowed_cacerts = ["/etc/telegraf/clientca.pem"]

  ## Add service certificate and key
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
```

This plugin assumes you have already created a PUSH subscription for a given
PubSub topic.

[pubsub]: https://cloud.google.com/pubsub
[input data formats]: /docs/DATA_FORMATS_INPUT.md
