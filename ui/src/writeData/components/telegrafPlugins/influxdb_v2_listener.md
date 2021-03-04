# InfluxDB V2 Listener Input Plugin

InfluxDB V2 Listener is a service input plugin that listens for requests sent
according to the [InfluxDB HTTP API][influxdb_http_api].  The intent of the
plugin is to allow Telegraf to serve as a proxy/router for the `/api/v2/write`
endpoint of the InfluxDB HTTP API.

The `/api/v2/write` endpoint supports the `precision` query parameter and can be set
to one of `ns`, `us`, `ms`, `s`.  All other parameters are ignored and
defer to the output plugins configuration.

Telegraf minimum version: Telegraf 1.16.0

### Configuration:

```toml
[[inputs.influxdb_v2_listener]]
  ## Address and port to host InfluxDB listener on
  ## (Double check the port. Could be 9999 if using OSS Beta)
  service_address = ":8086"

  ## Maximum allowed HTTP request body size in bytes.
  ## 0 means to use the default of 32MiB.
  # max_body_size = "32MiB"

  ## Optional tag to determine the bucket.
  ## If the write has a bucket in the query string then it will be kept in this tag name.
  ## This tag can be used in downstream outputs.
  ## The default value of nothing means it will be off and the database will not be recorded.
  # bucket_tag = ""

  ## Set one or more allowed client CA certificate file names to
  ## enable mutually authenticated TLS connections
  # tls_allowed_cacerts = ["/etc/telegraf/clientca.pem"]

  ## Add service certificate and key
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"

  ## Optional token to accept for HTTP authentication.
  ## You probably want to make sure you have TLS configured above for this.
  # token = "some-long-shared-secret-token"
```

### Metrics:

Metrics are created from InfluxDB Line Protocol in the request body.

### Troubleshooting:

**Example Query:**
```
curl -i -XPOST 'http://localhost:8186/api/v2/write' --data-binary 'cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000'
```

[influxdb_http_api]: https://v2.docs.influxdata.com/v2.0/api/
