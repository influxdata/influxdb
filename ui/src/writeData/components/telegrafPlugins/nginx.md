# Telegraf Plugin: Nginx

### Configuration:

```
# Read Nginx's basic status information (ngx_http_stub_status_module)
[[inputs.nginx]]
  ## An array of Nginx stub_status URI to gather stats.
  urls = ["http://localhost/server_status"]

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false

  ## HTTP response timeout (default: 5s)
  response_timeout = "5s"
```

### Measurements & Fields:

- Measurement
    - accepts
    - active
    - handled
    - reading
    - requests
    - waiting
    - writing

### Tags:

- All measurements have the following tags:
    - port
    - server

### Example Output:

Using this configuration:
```
[[inputs.nginx]]
  ## An array of Nginx stub_status URI to gather stats.
  urls = ["http://localhost/status"]
```

When run with:
```
./telegraf --config telegraf.conf --input-filter nginx --test
```

It produces:
```
* Plugin: nginx, Collection 1
> nginx,port=80,server=localhost accepts=605i,active=2i,handled=605i,reading=0i,requests=12132i,waiting=1i,writing=1i 1456690994701784331
```
