# Apache Input Plugin

The Apache plugin collects server performance information using the [`mod_status`](https://httpd.apache.org/docs/2.4/mod/mod_status.html) module of the [Apache HTTP Server](https://httpd.apache.org/).

Typically, the `mod_status` module is configured to expose a page at the `/server-status?auto` location of the Apache server.  The [ExtendedStatus](https://httpd.apache.org/docs/2.4/mod/core.html#extendedstatus) option must be enabled in order to collect all available fields.  For information about how to configure your server reference the [module documentation](https://httpd.apache.org/docs/2.4/mod/mod_status.html#enable).

### Configuration:

```toml
# Read Apache status information (mod_status)
[[inputs.apache]]
  ## An array of URLs to gather from, must be directed at the machine
  ## readable version of the mod_status page including the auto query string.
  ## Default is "http://localhost/server-status?auto".
  urls = ["http://localhost/server-status?auto"]

  ## Credentials for basic HTTP authentication.
  # username = "myuser"
  # password = "mypassword"

  ## Maximum time to receive response.
  # response_timeout = "5s"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

### Measurements & Fields:

- apache
  - BusyWorkers (float)
  - BytesPerReq (float)
  - BytesPerSec (float)
  - ConnsAsyncClosing (float)
  - ConnsAsyncKeepAlive (float)
  - ConnsAsyncWriting (float)
  - ConnsTotal (float)
  - CPUChildrenSystem (float)
  - CPUChildrenUser (float)
  - CPULoad (float)
  - CPUSystem (float)
  - CPUUser (float)
  - IdleWorkers (float)
  - Load1 (float)
  - Load5 (float)
  - Load15 (float)
  - ParentServerConfigGeneration (float)
  - ParentServerMPMGeneration (float)
  - ReqPerSec (float)
  - ServerUptimeSeconds (float)
  - TotalAccesses (float)
  - TotalkBytes (float)
  - Uptime (float)

The following fields are collected from the `Scoreboard`, and represent the number of requests in the given state:

- apache
  - scboard_closing (float)
  - scboard_dnslookup (float)
  - scboard_finishing (float)
  - scboard_idle_cleanup (float)
  - scboard_keepalive (float)
  - scboard_logging (float)
  - scboard_open (float)
  - scboard_reading (float)
  - scboard_sending (float)
  - scboard_starting (float)
  - scboard_waiting (float)

### Tags:

- All measurements have the following tags:
    - port
    - server

### Example Output:

```
apache,port=80,server=debian-stretch-apache BusyWorkers=1,BytesPerReq=0,BytesPerSec=0,CPUChildrenSystem=0,CPUChildrenUser=0,CPULoad=0.00995025,CPUSystem=0.01,CPUUser=0.01,ConnsAsyncClosing=0,ConnsAsyncKeepAlive=0,ConnsAsyncWriting=0,ConnsTotal=0,IdleWorkers=49,Load1=0.01,Load15=0,Load5=0,ParentServerConfigGeneration=3,ParentServerMPMGeneration=2,ReqPerSec=0.00497512,ServerUptimeSeconds=201,TotalAccesses=1,TotalkBytes=0,Uptime=201,scboard_closing=0,scboard_dnslookup=0,scboard_finishing=0,scboard_idle_cleanup=0,scboard_keepalive=0,scboard_logging=0,scboard_open=100,scboard_reading=0,scboard_sending=1,scboard_starting=0,scboard_waiting=49 1502489900000000000
```
