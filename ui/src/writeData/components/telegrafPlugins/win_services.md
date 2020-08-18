# Windows Services Input Plugin

Reports information about Windows service status.

Monitoring some services may require running Telegraf with administrator privileges.

### Configuration:

```toml
[[inputs.win_services]]
  ## Names of the services to monitor. Leave empty to monitor all the available services on the host
  service_names = [
    "LanmanServer",
    "TermService",
  ]
```

### Measurements & Fields:

- win_services
    - state : integer
    - startup_mode : integer

The `state` field can have the following values:
- 1 - stopped
- 2 - start pending
- 3 - stop pending
- 4 - running
- 5 - continue pending
- 6 - pause pending
- 7 - paused

The `startup_mode` field can have the following values:
- 0 - boot start
- 1 - system start
- 2 - auto start
- 3 - demand start
- 4 - disabled

### Tags:

- All measurements have the following tags:
    - service_name
    - display_name

### Example Output:
```
win_services,host=WIN2008R2H401,display_name=Server,service_name=LanmanServer state=4i,startup_mode=2i 1500040669000000000
win_services,display_name=Remote\ Desktop\ Services,service_name=TermService,host=WIN2008R2H401 state=1i,startup_mode=3i 1500040669000000000
```
### TICK Scripts

A sample TICK script for a notification about a not running service.
It sends a notification whenever any service changes its state to be not _running_ and when it changes that state back to _running_.
The notification is sent via an HTTP POST call.

```
stream
    |from()
        .database('telegraf')
        .retentionPolicy('autogen')
        .measurement('win_services')
        .groupBy('host','service_name')
    |alert()
        .crit(lambda: "state" != 4)
        .stateChangesOnly()
        .message('Service {{ index .Tags "service_name" }} on Host {{ index .Tags "host" }} is in state {{ index .Fields "state" }} ')
        .post('http://localhost:666/alert/service')
```
