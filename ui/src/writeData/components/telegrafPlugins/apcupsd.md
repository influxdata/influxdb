# APCUPSD Input Plugin

This plugin reads data from an apcupsd daemon over its NIS network protocol.

### Requirements

apcupsd should be installed and it's daemon should be running.

### Configuration

```toml
[[inputs.apcupsd]]
  # A list of running apcupsd server to connect to.
  # If not provided will default to tcp://127.0.0.1:3551
  servers = ["tcp://127.0.0.1:3551"]

  ## Timeout for dialing server.
  timeout = "5s"
```

### Metrics

- apcupsd
  - tags:
    - serial
    - status (string representing the set status_flags)
    - ups_name
    - model
  - fields:
    - status_flags ([status-bits][])
    - input_voltage
    - load_percent
    - battery_charge_percent
    - time_left_ns
    - output_voltage
    - internal_temp
    - battery_voltage
    - input_frequency
    - time_on_battery_ns
    - battery_date
    - nominal_input_voltage
    - nominal_battery_voltage
    - nominal_power
    - firmware



### Example output

```
apcupsd,serial=AS1231515,status=ONLINE,ups_name=name1 time_on_battery=0,load_percent=9.7,time_left_minutes=98,output_voltage=230.4,internal_temp=32.4,battery_voltage=27.4,input_frequency=50.2,input_voltage=230.4,battery_charge_percent=100,status_flags=8i 1490035922000000000
```

[status-bits]: http://www.apcupsd.org/manual/manual.html#status-bits
