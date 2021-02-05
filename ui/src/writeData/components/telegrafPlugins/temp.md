# Temperature Input Plugin

The temp input plugin gather metrics on system temperature.  This plugin is
meant to be multi platform and uses platform specific collection methods.

Currently supports Linux and Windows.

### Configuration

```toml
[[inputs.temp]]
  # no configuration
```

### Metrics

- temp
  - tags:
    - sensor
  - fields:
    - temp (float, celcius)


### Troubleshooting

On **Windows**, the plugin uses a WMI call that is can be replicated with the
following command:
```
wmic /namespace:\\root\wmi PATH MSAcpi_ThermalZoneTemperature
```

### Example Output

```
temp,sensor=coretemp_physicalid0_crit temp=100 1531298763000000000
temp,sensor=coretemp_physicalid0_critalarm temp=0 1531298763000000000
temp,sensor=coretemp_physicalid0_input temp=100 1531298763000000000
temp,sensor=coretemp_physicalid0_max temp=100 1531298763000000000
```
