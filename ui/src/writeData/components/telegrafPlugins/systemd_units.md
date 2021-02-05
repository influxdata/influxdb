# systemd Units Input Plugin

The systemd_units plugin gathers systemd unit status on Linux. It relies on
`systemctl list-units --all --plain --type=service` to collect data on service status.

The results are tagged with the unit name and provide enumerated fields for
loaded, active and running fields, indicating the unit health.

This plugin is related to the [win_services module](/plugins/inputs/win_services/), which
fulfills the same purpose on windows.

In addition to services, this plugin can gather other unit types as well,
see `systemctl list-units --all --type help` for possible options.

### Configuration
```toml
[[inputs.systemd_units]]
  ## Set timeout for systemctl execution
  # timeout = "1s"
  #
  ## Filter for a specific unit type, default is "service", other possible
  ## values are "socket", "target", "device", "mount", "automount", "swap",
  ## "timer", "path", "slice" and "scope ":
  # unittype = "service"
```

### Metrics
- systemd_units:
  - tags:
    - name (string, unit name)
    - load (string, load state)
    - active (string, active state)
    - sub (string, sub state)
  - fields:
    - load_code (int, see below)
    - active_code (int, see below)
    - sub_code (int, see below)

#### Load

enumeration of [unit_load_state_table](https://github.com/systemd/systemd/blob/c87700a1335f489be31cd3549927da68b5638819/src/basic/unit-def.c#L87)

| Value | Meaning     | Description                     |
| ----- | -------     | -----------                     |
| 0     | loaded      | unit is ~                       |
| 1     | stub        | unit is ~                       |
| 2     | not-found   | unit is ~                       |
| 3     | bad-setting | unit is ~                       |
| 4     | error       | unit is ~                       |
| 5     | merged      | unit is ~                       |
| 6     | masked      | unit is ~                       |

#### Active

enumeration of [unit_active_state_table](https://github.com/systemd/systemd/blob/c87700a1335f489be31cd3549927da68b5638819/src/basic/unit-def.c#L99)

| Value | Meaning   | Description                        |
| ----- | -------   | -----------                        |
| 0     | active       | unit is ~                       |
| 1     | reloading    | unit is ~                       |
| 2     | inactive     | unit is ~                       |
| 3     | failed       | unit is ~                       |
| 4     | activating   | unit is ~                       |
| 5     | deactivating | unit is ~                       |

#### Sub

enumeration of sub states, see various [unittype_state_tables](https://github.com/systemd/systemd/blob/c87700a1335f489be31cd3549927da68b5638819/src/basic/unit-def.c#L163);
duplicates were removed, tables are hex aligned to keep some space for future
values

| Value  | Meaning               | Description                         |
| -----  | -------               | -----------                         |
|        |                       | service_state_table start at 0x0000 |
| 0x0000 | running               | unit is ~                           |
| 0x0001 | dead                  | unit is ~                           |
| 0x0002 | start-pre             | unit is ~                           |
| 0x0003 | start                 | unit is ~                           |
| 0x0004 | exited                | unit is ~                           |
| 0x0005 | reload                | unit is ~                           |
| 0x0006 | stop                  | unit is ~                           |
| 0x0007 | stop-watchdog         | unit is ~                           |
| 0x0008 | stop-sigterm          | unit is ~                           |
| 0x0009 | stop-sigkill          | unit is ~                           |
| 0x000a | stop-post             | unit is ~                           |
| 0x000b | final-sigterm         | unit is ~                           |
| 0x000c | failed                | unit is ~                           |
| 0x000d | auto-restart          | unit is ~                           |
|        |                       | service_state_table start at 0x0010 |
| 0x0010 | waiting               | unit is ~                           |
|        |                       | service_state_table start at 0x0020 |
| 0x0020 | tentative             | unit is ~                           |
| 0x0021 | plugged               | unit is ~                           |
|        |                       | service_state_table start at 0x0030 |
| 0x0030 | mounting              | unit is ~                           |
| 0x0031 | mounting-done         | unit is ~                           |
| 0x0032 | mounted               | unit is ~                           |
| 0x0033 | remounting            | unit is ~                           |
| 0x0034 | unmounting            | unit is ~                           |
| 0x0035 | remounting-sigterm    | unit is ~                           |
| 0x0036 | remounting-sigkill    | unit is ~                           |
| 0x0037 | unmounting-sigterm    | unit is ~                           |
| 0x0038 | unmounting-sigkill    | unit is ~                           |
|        |                       | service_state_table start at 0x0040 |
|        |                       | service_state_table start at 0x0050 |
| 0x0050 | abandoned             | unit is ~                           |
|        |                       | service_state_table start at 0x0060 |
| 0x0060 | active                | unit is ~                           |
|        |                       | service_state_table start at 0x0070 |
| 0x0070 | start-chown           | unit is ~                           |
| 0x0071 | start-post            | unit is ~                           |
| 0x0072 | listening             | unit is ~                           |
| 0x0073 | stop-pre              | unit is ~                           |
| 0x0074 | stop-pre-sigterm      | unit is ~                           |
| 0x0075 | stop-pre-sigkill      | unit is ~                           |
| 0x0076 | final-sigkill         | unit is ~                           |
|        |                       | service_state_table start at 0x0080 |
| 0x0080 | activating            | unit is ~                           |
| 0x0081 | activating-done       | unit is ~                           |
| 0x0082 | deactivating          | unit is ~                           |
| 0x0083 | deactivating-sigterm  | unit is ~                           |
| 0x0084 | deactivating-sigkill  | unit is ~                           |
|        |                       | service_state_table start at 0x0090 |
|        |                       | service_state_table start at 0x00a0 |
| 0x00a0 | elapsed               | unit is ~                           |
|        |                       |                                     |

### Example Output

```
systemd_units,host=host1.example.com,name=dbus.service,load=loaded,active=active,sub=running load_code=0i,active_code=0i,sub_code=0i 1533730725000000000
systemd_units,host=host1.example.com,name=networking.service,load=loaded,active=failed,sub=failed load_code=0i,active_code=3i,sub_code=12i 1533730725000000000
systemd_units,host=host1.example.com,name=ssh.service,load=loaded,active=active,sub=running load_code=0i,active_code=0i,sub_code=0i 1533730725000000000
...
```
