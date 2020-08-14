# System Input Plugin

The system plugin gathers general stats on system load, uptime,
and number of users logged in. It is similar to the unix `uptime` command.

Number of CPUs is obtained from the /proc/cpuinfo file.

### Configuration:

```toml
# Read metrics about system load & uptime
[[inputs.system]]
  # no configuration
```
#### Permissions:

The `n_users` field requires read access to `/var/run/utmp`, and may require
the `telegraf` user to be added to the `utmp` group on some systems. If this file does not exist `n_users` will be skipped.

### Metrics:

- system
  - fields:
	- load1 (float)
	- load15 (float)
	- load5 (float)
	- n_users (integer)
	- n_cpus (integer)
	- uptime (integer, seconds)
	- uptime_format (string, deprecated in 1.10, use `uptime` field)

### Example Output:

```
system,host=tyrion load1=3.72,load5=2.4,load15=2.1,n_users=3i,n_cpus=4i 1483964144000000000
system,host=tyrion uptime=1249632i 1483964144000000000
system,host=tyrion uptime_format="14 days, 11:07" 1483964144000000000
```
