# Processes Input Plugin

This plugin gathers info about the total number of processes and groups
them by status (zombie, sleeping, running, etc.)

On linux this plugin requires access to procfs (/proc), on other OSes
it requires access to execute `ps`.

**Supported Platforms**: Linux, FreeBSD, Darwin

### Configuration

```toml
# Get the number of processes and group them by status
[[inputs.processes]]
  # no configuration
```

Another possible configuration is to define an alternative path for resolving the /proc location.
Using the environment variable `HOST_PROC` the plugin will retrieve process information from the specified location.

`docker run -v /proc:/rootfs/proc:ro -e HOST_PROC=/rootfs/proc`

### Metrics

- processes
  - fields:
    - blocked (aka disk sleep or uninterruptible sleep)
    - running
    - sleeping
    - stopped
    - total
    - zombie
    - dead
    - wait (freebsd only)
    - idle (bsd and Linux 4+ only)
    - paging (linux only)
    - parked (linux only)
    - total_threads (linux only)

### Process State Mappings

Different OSes use slightly different State codes for their processes, these
state codes are documented in `man ps`, and I will give a mapping of what major
OS state codes correspond to in telegraf metrics:

```
Linux  FreeBSD  Darwin  meaning
  R       R       R     running
  S       S       S     sleeping
  Z       Z       Z     zombie
  X      none    none   dead
  T       T       T     stopped
  I       I       I     idle (sleeping for longer than about 20 seconds)
  D      D,L      U     blocked (waiting in uninterruptible sleep, or locked)
  W       W      none   paging (linux kernel < 2.6 only), wait (freebsd)
```

### Example Output

```
processes blocked=8i,running=1i,sleeping=265i,stopped=0i,total=274i,zombie=0i,dead=0i,paging=0i,total_threads=687i 1457478636980905042
```
