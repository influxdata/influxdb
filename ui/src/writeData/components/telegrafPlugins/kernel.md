# Kernel Input Plugin

This plugin is only available on Linux.

The kernel plugin gathers info about the kernel that doesn't fit into other
plugins. In general, it is the statistics available in `/proc/stat` that are
not covered by other plugins as well as the value of `/proc/sys/kernel/random/entropy_avail`

The metrics are documented in `man proc` under the `/proc/stat` section.
The metrics are documented in `man 4 random` under the `/proc/stat` section.

```


/proc/sys/kernel/random/entropy_avail
Contains the value of available entropy

/proc/stat
kernel/system statistics. Varies with architecture. Common entries include:

page 5741 1808
The number of pages the system paged in and the number that were paged out (from disk).

swap 1 0
The number of swap pages that have been brought in and out.

intr 1462898
This line shows counts of interrupts serviced since boot time, for each of
the possible system interrupts. The first column is the total of all
interrupts serviced; each subsequent column is the total for a particular interrupt.

ctxt 115315
The number of context switches that the system underwent.

btime 769041601
boot time, in seconds since the Epoch, 1970-01-01 00:00:00 +0000 (UTC).

processes 86031
Number of forks since boot.
```

### Configuration:

```toml
# Get kernel statistics from /proc/stat
[[inputs.kernel]]
  # no configuration
```

### Measurements & Fields:

- kernel
    - boot_time (integer, seconds since epoch, `btime`)
    - context_switches (integer, `ctxt`)
    - disk_pages_in (integer, `page (0)`)
    - disk_pages_out (integer, `page (1)`)
    - interrupts (integer, `intr`)
    - processes_forked (integer, `processes`)
    - entropy_avail (integer, `entropy_available`)

### Tags:

None

### Example Output:

```
$ telegraf --config ~/ws/telegraf.conf --input-filter kernel --test
* Plugin: kernel, Collection 1
> kernel entropy_available=2469i,boot_time=1457505775i,context_switches=2626618i,disk_pages_in=5741i,disk_pages_out=1808i,interrupts=1472736i,processes_forked=10673i 1457613402960879816
```
