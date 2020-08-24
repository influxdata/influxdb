# S.M.A.R.T. Input Plugin

Get metrics using the command line utility `smartctl` for S.M.A.R.T. (Self-Monitoring, Analysis and Reporting Technology) storage devices. SMART is a monitoring system included in computer hard disk drives (HDDs) and solid-state drives (SSDs)[1] that detects and reports on various indicators of drive reliability, with the intent of enabling the anticipation of hardware failures.
See smartmontools (https://www.smartmontools.org/).

SMART information is separated between different measurements: `smart_device` is used for general information, while `smart_attribute` stores the detailed attribute information if `attributes = true` is enabled in the plugin configuration.

If no devices are specified, the plugin will scan for SMART devices via the following command:

```
smartctl --scan
```

Metrics will be reported from the following `smartctl` command:

```
smartctl --info --attributes --health -n <nocheck> --format=brief <device>
```

This plugin supports _smartmontools_ version 5.41 and above, but v. 5.41 and v. 5.42
might require setting `nocheck`, see the comment in the sample configuration.

To enable SMART on a storage device run:

```
smartctl -s on <device>
```

### Configuration

```toml
# Read metrics from storage devices supporting S.M.A.R.T.
[[inputs.smart]]
  ## Optionally specify the path to the smartctl executable
  # path = "/usr/bin/smartctl"

  ## On most platforms smartctl requires root access.
  ## Setting 'use_sudo' to true will make use of sudo to run smartctl.
  ## Sudo must be configured to to allow the telegraf user to run smartctl
  ## without a password.
  # use_sudo = false

  ## Skip checking disks in this power mode. Defaults to
  ## "standby" to not wake up disks that have stoped rotating.
  ## See --nocheck in the man pages for smartctl.
  ## smartctl version 5.41 and 5.42 have faulty detection of
  ## power mode and might require changing this value to
  ## "never" depending on your disks.
  # nocheck = "standby"

  ## Gather all returned S.M.A.R.T. attribute metrics and the detailed
  ## information from each drive into the `smart_attribute` measurement.
  # attributes = false

  ## Optionally specify devices to exclude from reporting.
  # excludes = [ "/dev/pass6" ]

  ## Optionally specify devices and device type, if unset
  ## a scan (smartctl --scan) for S.M.A.R.T. devices will
  ## done and all found will be included except for the
  ## excluded in excludes.
  # devices = [ "/dev/ada0 -d atacam" ]

  ## Timeout for the smartctl command to complete.
  # timeout = "30s"
```

### Permissions

It's important to note that this plugin references smartctl, which may require additional permissions to execute successfully.
Depending on the user/group permissions of the telegraf user executing this plugin, you may need to  use sudo.


You will need the following in your telegraf config:
```toml
[[inputs.smart]]
  use_sudo = true
```

You will also need to update your sudoers file:
```bash
$ visudo
# Add the following line:
Cmnd_Alias SMARTCTL = /usr/bin/smartctl
telegraf  ALL=(ALL) NOPASSWD: SMARTCTL
Defaults!SMARTCTL !logfile, !syslog, !pam_session
```

### Metrics

- smart_device:
  - tags:
    - capacity
    - device
    - enabled
    - model
    - serial_no
    - wwn
  - fields:
    - exit_status
    - health_ok
    - read_error_rate
    - seek_error
    - temp_c
    - udma_crc_errors

- smart_attribute:
  - tags:
    - capacity
    - device
    - enabled
    - fail
    - flags
    - id
    - model
    - name
    - serial_no
    - wwn
  - fields:
    - exit_status
    - raw_value
    - threshold
    - value
    - worst

#### Flags

The interpretation of the tag `flags` is:
 - `K` auto-keep
 - `C` event count
 - `R` error rate
 - `S` speed/performance
 - `O` updated online
 - `P` prefailure warning

#### Exit Status

The `exit_status` field captures the exit status of the smartctl command which
is defined by a bitmask. For the interpretation of the bitmask see the man page for
smartctl.

#### Device Names

Device names, e.g., `/dev/sda`, are *not persistent*, and may be
subject to change across reboots or system changes. Instead, you can the
*World Wide Name* (WWN) or serial number to identify devices. On Linux block
devices can be referenced by the WWN in the following location:
`/dev/disk/by-id/`.

To run `smartctl` with `sudo` create a wrapper script and use `path` in
the configuration to execute that.

### Troubleshooting

If this plugin is not working as expected for your SMART enabled device,
please run these commands and include the output in a bug report:
```
smartctl --scan
```

Run the following command replacing your configuration setting for NOCHECK and
the DEVICE from the previous command:
```
smartctl --info --health --attributes --tolerance=verypermissive --nocheck NOCHECK --format=brief -d DEVICE
```

### Example Output

```
smart_device,enabled=Enabled,host=mbpro.local,device=rdisk0,model=APPLE\ SSD\ SM0512F,serial_no=S1K5NYCD964433,wwn=5002538655584d30,capacity=500277790720 udma_crc_errors=0i,exit_status=0i,health_ok=true,read_error_rate=0i,temp_c=40i 1502536854000000000
smart_attribute,capacity=500277790720,device=rdisk0,enabled=Enabled,fail=-,flags=-O-RC-,host=mbpro.local,id=199,model=APPLE\ SSD\ SM0512F,name=UDMA_CRC_Error_Count,serial_no=S1K5NYCD964433,wwn=5002538655584d30 exit_status=0i,raw_value=0i,threshold=0i,value=200i,worst=200i 1502536854000000000
smart_attribute,capacity=500277790720,device=rdisk0,enabled=Enabled,fail=-,flags=-O---K,host=mbpro.local,id=199,model=APPLE\ SSD\ SM0512F,name=Unknown_SSD_Attribute,serial_no=S1K5NYCD964433,wwn=5002538655584d30 exit_status=0i,raw_value=0i,threshold=0i,value=100i,worst=100i 1502536854000000000
```
