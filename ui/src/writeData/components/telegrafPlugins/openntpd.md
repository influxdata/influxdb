# OpenNTPD Input Plugin

Get standard NTP query metrics from [OpenNTPD][] using the ntpctl command.

[OpenNTPD]: http://www.openntpd.org/

Below is the documentation of the various headers returned from the NTP query
command when running `ntpctl -s peers`.

- remote – The remote peer or server being synced to.
- wt – the peer weight
- tl – the peer trust level
- st (stratum) – The remote peer or server Stratum
- next – number of seconds until the next poll
- poll – polling interval in seconds
- delay – Round trip communication delay to the remote peer
or server (milliseconds);
- offset – Mean offset (phase) in the times reported between this local host and
the remote peer or server (RMS, milliseconds);
- jitter – Mean deviation (jitter) in the time reported for that remote peer or
server (RMS of difference of multiple time samples, milliseconds);

### Configuration

```toml
[[inputs.openntpd]]
  ## Run ntpctl binary with sudo.
  # use_sudo = false

  ## Location of the ntpctl binary.
  # binary = "/usr/sbin/ntpctl"

  ## Maximum time the ntpctl binary is allowed to run.
  # timeout = "5ms"
```

### Metrics

- ntpctl
  - tags:
    - remote
    - stratum
  - fields:
    - delay (float, milliseconds)
    - jitter (float, milliseconds)
    - offset (float, milliseconds)
    - poll (int, seconds)
    - next (int, seconds)
    - wt (int)
    - tl (int)

### Permissions

It's important to note that this plugin references ntpctl, which may require
additional permissions to execute successfully.
Depending on the user/group permissions of the telegraf user executing this
plugin, you may need to alter the group membership, set facls, or use sudo.

**Group membership (Recommended)**:
```bash
$ groups telegraf
telegraf : telegraf

$ usermod -a -G ntpd telegraf

$ groups telegraf
telegraf : telegraf ntpd
```

**Sudo privileges**:
If you use this method, you will need the following in your telegraf config:
```toml
[[inputs.openntpd]]
  use_sudo = true
```

You will also need to update your sudoers file:
```bash
$ visudo
# Add the following lines:
Cmnd_Alias NTPCTL = /usr/sbin/ntpctl
telegraf ALL=(ALL) NOPASSWD: NTPCTL
Defaults!NTPCTL !logfile, !syslog, !pam_session
```

Please use the solution you see as most appropriate.

### Example Output

```
openntpd,remote=194.57.169.1,stratum=2,host=localhost tl=10i,poll=1007i,
offset=2.295,jitter=3.896,delay=53.766,next=266i,wt=1i 1514454299000000000
```
