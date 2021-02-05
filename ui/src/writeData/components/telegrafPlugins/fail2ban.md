# Fail2ban Input Plugin

The fail2ban plugin gathers the count of failed and banned ip addresses using
[fail2ban](https://www.fail2ban.org).

This plugin runs the `fail2ban-client` command which generally requires root access.
Acquiring the required permissions can be done using several methods:

- [Use sudo](#using-sudo) run fail2ban-client.
- Run telegraf as root. (not recommended)

### Configuration

```toml
# Read metrics from fail2ban.
[[inputs.fail2ban]]
  ## Use sudo to run fail2ban-client
  use_sudo = false
```

### Using sudo

Make sure to set `use_sudo = true` in your configuration file.

You will also need to update your sudoers file.  It is recommended to modify a
file in the `/etc/sudoers.d` directory using `visudo`:

```bash
$ sudo visudo -f /etc/sudoers.d/telegraf
```

Add the following lines to the file, these commands allow the `telegraf` user
to call `fail2ban-client` without needing to provide a password and disables
logging of the call in the auth.log.  Consult `man 8 visudo` and `man 5
sudoers` for details.
```
Cmnd_Alias FAIL2BAN = /usr/bin/fail2ban-client status, /usr/bin/fail2ban-client status *
telegraf  ALL=(root) NOEXEC: NOPASSWD: FAIL2BAN
Defaults!FAIL2BAN !logfile, !syslog, !pam_session
```

### Metrics

- fail2ban
  - tags:
    - jail
  - fields:
    - failed (integer, count)
    - banned (integer, count)

### Example Output

```
# fail2ban-client status sshd
Status for the jail: sshd
|- Filter
|  |- Currently failed: 5
|  |- Total failed:     20
|  `- File list:        /var/log/secure
`- Actions
   |- Currently banned: 2
   |- Total banned:     10
   `- Banned IP list:   192.168.0.1 192.168.0.2
```

```
fail2ban,jail=sshd failed=5i,banned=2i 1495868667000000000
```
