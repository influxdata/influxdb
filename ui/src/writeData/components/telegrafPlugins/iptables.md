# Iptables Plugin

The iptables plugin gathers packets and bytes counters for rules within a set of table and chain from the Linux's iptables firewall.

Rules are identified through associated comment. **Rules without comment are ignored**.
Indeed we need a unique ID for the rule and the rule number is not a constant: it may vary when rules are inserted/deleted at start-up or by automatic tools (interactive firewalls, fail2ban, ...).
Also when the rule set is becoming big (hundreds of lines) most people are interested in monitoring only a small part of the rule set.

Before using this plugin **you must ensure that the rules you want to monitor are named with a unique comment**. Comments are added using the `-m comment --comment "my comment"` iptables options.

The iptables command requires CAP_NET_ADMIN and CAP_NET_RAW capabilities. You have several options to grant telegraf to run iptables:

* Run telegraf as root. This is strongly discouraged.
* Configure systemd to run telegraf with CAP_NET_ADMIN and CAP_NET_RAW. This is the simplest and recommended option.
* Configure sudo to grant telegraf to run iptables. This is the most restrictive option, but require sudo setup.

### Using systemd capabilities

You may run `systemctl edit telegraf.service` and add the following:

```
[Service]
CapabilityBoundingSet=CAP_NET_RAW CAP_NET_ADMIN
AmbientCapabilities=CAP_NET_RAW CAP_NET_ADMIN
```

Since telegraf will fork a process to run iptables, `AmbientCapabilities` is required to transmit the capabilities bounding set to the forked process.

### Using sudo

You will need the following in your telegraf config:
```toml
[[inputs.iptables]]
  use_sudo = true
```

You will also need to update your sudoers file:

```bash
$ visudo
# Add the following line:
Cmnd_Alias IPTABLESSHOW = /usr/bin/iptables -nvL *
telegraf  ALL=(root) NOPASSWD: IPTABLESSHOW
Defaults!IPTABLESSHOW !logfile, !syslog, !pam_session
```

### Using IPtables lock feature

Defining multiple instances of this plugin in telegraf.conf can lead to concurrent IPtables access resulting in "ERROR in input [inputs.iptables]: exit status 4" messages in telegraf.log and missing metrics. Setting 'use_lock = true' in the plugin configuration will run IPtables with the '-w' switch, allowing a lock usage to prevent this error.

### Configuration:

```toml
  # use sudo to run iptables
  use_sudo = false
  # run iptables with the lock option
  use_lock = false
  # Define an alternate executable, such as "ip6tables". Default is "iptables".
  # binary = "ip6tables"
  # defines the table to monitor:
  table = "filter"
  # defines the chains to monitor:
  chains = [ "INPUT" ]
```

### Measurements & Fields:


- iptables
    - pkts (integer, count)
    - bytes (integer, bytes)

### Tags:

- All measurements have the following tags:
    - table
    - chain
    - ruleid

The `ruleid` is the comment associated to the rule.

### Example Output:

```
$ iptables -nvL INPUT
Chain INPUT (policy DROP 0 packets, 0 bytes)
pkts bytes target     prot opt in     out     source               destination
100   1024   ACCEPT     tcp  --  *      *       192.168.0.0/24       0.0.0.0/0            tcp dpt:22 /* ssh */
 42   2048   ACCEPT     tcp  --  *      *       192.168.0.0/24       0.0.0.0/0            tcp dpt:80 /* httpd */
```

```
$ ./telegraf --config telegraf.conf --input-filter iptables --test
iptables,table=filter,chain=INPUT,ruleid=ssh pkts=100i,bytes=1024i 1453831884664956455
iptables,table=filter,chain=INPUT,ruleid=httpd pkts=42i,bytes=2048i 1453831884664956455
```
