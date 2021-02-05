# SNMP Trap Input Plugin

The SNMP Trap plugin is a service input plugin that receives SNMP
notifications (traps and inform requests).

Notifications are received on plain UDP. The port to listen is
configurable.

### Prerequisites

This plugin uses the `snmptranslate` programs from the
[net-snmp][] project.  These tools will need to be installed into the `PATH` in
order to be located.  Other utilities from the net-snmp project may be useful
for troubleshooting, but are not directly used by the plugin.

These programs will load available MIBs on the system.  Typically the default
directory for MIBs is `/usr/share/snmp/mibs`, but if your MIBs are in a
different location you may need to make the paths known to net-snmp.  The
location of these files can be configured in the `snmp.conf` or via the
`MIBDIRS` environment variable. See [`man 1 snmpcmd`][man snmpcmd] for more
information.

### Configuration
```toml
[[inputs.snmp_trap]]
  ## Transport, local address, and port to listen on.  Transport must
  ## be "udp://".  Omit local address to listen on all interfaces.
  ##   example: "udp://127.0.0.1:1234"
  ##
  ## Special permissions may be required to listen on a port less than
  ## 1024.  See README.md for details
  ##
  # service_address = "udp://:162"
  ## Timeout running snmptranslate command
  # timeout = "5s"
  ## Snmp version
  # version = "2c"
  ## SNMPv3 authentication and encryption options.
  ##
  ## Security Name.
  # sec_name = "myuser"
  ## Authentication protocol; one of "MD5", "SHA" or "".
  # auth_protocol = "MD5"
  ## Authentication password.
  # auth_password = "pass"
  ## Security Level; one of "noAuthNoPriv", "authNoPriv", or "authPriv".
  # sec_level = "authNoPriv"
  ## Privacy protocol used for encrypted messages; one of "DES", "AES", "AES192", "AES192C", "AES256", "AES256C" or "".
  # priv_protocol = ""
  ## Privacy password used for encrypted messages.
  # priv_password = ""
```

#### Using a Privileged Port

On many operating systems, listening on a privileged port (a port
number less than 1024) requires extra permission.  Since the default
SNMP trap port 162 is in this category, using telegraf to receive SNMP
traps may need extra permission.

Instructions for listening on a privileged port vary by operating
system. It is not recommended to run telegraf as superuser in order to
use a privileged port. Instead follow the principle of least privilege
and use a more specific operating system mechanism to allow telegraf to
use the port.  You may also be able to have telegraf use an
unprivileged port and then configure a firewall port forward rule from
the privileged port.

To use a privileged port on Linux, you can use setcap to enable the
CAP_NET_BIND_SERVICE capability on the telegraf binary:

```
setcap cap_net_bind_service=+ep /usr/bin/telegraf
```

On Mac OS, listening on privileged ports is unrestricted on versions
10.14 and later.

### Metrics

- snmp_trap
  - tags:
	- source (string, IP address of trap source)
	- name (string, value from SNMPv2-MIB::snmpTrapOID.0 PDU)
	- mib (string, MIB from SNMPv2-MIB::snmpTrapOID.0 PDU)
	- oid (string, OID string from SNMPv2-MIB::snmpTrapOID.0 PDU)
	- version (string, "1" or "2c" or "3")
	- context_name (string, value from v3 trap)
	- engine_id (string, value from v3 trap)
	- community (string, value from 1 or 2c trap)
  - fields:
	- Fields are mapped from variables in the trap. Field names are
      the trap variable names after MIB lookup. Field values are trap
      variable values.

### Example Output
```
snmp_trap,mib=SNMPv2-MIB,name=coldStart,oid=.1.3.6.1.6.3.1.1.5.1,source=192.168.122.102,version=2c,community=public snmpTrapEnterprise.0="linux",sysUpTimeInstance=1i 1574109187723429814
snmp_trap,mib=NET-SNMP-AGENT-MIB,name=nsNotifyShutdown,oid=.1.3.6.1.4.1.8072.4.0.2,source=192.168.122.102,version=2c,community=public sysUpTimeInstance=5803i,snmpTrapEnterprise.0="netSnmpNotificationPrefix" 1574109186555115459
```

[net-snmp]: http://www.net-snmp.org/
[man snmpcmd]: http://net-snmp.sourceforge.net/docs/man/snmpcmd.html#lbAK
