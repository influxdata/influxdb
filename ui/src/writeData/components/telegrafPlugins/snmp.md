# SNMP Input Plugin

The `snmp` input plugin uses polling to gather metrics from SNMP agents.
Support for gathering individual OIDs as well as complete SNMP tables is
included.

### Prerequisites

This plugin uses the `snmptable` and `snmptranslate` programs from the
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
[[inputs.snmp]]
  ## Agent addresses to retrieve values from.
  ##   example: agents = ["udp://127.0.0.1:161"]
  ##            agents = ["tcp://127.0.0.1:161"]
  agents = ["udp://127.0.0.1:161"]

  ## Timeout for each request.
  # timeout = "5s"

  ## SNMP version; can be 1, 2, or 3.
  # version = 2

  ## SNMP community string.
  # community = "public"

  ## Agent host tag
  # agent_host_tag = "agent_host"

  ## Number of retries to attempt.
  # retries = 3

  ## The GETBULK max-repetitions parameter.
  # max_repetitions = 10

  ## SNMPv3 authentication and encryption options.
  ##
  ## Security Name.
  # sec_name = "myuser"
  ## Authentication protocol; one of "MD5", "SHA", or "".
  # auth_protocol = "MD5"
  ## Authentication password.
  # auth_password = "pass"
  ## Security Level; one of "noAuthNoPriv", "authNoPriv", or "authPriv".
  # sec_level = "authNoPriv"
  ## Context Name.
  # context_name = ""
  ## Privacy protocol used for encrypted messages; one of "DES", "AES" or "".
  # priv_protocol = ""
  ## Privacy password used for encrypted messages.
  # priv_password = ""

  ## Add fields and tables defining the variables you wish to collect.  This
  ## example collects the system uptime and interface variables.  Reference the
  ## full plugin documentation for configuration details.
  [[inputs.snmp.field]]
    oid = "RFC1213-MIB::sysUpTime.0"
    name = "uptime"

  [[inputs.snmp.field]]
    oid = "RFC1213-MIB::sysName.0"
    name = "source"
    is_tag = true

  [[inputs.snmp.table]]
    oid = "IF-MIB::ifTable"
    name = "interface"
    inherit_tags = ["source"]

    [[inputs.snmp.table.field]]
      oid = "IF-MIB::ifDescr"
      name = "ifDescr"
      is_tag = true
```

#### Configure SNMP Requests

This plugin provides two methods for configuring the SNMP requests: `fields`
and `tables`.  Use the `field` option to gather single ad-hoc variables.
To collect SNMP tables, use the `table` option.

##### Field

Use a `field` to collect a variable by OID.  Requests specified with this
option operate similar to the `snmpget` utility.

```toml
[[inputs.snmp]]
  # ... snip ...

  [[inputs.snmp.field]]
    ## Object identifier of the variable as a numeric or textual OID.
    oid = "RFC1213-MIB::sysName.0"

    ## Name of the field or tag to create.  If not specified, it defaults to
    ## the value of 'oid'. If 'oid' is numeric, an attempt to translate the
    ## numeric OID into a textual OID will be made.
    # name = ""

    ## If true the variable will be added as a tag, otherwise a field will be
    ## created.
    # is_tag = false

    ## Apply one of the following conversions to the variable value:
    ##   float(X) Convert the input value into a float and divides by the
    ##            Xth power of 10. Effectively just moves the decimal left
    ##            X places. For example a value of `123` with `float(2)`
    ##            will result in `1.23`.
    ##   float:   Convert the value into a float with no adjustment. Same
    ##            as `float(0)`.
    ##   int:     Convert the value into an integer.
    ##   hwaddr:  Convert the value to a MAC address.
    ##   ipaddr:  Convert the value to an IP address.
    # conversion = ""
```

##### Table

Use a `table` to configure the collection of a SNMP table.  SNMP requests
formed with this option operate similarly way to the `snmptable` command.

Control the handling of specific table columns using a nested `field`.  These
nested fields are specified similarly to a top-level `field`.

By default all columns of the SNMP table will be collected - it is not required
to add a nested field for each column, only those which you wish to modify. To
*only* collect certain columns, omit the `oid` from the `table` section and only
include `oid` settings in `field` sections. For more complex include/exclude
cases for columns use [metric filtering][].

One [metric][] is created for each row of the SNMP table.

```toml
[[inputs.snmp]]
  # ... snip ...

  [[inputs.snmp.table]]
    ## Object identifier of the SNMP table as a numeric or textual OID.
    oid = "IF-MIB::ifTable"

    ## Name of the field or tag to create.  If not specified, it defaults to
    ## the value of 'oid'.  If 'oid' is numeric an attempt to translate the
    ## numeric OID into a textual OID will be made.
    # name = ""

    ## Which tags to inherit from the top-level config and to use in the output
    ## of this table's measurement.
    ## example: inherit_tags = ["source"]
    # inherit_tags = []

    ## Add an 'index' tag with the table row number.  Use this if the table has
    ## no indexes or if you are excluding them.  This option is normally not
    ## required as any index columns are automatically added as tags.
    # index_as_tag = false

    [[inputs.snmp.table.field]]
      ## OID to get. May be a numeric or textual module-qualified OID.
      oid = "IF-MIB::ifDescr"

      ## Name of the field or tag to create.  If not specified, it defaults to
      ## the value of 'oid'. If 'oid' is numeric an attempt to translate the
      ## numeric OID into a textual OID will be made.
      # name = ""

      ## Output this field as a tag.
      # is_tag = false

      ## The OID sub-identifier to strip off so that the index can be matched
      ## against other fields in the table.
      # oid_index_suffix = ""

      ## Specifies the length of the index after the supplied table OID (in OID
      ## path segments). Truncates the index after this point to remove non-fixed
      ## value or length index suffixes.
      # oid_index_length = 0
```

### Troubleshooting

Check that a numeric field can be translated to a textual field:
```
$ snmptranslate .1.3.6.1.2.1.1.3.0
DISMAN-EVENT-MIB::sysUpTimeInstance
```

Request a top-level field:
```
$ snmpget -v2c -c public 127.0.0.1 sysUpTime.0
```

Request a table:
```
$ snmptable -v2c -c public 127.0.0.1 ifTable
```

To collect a packet capture, run this command in the background while running
Telegraf or one of the above commands.  Adjust the interface, host and port as
needed:
```
$ sudo tcpdump -s 0 -i eth0 -w telegraf-snmp.pcap host 127.0.0.1 and port 161
```

### Example Output

```
snmp,agent_host=127.0.0.1,source=loaner uptime=11331974i 1575509815000000000
interface,agent_host=127.0.0.1,ifDescr=wlan0,ifIndex=3,source=example.org ifAdminStatus=1i,ifInDiscards=0i,ifInErrors=0i,ifInNUcastPkts=0i,ifInOctets=3436617431i,ifInUcastPkts=2717778i,ifInUnknownProtos=0i,ifLastChange=0i,ifMtu=1500i,ifOperStatus=1i,ifOutDiscards=0i,ifOutErrors=0i,ifOutNUcastPkts=0i,ifOutOctets=581368041i,ifOutQLen=0i,ifOutUcastPkts=1354338i,ifPhysAddress="c8:5b:76:c9:e6:8c",ifSpecific=".0.0",ifSpeed=0i,ifType=6i 1575509815000000000
interface,agent_host=127.0.0.1,ifDescr=eth0,ifIndex=2,source=example.org ifAdminStatus=1i,ifInDiscards=0i,ifInErrors=0i,ifInNUcastPkts=21i,ifInOctets=3852386380i,ifInUcastPkts=3634004i,ifInUnknownProtos=0i,ifLastChange=9088763i,ifMtu=1500i,ifOperStatus=1i,ifOutDiscards=0i,ifOutErrors=0i,ifOutNUcastPkts=0i,ifOutOctets=434865441i,ifOutQLen=0i,ifOutUcastPkts=2110394i,ifPhysAddress="c8:5b:76:c9:e6:8c",ifSpecific=".0.0",ifSpeed=1000000000i,ifType=6i 1575509815000000000
interface,agent_host=127.0.0.1,ifDescr=lo,ifIndex=1,source=example.org ifAdminStatus=1i,ifInDiscards=0i,ifInErrors=0i,ifInNUcastPkts=0i,ifInOctets=51555569i,ifInUcastPkts=339097i,ifInUnknownProtos=0i,ifLastChange=0i,ifMtu=65536i,ifOperStatus=1i,ifOutDiscards=0i,ifOutErrors=0i,ifOutNUcastPkts=0i,ifOutOctets=51555569i,ifOutQLen=0i,ifOutUcastPkts=339097i,ifSpecific=".0.0",ifSpeed=10000000i,ifType=24i 1575509815000000000
```

[net-snmp]: http://www.net-snmp.org/
[man snmpcmd]: http://net-snmp.sourceforge.net/docs/man/snmpcmd.html#lbAK
[metric filtering]: /docs/CONFIGURATION.md#metric-filtering
[metric]: /docs/METRICS.md
