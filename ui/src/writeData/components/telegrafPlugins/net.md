# Net Input Plugin

This plugin gathers metrics about network interface and protocol usage (Linux only).

### Configuration:

```toml
# Gather metrics about network interfaces
[[inputs.net]]
  ## By default, telegraf gathers stats from any up interface (excluding loopback)
  ## Setting interfaces will tell it to gather these explicit interfaces,
  ## regardless of status. When specifying an interface, glob-style
  ## patterns are also supported.
  ##
  # interfaces = ["eth*", "enp0s[0-1]", "lo"]
  ##
  ## On linux systems telegraf also collects protocol stats.
  ## Setting ignore_protocol_stats to true will skip reporting of protocol metrics.
  ##
  # ignore_protocol_stats = false
  ##
```

### Measurements & Fields:

The fields from this plugin are gathered in the _net_ measurement.

Fields (all platforms):

* bytes_sent - The total number of bytes sent by the interface
* bytes_recv - The total number of bytes received by the interface
* packets_sent - The total number of packets sent by the interface
* packets_recv - The total number of packets received by the interface
* err_in - The total number of receive errors detected by the interface
* err_out - The total number of transmit errors detected by the interface
* drop_in - The total number of received packets dropped by the interface
* drop_out - The total number of transmitted packets dropped by the interface

Different platforms gather the data above with different mechanisms. Telegraf uses the ([gopsutil](https://github.com/shirou/gopsutil)) package, which under Linux reads the /proc/net/dev file.
Under freebsd/openbsd and darwin the plugin uses netstat.

Additionally, for the time being _only under Linux_, the plugin gathers system wide stats for different network protocols using /proc/net/snmp (tcp, udp, icmp, etc.).
Explanation of the different metrics exposed by snmp is out of the scope of this document. The best way to find information would be tracing the constants in the Linux kernel source [here](https://elixir.bootlin.com/linux/latest/source/net/ipv4/proc.c) and their usage. If /proc/net/snmp cannot be read for some reason, telegraf ignores the error silently.

### Tags:

* Net measurements have the following tags:
    - interface (the interface from which metrics are gathered)

Under Linux the system wide protocol metrics have the interface=all tag.

### Sample Queries:

You can use the following query to get the upload/download traffic rate per second for all interfaces in the last hour. The query uses the [derivative function](https://docs.influxdata.com/influxdb/v1.2/query_language/functions#derivative) which calculates the rate of change between subsequent field values.

```sql
SELECT derivative(first(bytes_recv), 1s) as "download bytes/sec", derivative(first(bytes_sent), 1s) as "upload bytes/sec" FROM net WHERE time > now() - 1h AND interface != 'all' GROUP BY time(10s), interface fill(0);
```

### Example Output:

```
# All platforms
$ ./telegraf --config telegraf.conf --input-filter net --test
net,interface=eth0,host=HOST bytes_sent=451838509i,bytes_recv=3284081640i,packets_sent=2663590i,packets_recv=3585442i,err_in=0i,err_out=0i,drop_in=4i,drop_out=0i 1492834180000000000
```

```
# Linux
$ ./telegraf --config telegraf.conf --input-filter net --test
net,interface=eth0,host=HOST bytes_sent=451838509i,bytes_recv=3284081640i,packets_sent=2663590i,packets_recv=3585442i,err_in=0i,err_out=0i,drop_in=4i,drop_out=0i 1492834180000000000
net,interface=all,host=HOST ip_reasmfails=0i,icmp_insrcquenchs=0i,icmp_outtimestamps=0i,ip_inhdrerrors=0i,ip_inunknownprotos=0i,icmp_intimeexcds=10i,icmp_outaddrmasks=0i,icmp_indestunreachs=11005i,icmpmsg_outtype0=6i,tcp_retranssegs=14669i,udplite_outdatagrams=0i,ip_reasmtimeout=0i,ip_outnoroutes=2577i,ip_inaddrerrors=186i,icmp_outaddrmaskreps=0i,tcp_incsumerrors=0i,tcp_activeopens=55965i,ip_reasmoks=0i,icmp_inechos=6i,icmp_outdestunreachs=9417i,ip_reasmreqds=0i,icmp_outtimestampreps=0i,tcp_rtoalgorithm=1i,icmpmsg_intype3=11005i,icmpmsg_outtype69=129i,tcp_outsegs=2777459i,udplite_rcvbuferrors=0i,ip_fragoks=0i,icmp_inmsgs=13398i,icmp_outerrors=0i,tcp_outrsts=14951i,udplite_noports=0i,icmp_outmsgs=11517i,icmp_outechoreps=6i,icmpmsg_intype11=10i,icmp_inparmprobs=0i,ip_forwdatagrams=0i,icmp_inechoreps=1909i,icmp_outredirects=0i,icmp_intimestampreps=0i,icmpmsg_intype5=468i,tcp_rtomax=120000i,tcp_maxconn=-1i,ip_fragcreates=0i,ip_fragfails=0i,icmp_inredirects=468i,icmp_outtimeexcds=0i,icmp_outechos=1965i,icmp_inaddrmasks=0i,tcp_inerrs=389i,tcp_rtomin=200i,ip_defaultttl=64i,ip_outrequests=3366408i,ip_forwarding=2i,udp_incsumerrors=0i,udp_indatagrams=522136i,udplite_incsumerrors=0i,ip_outdiscards=871i,icmp_inerrors=958i,icmp_outsrcquenchs=0i,icmpmsg_intype0=1909i,tcp_insegs=3580226i,udp_outdatagrams=577265i,udp_rcvbuferrors=0i,udplite_sndbuferrors=0i,icmp_incsumerrors=0i,icmp_outparmprobs=0i,icmpmsg_outtype3=9417i,tcp_attemptfails=2652i,udplite_inerrors=0i,udplite_indatagrams=0i,ip_inreceives=4172969i,icmpmsg_outtype8=1965i,tcp_currestab=59i,udp_noports=5961i,ip_indelivers=4099279i,ip_indiscards=0i,tcp_estabresets=5818i,udp_sndbuferrors=3i,icmp_intimestamps=0i,icmpmsg_intype8=6i,udp_inerrors=0i,icmp_inaddrmaskreps=0i,tcp_passiveopens=452i 1492831540000000000
```
