# PowerDNS Input Plugin

The powerdns plugin gathers metrics about PowerDNS using unix socket.

### Configuration:

```
# Description
[[inputs.powerdns]]
  # An array of sockets to gather stats about.
  # Specify a path to unix socket.
  #
  # If no servers are specified, then '/var/run/pdns.controlsocket' is used as the path.
  unix_sockets = ["/var/run/pdns.controlsocket"]
```

#### Permissions

Telegraf will need read access to the powerdns control socket.

On many systems this can be accomplished by adding the `telegraf` user to the
`pdns` group:
```
usermod telegraf -a -G pdns
```

### Measurements & Fields:

- powerdns
  - corrupt-packets
  - deferred-cache-inserts
  - deferred-cache-lookup
  - dnsupdate-answers
  - dnsupdate-changes
  - dnsupdate-queries
  - dnsupdate-refused
  - packetcache-hit
  - packetcache-miss
  - packetcache-size
  - query-cache-hit
  - query-cache-miss
  - rd-queries
  - recursing-answers
  - recursing-questions
  - recursion-unanswered
  - security-status
  - servfail-packets
  - signatures
  - tcp-answers
  - tcp-queries
  - timedout-packets
  - udp-answers
  - udp-answers-bytes
  - udp-do-queries
  - udp-queries
  - udp4-answers
  - udp4-queries
  - udp6-answers
  - udp6-queries
  - key-cache-size
  - latency
  - meta-cache-size
  - qsize-q
  - signature-cache-size
  - sys-msec
  - uptime
  - user-msec

### Tags:

- tags: `server=socket`

### Example Output:

```
$ ./telegraf --config telegraf.conf --input-filter powerdns --test
> powerdns,server=/var/run/pdns.controlsocket corrupt-packets=0i,deferred-cache-inserts=0i,deferred-cache-lookup=0i,dnsupdate-answers=0i,dnsupdate-changes=0i,dnsupdate-queries=0i,dnsupdate-refused=0i,key-cache-size=0i,latency=26i,meta-cache-size=0i,packetcache-hit=0i,packetcache-miss=1i,packetcache-size=0i,qsize-q=0i,query-cache-hit=0i,query-cache-miss=6i,rd-queries=1i,recursing-answers=0i,recursing-questions=0i,recursion-unanswered=0i,security-status=3i,servfail-packets=0i,signature-cache-size=0i,signatures=0i,sys-msec=4349i,tcp-answers=0i,tcp-queries=0i,timedout-packets=0i,udp-answers=1i,udp-answers-bytes=50i,udp-do-queries=0i,udp-queries=0i,udp4-answers=1i,udp4-queries=1i,udp6-answers=0i,udp6-queries=0i,uptime=166738i,user-msec=3036i 1454078624932715706
```
